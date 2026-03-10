"""
Merge SQL, Blob, and Local dry-run exports into a single temp_dir so Delphix runs once over all CSVs.
"""
import os
import shutil
import uuid


def _is_data_csv(name):
    if not name.lower().endswith(".csv"):
        return False
    # exclude Delphix header pattern *_xxxxxxxx.csv
    if len(name) > 13 and name[-13:-4].isalnum() and name[-13:-4].islower():
        pass  # not reliable; delphix_flow excludes by regex — here just copy all csv
    return True


def merge_dry_run_sources(cfg, max_rows, temp_base, local_temp_dir=None):
    """
    Build one temp_dir containing CSVs from all enabled sources (use_sql, use_blob, use_local).
    cfg must include same keys as single-source flows. local_temp_dir is SESSION_TEMP_DIR when local was uploaded.
    Returns (True, temp_dir) or (False, error_message).
    """
    use_sql = cfg.get("use_sql")
    use_blob = cfg.get("use_blob")
    use_local = cfg.get("use_local")
    if not (use_sql or use_blob or use_local):
        return False, "No source enabled"

    os.makedirs(temp_base, exist_ok=True)
    subdir = os.path.join(temp_base, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)

    if use_sql:
        server = (cfg.get("server") or "").strip()
        database = (cfg.get("database") or "").strip()
        export_mode = cfg.get("export_mode") or "tables"
        tables = cfg.get("tables") or []
        query = (cfg.get("query") or "").strip()
        if not server or not database:
            return False, "SQL server and database required"
        if export_mode == "tables" and not tables:
            return False, "SQL: select at least one table or provide a query"
        if export_mode == "query" and not query:
            return False, "SQL: query is empty"
        from app.services.sql_source import export_tables_top_n_prefixed, export_query_top_n_prefixed
        if export_mode == "tables":
            ok, result = export_tables_top_n_prefixed(
                server, database, tables, subdir, max_rows, prefix="sql"
            )
        else:
            ok, result = export_query_top_n_prefixed(
                server, database, query, subdir, max_rows, prefix="sql"
            )
        if not ok:
            return False, result

    if use_blob:
        account_name = (cfg.get("account_name") or "").strip()
        container = (cfg.get("container") or "").strip()
        key = (cfg.get("key") or "").strip()
        selected_blobs = cfg.get("selected_blobs") or []
        delimiter = cfg.get("delimiter", ",") or ","
        if not account_name or not container or not key:
            return False, "Blob account, container and key required"
        if not selected_blobs:
            return False, "Blob: select at least one file"
        from app.services.blob_source import download_top_n_rows
        ok, result = download_top_n_rows(
            account_name, container, key, selected_blobs, delimiter, subdir, max_rows,
            filename_prefix="blob",
        )
        if not ok:
            return False, result

    if use_local:
        if not local_temp_dir or not os.path.isdir(local_temp_dir):
            return False, "Local: no uploaded file (upload again)"
        for name in os.listdir(local_temp_dir):
            if not name.lower().endswith(".csv"):
                continue
            src = os.path.join(local_temp_dir, name)
            if not os.path.isfile(src):
                continue
            # skip header-like files if any
            if "_dry_run_originals" in name:
                continue
            base = os.path.basename(name)
            dst_name = f"local_{base}" if not base.startswith("local_") else base
            dst = os.path.join(subdir, dst_name)
            if os.path.abspath(src) != os.path.abspath(dst):
                shutil.copy2(src, dst)

    # Ensure at least one csv exists
    csvs = [f for f in os.listdir(subdir) if f.lower().endswith(".csv") and os.path.isfile(os.path.join(subdir, f))]
    if not csvs:
        return False, "No CSV files produced from merged sources"
    return True, subdir
