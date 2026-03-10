"""
Normalize flow session config into optional sql / blob / local blocks for multi-source dry run.
Legacy single source_type configs are mapped to one block.
"""
import os
import shutil


def _sql_block_from_legacy(cfg):
    if cfg.get("source_type") != "sql":
        return None
    server = (cfg.get("server") or "").strip()
    database = (cfg.get("database") or "").strip()
    if not server or not database:
        return None
    export_mode = cfg.get("export_mode") or "tables"
    if export_mode == "tables":
        tables = cfg.get("tables") or []
        if not tables:
            return None
        return {
            "server": server,
            "database": database,
            "export_mode": "tables",
            "tables": tables,
        }
    query = (cfg.get("query") or "").strip()
    if not query:
        return None
    return {
        "server": server,
        "database": database,
        "export_mode": "query",
        "query": query,
    }


def _blob_block_from_legacy(cfg):
    if cfg.get("source_type") != "blob":
        return None
    account_name = (cfg.get("account_name") or "").strip()
    container = (cfg.get("container") or "").strip()
    key = (cfg.get("key") or "").strip()
    selected_blobs = cfg.get("selected_blobs") or []
    if not account_name or not container or not key or not selected_blobs:
        return None
    return {
        "account_name": account_name,
        "container": container,
        "key": key,
        "selected_blobs": list(selected_blobs),
        "delimiter": cfg.get("delimiter") or ",",
    }


def _local_block_from_legacy(cfg, temp_dir):
    if cfg.get("source_type") != "local" or not temp_dir or not os.path.isdir(temp_dir):
        return None
    if cfg.get("skip_source_config"):
        return None
    return {
        "temp_dir": temp_dir,
        "delimiter": cfg.get("delimiter") or ",",
        "has_header": cfg.get("has_header", True),
    }


def get_source_blocks(cfg, session_temp_dir):
    """
    Return (sql_block_or_none, blob_block_or_none, local_block_or_none).
    Supports cfg['sql'], cfg['blob'], cfg['local'] or legacy source_type.
    """
    sql_block = cfg.get("sql") if isinstance(cfg.get("sql"), dict) else None
    blob_block = cfg.get("blob") if isinstance(cfg.get("blob"), dict) else None
    local_block = cfg.get("local") if isinstance(cfg.get("local"), dict) else None
    if local_block:
        td = local_block.get("temp_dir") or session_temp_dir
        if td and os.path.isdir(td):
            local_block = dict(local_block)
            local_block["temp_dir"] = td
        else:
            local_block = None

    if not sql_block:
        sql_block = _sql_block_from_legacy(cfg)
    if not blob_block:
        blob_block = _blob_block_from_legacy(cfg)
    if not local_block:
        local_block = _local_block_from_legacy(cfg, session_temp_dir)
    elif local_block and not local_block.get("temp_dir") and session_temp_dir:
        local_block = dict(local_block)
        local_block["temp_dir"] = session_temp_dir

    if local_block and local_block.get("temp_dir") and not os.path.isdir(local_block["temp_dir"]):
        local_block = None

    return sql_block, blob_block, local_block


def has_any_source(cfg, session_temp_dir):
    sql_block, blob_block, local_block = get_source_blocks(cfg, session_temp_dir)
    return bool(sql_block or blob_block or local_block)


def copy_local_csvs_into_dir(local_temp_dir, dest_dir, prefix="local"):
    """Copy non-header CSVs from local_temp_dir into dest_dir with prefix on basename."""
    if not local_temp_dir or not os.path.isdir(local_temp_dir):
        return False, "Invalid local temp dir"
    import re
    header_pat = re.compile(r"^.*_[0-9a-f]{8}\.csv$", re.IGNORECASE)
    p = (prefix or "local").strip()
    if p and not p.endswith("_"):
        p = p + "_"
    copied = 0
    for name in os.listdir(local_temp_dir):
        path = os.path.join(local_temp_dir, name)
        if not os.path.isfile(path) or not name.lower().endswith(".csv"):
            continue
        if header_pat.match(name):
            continue
        dest_name = p + name
        shutil.copy2(path, os.path.join(dest_dir, dest_name))
        copied += 1
    if copied == 0:
        return False, "No CSV files in local upload"
    return True, None
