"""
On-prem SQL Server via pyodbc: ActiveDirectoryIntegrated, TrustServerCertificate=yes.
"""
import csv
import os
import uuid

DRIVER = "ODBC Driver 17 for SQL Server"


def _conn_str(server, database):
    return (
        f"Driver={{{DRIVER}}};"
        f"Server={server};"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=yes;"
        "Authentication=ActiveDirectoryIntegrated;"
    )


def validate_connection(server, database):
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        conn.close()
        return True, None
    except Exception as e:
        return False, str(e)


def list_tables(server, database):
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        cur = conn.cursor()
        cur.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        rows = cur.fetchall()
        conn.close()
        tables = [f"{r[0]}.{r[1]}" for r in rows]
        return True, tables
    except Exception as e:
        return False, str(e)


def export_tables_top10(server, database, table_list, temp_dir):
    if not table_list:
        return False, "No tables selected"
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        cur = conn.cursor()
        files = []
        for qual in table_list:
            parts = qual.split(".", 1)
            if len(parts) == 2:
                schema, name = parts
            else:
                schema, name = "dbo", qual
            safe_name = name.replace(" ", "_")
            fpath = os.path.join(temp_dir, f"{safe_name}.csv")
            cur.execute(f'SELECT * FROM [{schema}].[{name}]')
            rows = cur.fetchmany(10)
            if rows:
                with open(fpath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([d[0] for d in cur.description])
                    writer.writerows(rows)
            else:
                with open(fpath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([d[0] for d in cur.description])
            files.append({"name": os.path.basename(fpath), "path": fpath})
        conn.close()
        return True, files
    except Exception as e:
        return False, str(e)


def export_query_top10(server, database, query, temp_dir):
    return export_query_top_n(server, database, query, temp_dir, 10)


def export_tables_top_n(server, database, table_list, temp_dir, n, filename_prefix=""):
    """Export up to n rows per table to CSV files in temp_dir. Optional filename_prefix avoids collisions when merging sources."""
    if not table_list:
        return False, "No tables selected"
    if n is None or n < 1:
        n = 1
    prefix = (filename_prefix or "").strip()
    if prefix and not prefix.endswith("_"):
        prefix = prefix + "_"
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        cur = conn.cursor()
        files = []
        for qual in table_list:
            parts = qual.split(".", 1)
            if len(parts) == 2:
                schema, name = parts
            else:
                schema, name = "dbo", qual
            safe_name = name.replace(" ", "_").replace(".", "_")
            fpath = os.path.join(temp_dir, f"{prefix}{safe_name}.csv")
            cur.execute(f"SELECT TOP ({n}) * FROM [{schema}].[{name}]")
            rows = cur.fetchall()
            if rows:
                with open(fpath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([d[0] for d in cur.description])
                    writer.writerows(rows)
            else:
                with open(fpath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([d[0] for d in cur.description])
            files.append({"name": os.path.basename(fpath), "path": fpath})
        conn.close()
        return True, files
    except Exception as e:
        return False, str(e)


def export_query_top_n(server, database, query, temp_dir, n, filename_prefix=""):
    """Wrap query in SELECT TOP (n) * FROM (query) AS t, execute, write one CSV. Optional filename_prefix for multi-source merge."""
    if n is None or n < 1:
        n = 1
    prefix = (filename_prefix or "").strip()
    if prefix and not prefix.endswith("_"):
        prefix = prefix + "_"
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        wrapped = f"SELECT TOP ({n}) * FROM ({query.strip().rstrip(';')}) AS t"
        cur = conn.execute(wrapped)
        fpath = os.path.join(temp_dir, f"{prefix}query_result.csv")
        rows = cur.fetchall()
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([d[0] for d in cur.description])
            writer.writerows(rows)
        conn.close()
        return True, [{"name": os.path.basename(fpath), "path": fpath}]
    except Exception as e:
        return False, str(e)


def fetch_sql_dry_run(server, database, export_mode, tables_or_query, max_rows, temp_base):
    """
    Create a temp dir, fetch from SQL (tables or query) with max_rows, write CSVs.
    Returns (True, temp_dir) or (False, error_message).
    """
    os.makedirs(temp_base, exist_ok=True)
    subdir = os.path.join(temp_base, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    if export_mode == "tables":
        if not tables_or_query:
            return False, "No tables selected"
        ok, result = export_tables_top_n(server, database, tables_or_query, subdir, max_rows, filename_prefix="")
    else:
        query = (tables_or_query or "").strip()
        if not query:
            return False, "Query is empty"
        ok, result = export_query_top_n(server, database, query, subdir, max_rows, filename_prefix="")
    if not ok:
        return False, result
    return True, subdir


def export_sql_into_dir(server, database, export_mode, tables_or_query, max_rows, temp_dir):
    """
    Export SQL tables or query into existing temp_dir with sql_ filename prefix.
    Returns (True, None) or (False, error_message).
    """
    if export_mode == "tables":
        if not tables_or_query:
            return False, "No tables selected"
        ok, result = export_tables_top_n(
            server, database, tables_or_query, temp_dir, max_rows, filename_prefix="sql"
        )
    else:
        query = (tables_or_query or "").strip()
        if not query:
            return False, "Query is empty"
        ok, result = export_query_top_n(
            server, database, query, temp_dir, max_rows, filename_prefix="sql"
        )
    if not ok:
        return False, result
    return True, None


_CHUNK_SIZE = 10000


def export_tables_full(server, database, table_list, temp_dir, filename_prefix=""):
    """Export all rows from each table to CSV files in temp_dir. Streams in chunks to limit memory."""
    if not table_list:
        return False, "No tables selected"
    prefix = (filename_prefix or "").strip()
    if prefix and not prefix.endswith("_"):
        prefix = prefix + "_"
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        cur = conn.cursor()
        files = []
        for qual in table_list:
            parts = qual.split(".", 1)
            if len(parts) == 2:
                schema, name = parts
            else:
                schema, name = "dbo", qual
            safe_name = name.replace(" ", "_").replace(".", "_")
            fpath = os.path.join(temp_dir, f"{prefix}{safe_name}.csv")
            cur.execute(f"SELECT * FROM [{schema}].[{name}]")
            with open(fpath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([d[0] for d in cur.description])
                while True:
                    rows = cur.fetchmany(_CHUNK_SIZE)
                    if not rows:
                        break
                    writer.writerows(rows)
            files.append({"name": os.path.basename(fpath), "path": fpath})
        conn.close()
        return True, files
    except Exception as e:
        return False, str(e)


def export_query_full(server, database, query, temp_dir, filename_prefix=""):
    """Execute query and stream all rows to one CSV. No row limit."""
    prefix = (filename_prefix or "").strip()
    if prefix and not prefix.endswith("_"):
        prefix = prefix + "_"
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        wrapped = f"SELECT * FROM ({query.strip().rstrip(';')}) AS t"
        cur = conn.execute(wrapped)
        fpath = os.path.join(temp_dir, f"{prefix}query_result.csv")
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([d[0] for d in cur.description])
            while True:
                rows = cur.fetchmany(_CHUNK_SIZE)
                if not rows:
                    break
                writer.writerows(rows)
        conn.close()
        return True, [{"name": os.path.basename(fpath), "path": fpath}]
    except Exception as e:
        return False, str(e)


def export_sql_into_dir_full(server, database, export_mode, tables_or_query, temp_dir):
    """
    Export SQL tables or query into existing temp_dir with sql_ prefix; full data, no row limit.
    Returns (True, None) or (False, error_message).
    """
    if export_mode == "tables":
        if not tables_or_query:
            return False, "No tables selected"
        ok, _ = export_tables_full(
            server, database, tables_or_query, temp_dir, filename_prefix="sql"
        )
    else:
        query = (tables_or_query or "").strip()
        if not query:
            return False, "Query is empty"
        ok, _ = export_query_full(
            server, database, query, temp_dir, filename_prefix="sql"
        )
    if not ok:
        return False, _
    return True, None
