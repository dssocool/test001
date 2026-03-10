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


def export_tables_top_n(server, database, table_list, temp_dir, n):
    """Export up to n rows per table to CSV files in temp_dir. Returns (True, files) or (False, error)."""
    if not table_list:
        return False, "No tables selected"
    if n is None or n < 1:
        n = 1
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


def export_query_top_n(server, database, query, temp_dir, n):
    """Wrap query in SELECT TOP (n) * FROM (query) AS t, execute, write one CSV. Returns (True, files) or (False, error)."""
    if n is None or n < 1:
        n = 1
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        wrapped = f"SELECT TOP ({n}) * FROM ({query.strip().rstrip(';')}) AS t"
        cur = conn.execute(wrapped)
        fpath = os.path.join(temp_dir, "query_result.csv")
        rows = cur.fetchall()
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([d[0] for d in cur.description])
            writer.writerows(rows)
        conn.close()
        return True, [{"name": "query_result.csv", "path": fpath}]
    except Exception as e:
        return False, str(e)


def export_tables_top_n_prefixed(server, database, table_list, temp_dir, n, prefix="sql"):
    """Like export_tables_top_n but each file is named {prefix}_{safe_table}.csv for merge."""
    if not table_list:
        return False, "No tables selected"
    if n is None or n < 1:
        n = 1
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
            fpath = os.path.join(temp_dir, f"{prefix}_{safe_name}.csv")
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


def export_query_top_n_prefixed(server, database, query, temp_dir, n, prefix="sql"):
    """Like export_query_top_n but writes {prefix}_query_result.csv."""
    if n is None or n < 1:
        n = 1
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        wrapped = f"SELECT TOP ({n}) * FROM ({query.strip().rstrip(';')}) AS t"
        cur = conn.execute(wrapped)
        fpath = os.path.join(temp_dir, f"{prefix}_query_result.csv")
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
        ok, result = export_tables_top_n(server, database, tables_or_query, subdir, max_rows)
    else:
        query = (tables_or_query or "").strip()
        if not query:
            return False, "Query is empty"
        ok, result = export_query_top_n(server, database, query, subdir, max_rows)
    if not ok:
        return False, result
    return True, subdir
