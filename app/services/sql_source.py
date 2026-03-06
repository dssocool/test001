"""
On-prem SQL Server via pyodbc: ActiveDirectoryIntegrated, TrustServerCertificate=yes.
"""
import csv
import os

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
    try:
        import pyodbc
        conn = pyodbc.connect(_conn_str(server, database))
        cur = conn.execute(query)
        fpath = os.path.join(temp_dir, "query_result.csv")
        rows = cur.fetchmany(10)
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([d[0] for d in cur.description])
            writer.writerows(rows)
        conn.close()
        return True, [{"name": "query_result.csv", "path": fpath}]
    except Exception as e:
        return False, str(e)
