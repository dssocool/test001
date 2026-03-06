from flask import Blueprint, request, jsonify, current_app

sql_bp = Blueprint("sql_bp", __name__)


@sql_bp.route("/validate", methods=["POST"])
def validate():
    data = request.get_json() or {}
    server = data.get("server", "").strip()
    database = data.get("database", "").strip()
    if not server or not database:
        return jsonify({"ok": False, "error": "Server and database required"}), 400
    from app.services.sql_source import validate_connection
    ok, err = validate_connection(server, database)
    if not ok:
        return jsonify({"ok": False, "error": err}), 400
    return jsonify({"ok": True})


@sql_bp.route("/tables", methods=["GET"])
def tables():
    server = request.args.get("server", "").strip()
    database = request.args.get("database", "").strip()
    if not server or not database:
        return jsonify({"ok": False, "error": "Server and database required"}), 400
    from app.services.sql_source import list_tables
    ok, result = list_tables(server, database)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    return jsonify({"ok": True, "tables": result})


@sql_bp.route("/export-tables", methods=["POST"])
def export_tables():
    data = request.get_json() or {}
    server = data.get("server", "").strip()
    database = data.get("database", "").strip()
    tables_list = data.get("tables") or []
    if not server or not database or not tables_list:
        return jsonify({"ok": False, "error": "Server, database and tables required"}), 400
    import tempfile
    import uuid
    temp_dir = current_app.config["TEMP_BASE"]
    import os
    os.makedirs(temp_dir, exist_ok=True)
    subdir = os.path.join(temp_dir, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    from app.services.sql_source import export_tables_top10
    ok, result = export_tables_top10(server, database, tables_list, subdir)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    return jsonify({"ok": True, "temp_dir": subdir, "files": result})


@sql_bp.route("/export-query", methods=["POST"])
def export_query():
    data = request.get_json() or {}
    server = data.get("server", "").strip()
    database = data.get("database", "").strip()
    query = data.get("query", "").strip()
    if not server or not database or not query:
        return jsonify({"ok": False, "error": "Server, database and query required"}), 400
    import tempfile
    import uuid
    temp_dir = current_app.config["TEMP_BASE"]
    import os
    os.makedirs(temp_dir, exist_ok=True)
    subdir = os.path.join(temp_dir, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    from app.services.sql_source import export_query_top10
    ok, result = export_query_top10(server, database, query, subdir)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    return jsonify({"ok": True, "temp_dir": subdir, "files": result})
