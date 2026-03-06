from flask import Blueprint, request, jsonify, current_app

blob_bp = Blueprint("blob_bp", __name__)


@blob_bp.route("/validate", methods=["POST"])
def validate():
    data = request.get_json() or {}
    account_name = data.get("account_name", "").strip()
    container = data.get("container", "").strip()
    key = data.get("key", "").strip()
    if not account_name or not container or not key:
        return jsonify({"ok": False, "error": "Account name, container and key required"}), 400
    from app.services.blob_source import validate_and_list
    ok, result = validate_and_list(account_name, container, key)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    return jsonify({"ok": True, "blobs": result})


@blob_bp.route("/prepare-dry-run", methods=["POST"])
def prepare_dry_run():
    data = request.get_json() or {}
    account_name = data.get("account_name", "").strip()
    container = data.get("container", "").strip()
    key = data.get("key", "").strip()
    prefix = (data.get("prefix") or "").strip()
    file_type = (data.get("file_type") or "csv").strip().lower()
    delimiter = data.get("delimiter", ",")
    selected_blobs = data.get("selected_blobs") or []
    if not account_name or not container or not key:
        return jsonify({"ok": False, "error": "Account name, container and key required"}), 400
    import os
    import uuid
    temp_dir = current_app.config["TEMP_BASE"]
    os.makedirs(temp_dir, exist_ok=True)
    subdir = os.path.join(temp_dir, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    from app.services.blob_source import download_top10_rows
    ok, result = download_top10_rows(account_name, container, key, selected_blobs, delimiter, subdir)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    return jsonify({"ok": True, "temp_dir": subdir, "files": result})
