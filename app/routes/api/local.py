from flask import Blueprint, request, jsonify, current_app
import os
import uuid

local_bp = Blueprint("local_bp", __name__)


@local_bp.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"ok": False, "error": "No filename"}), 400
    delimiter = request.form.get("delimiter", ",")
    file_type = (request.form.get("file_type") or "csv").strip().lower()
    has_header = request.form.get("has_header", "1") in ("1", "true", "yes")
    end_of_record = "\r\n" if request.form.get("end_of_record", "").lower() in ("crlf", "windows") else "\n"
    temp_dir = current_app.config["TEMP_BASE"]
    os.makedirs(temp_dir, exist_ok=True)
    subdir = os.path.join(temp_dir, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    from app.services.file_source import save_upload_top10
    ok, result = save_upload_top10(f, delimiter, subdir, has_header=has_header, end_of_record=end_of_record, file_type=file_type)
    if not ok:
        return jsonify({"ok": False, "error": result}), 400
    return jsonify({"ok": True, "temp_dir": subdir, "files": result})
