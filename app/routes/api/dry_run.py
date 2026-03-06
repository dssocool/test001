from flask import Blueprint, request, jsonify
import os

dry_run_bp = Blueprint("dry_run_bp", __name__)


@dry_run_bp.route("/files", methods=["GET"])
def files():
    temp_dir = request.args.get("temp_dir", "").strip()
    if not temp_dir or not os.path.isdir(temp_dir):
        return jsonify({"ok": False, "error": "Invalid temp_dir"}), 400
    result = []
    for name in sorted(os.listdir(temp_dir)):
        path = os.path.join(temp_dir, name)
        if not os.path.isfile(path):
            continue
        rows = []
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fp:
                import csv
                reader = csv.reader(fp)
                for i, row in enumerate(reader):
                    if i >= 10:
                        break
                    rows.append(row)
        except Exception as e:
            rows = [["Error reading file: " + str(e)]]
        result.append({"name": name, "path": path, "rows": rows})
    return jsonify({"ok": True, "files": result})
