import csv
import io
import os
import re

from flask import Blueprint, request, jsonify, current_app

from app.services.azure_blob import download_blob
from app.services.delphix_client import load_delphix_config

dry_run_bp = Blueprint("dry_run_bp", __name__)

MAX_MASKED_ROWS = 10

# Temp header files (Delphix file format) have names like {base}_{8 hex}.csv — exclude from dry-run list
_HEADER_FILE_PATTERN = re.compile(r"^.*_[0-9a-f]{8}\.csv$", re.IGNORECASE)


@dry_run_bp.route("/files", methods=["GET"])
def files():
    temp_dir = request.args.get("temp_dir", "").strip()
    if not temp_dir or not os.path.isdir(temp_dir):
        return jsonify({"ok": False, "error": "Invalid temp_dir"}), 400
    max_rows = request.args.get("max_rows", type=int, default=10)
    if max_rows is None or max_rows < 1:
        max_rows = 1
    elif max_rows > 10:
        max_rows = 10
    result = []
    for name in sorted(os.listdir(temp_dir)):
        path = os.path.join(temp_dir, name)
        if not os.path.isfile(path):
            continue
        if not name.lower().endswith(".csv"):
            continue
        if _HEADER_FILE_PATTERN.match(name):
            continue
        rows = []
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fp:
                reader = csv.reader(fp)
                for i, row in enumerate(reader):
                    if i > max_rows:
                        break
                    rows.append(row)
        except Exception as e:
            rows = [["Error reading file: " + str(e)]]
        result.append({"name": name, "path": path, "rows": rows})
    return jsonify({"ok": True, "files": result})


@dry_run_bp.route("/masked-file", methods=["GET"])
def masked_file():
    """
    Download the masked blob from Azure (by name) and return first N rows as JSON.
    Uses Delphix config from instance path for Azure credentials.
    Query: blob_name (required). Optional: delimiter (default ",").
    """
    blob_name = (request.args.get("blob_name") or "").strip()
    if not blob_name:
        return jsonify({"ok": False, "error": "blob_name required"}), 400
    instance_path = current_app.config.get("INSTANCE_PATH")
    if not instance_path:
        return jsonify({"ok": False, "error": "Instance path not configured"}), 500
    config = load_delphix_config(instance_path)
    if not config or not config.get("azure"):
        return jsonify({"ok": False, "error": "Delphix not configured"}), 400
    azure = config["azure"]
    delimiter = (request.args.get("delimiter") or ",").strip() or ","
    try:
        content = download_blob(
            azure["account_name"],
            azure["container_name"],
            azure["access_key"],
            blob_name,
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    max_rows = request.args.get("max_rows", type=int, default=MAX_MASKED_ROWS)
    if max_rows is None or max_rows < 1:
        max_rows = 1
    if max_rows > MAX_MASKED_ROWS:
        max_rows = MAX_MASKED_ROWS
    text = content.decode("utf-8", errors="replace")
    rows = []
    try:
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        for i, row in enumerate(reader):
            if i > max_rows:
                break
            rows.append(row)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Parse error: {e}"}), 500
    return jsonify({"ok": True, "name": blob_name, "rows": rows})
