"""API for Delphix execution status (used on Test & Dry Run step)."""
from flask import Blueprint, request, jsonify, current_app

from app.services.delphix_client import DelphixClientError, get_delphix_client, load_delphix_config

delphix_bp = Blueprint("delphix_bp", __name__)


@delphix_bp.route("/status", methods=["POST"])
def status():
    """
    Expects JSON body with keys profile_execution_id, masking_execution_id (optional).
    Returns { ok: true, profile: {...}, masking: {...} } or { ok: false, error: "..." }.
    """
    data = request.get_json(silent=True) or {}
    profile_exec_id = data.get("profile_execution_id")
    masking_exec_id = data.get("masking_execution_id")

    if not profile_exec_id and not masking_exec_id:
        return jsonify({"ok": True, "profile": None, "masking": None})

    config = load_delphix_config(current_app.config["INSTANCE_PATH"])
    if not config:
        return jsonify({"ok": False, "error": "Delphix not configured"}), 400

    try:
        client = get_delphix_client(config)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    profile_result = None
    masking_result = None

    if profile_exec_id:
        try:
            profile_result = client.get_execution(profile_exec_id)
        except DelphixClientError as e:
            profile_result = {"error": str(e), "status": "ERROR"}

    if masking_exec_id:
        try:
            masking_result = client.get_execution(masking_exec_id)
        except DelphixClientError as e:
            masking_result = {"error": str(e), "status": "ERROR"}

    return jsonify({
        "ok": True,
        "profile": profile_result,
        "masking": masking_result,
    })


@delphix_bp.route("/file-field-metadata", methods=["GET"])
def file_field_metadata():
    """
    GET ?file_format_id=... Returns Delphix file field metadata (responseList, _pageInfo)
    for algorithm setup display. file_format_id comes from flow config delphix.file_format_ids.
    """
    file_format_id = request.args.get("file_format_id", type=int)
    if file_format_id is None:
        return jsonify({"ok": False, "error": "file_format_id required"}), 400
    config = load_delphix_config(current_app.config["INSTANCE_PATH"])
    if not config:
        return jsonify({"ok": False, "error": "Delphix not configured"}), 400
    try:
        client = get_delphix_client(config)
        data = client.get_file_field_metadata(file_format_id, page_number=1)
    except DelphixClientError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    return jsonify({"ok": True, "responseList": data.get("responseList", []), "_pageInfo": data.get("_pageInfo", {})})
