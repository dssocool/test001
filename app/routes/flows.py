import json
import os
import uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app, session, jsonify

from app.models import get_domain, get_flow, get_flow_count, create_flow, update_flow, delete_flow
from app.services.delphix_flow import run_delphix_flow

flows_bp = Blueprint("flows_bp", __name__)
SESSION_FLOW_CONFIG = "flow_config"
SESSION_TEMP_DIR = "flow_temp_dir"


def _render_flow_step1(app, domain_id, flow_config, temp_dir, delphix_error=None, edit_flow_id=None, flow=None):
    """Re-render step 1 (e.g. when Delphix fails)."""
    domain = get_domain(app, domain_id)
    if not domain:
        return "Domain not found", 404
    default_flow_name = "Flow {}".format(get_flow_count(app, domain_id) + 1)
    return render_template(
        "flow_create.html",
        domain=domain,
        step=1,
        flow_config=flow_config or {},
        temp_dir=temp_dir or "",
        edit_flow_id=edit_flow_id,
        flow=flow,
        default_flow_name=default_flow_name,
        delphix_error=delphix_error,
    )


def _parse_has_header(value):
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return None


def _parse_end_of_record(value):
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in ("crlf", "\\r\\n", "windows", "win"):
        return "\r\n"
    if v in ("lf", "\\n", "linux", "unix"):
        return "\n"
    return None


def _handle_step1_local_upload(domain_id):
    """Handle step 1 submit when source is local: detect file, upload, build config, store in session. Returns True if handled."""
    if "file" not in request.files or not request.files["file"].filename:
        return False
    f = request.files["file"]
    chunk = f.read(8192)
    from app.services.file_detection import detect_file
    detected = detect_file(chunk)
    try:
        f.stream.seek(0)
    except Exception:
        pass
    file_type = request.form.get("local_file_type") or detected.get("file_type") or "csv"
    delimiter = request.form.get("local_delimiter")
    if delimiter is None or delimiter == "":
        delimiter = detected.get("delimiter", ",")
    has_header = _parse_has_header(request.form.get("local_has_header"))
    if has_header is None:
        has_header = detected.get("has_header", True)
    end_of_record = _parse_end_of_record(request.form.get("local_end_of_record"))
    if end_of_record is None:
        end_of_record = detected.get("end_of_record", "\n")
    temp_base = current_app.config["TEMP_BASE"]
    os.makedirs(temp_base, exist_ok=True)
    subdir = os.path.join(temp_base, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    from app.services.file_source import save_upload_top10
    ok, result = save_upload_top10(
        f, delimiter, subdir,
        has_header=has_header,
        end_of_record=end_of_record,
        file_type=file_type,
    )
    if not ok:
        return False
    session[SESSION_FLOW_CONFIG] = {
        "source_type": "local",
        "upload_name": f.filename,
        "file_type": file_type,
        "delimiter": delimiter,
        "has_header": has_header,
        "end_of_record": end_of_record,
        "detected_file_type": detected.get("file_type", "csv"),
        "detected_delimiter": detected.get("delimiter", ","),
        "detected_has_header": detected.get("has_header", True),
        "detected_end_of_record": detected.get("end_of_record", "\n"),
    }
    session[SESSION_TEMP_DIR] = subdir
    return True


@flows_bp.route("/upload-local", methods=["POST"])
def upload_local(domain_id):
    """Upload a local file for Option B; sets session and returns JSON. Does not redirect. Delphix is called on the next page."""
    domain = get_domain(current_app, domain_id)
    if not domain:
        return jsonify({"ok": False, "error": "Domain not found"}), 404
    if not _handle_step1_local_upload(domain_id):
        return jsonify({"ok": False, "error": "No file or invalid file"}), 400
    cfg = session.get(SESSION_FLOW_CONFIG) or {}
    detected = {
        "file_type": cfg.get("detected_file_type", cfg.get("file_type", "csv")),
        "delimiter": cfg.get("detected_delimiter", cfg.get("delimiter", ",")),
        "has_header": cfg.get("detected_has_header", True),
        "end_of_record": cfg.get("detected_end_of_record", "\n"),
    }
    return jsonify({
        "ok": True,
        "filename": cfg.get("upload_name", ""),
        "file_type": cfg.get("file_type", "csv"),
        "delimiter": cfg.get("delimiter", ","),
        "has_header": cfg.get("has_header", True),
        "end_of_record": cfg.get("end_of_record", "\n"),
        "detected": detected,
    })


@flows_bp.route("/update-local-config", methods=["POST"])
def update_local_config(domain_id):
    """Update local file config (file_type, delimiter, etc.) in session. Used when user adjusts file type after upload."""
    domain = get_domain(current_app, domain_id)
    if not domain:
        return jsonify({"ok": False, "error": "Domain not found"}), 404
    cfg = session.get(SESSION_FLOW_CONFIG) or {}
    if cfg.get("source_type") != "local":
        return jsonify({"ok": False, "error": "Not a local file flow"}), 400
    data = request.get_json(silent=True) or {}
    if "file_type" in data:
        v = (data.get("file_type") or "csv").strip().lower()
        if v in ("csv", "json", "xml", "parquet"):
            cfg["file_type"] = v
    if "delimiter" in data:
        cfg["delimiter"] = data["delimiter"] if data["delimiter"] is not None else ","
    if "has_header" in data:
        cfg["has_header"] = data["has_header"] in (True, "true", "1", "yes")
    if "end_of_record" in data:
        e = (data.get("end_of_record") or "\n")
        cfg["end_of_record"] = "\r\n" if e in ("\r\n", "crlf", "windows") else "\n"
    session[SESSION_FLOW_CONFIG] = cfg
    return jsonify({"ok": True})


@flows_bp.route("/run-dry-run", methods=["POST"])
def run_dry_run(domain_id):
    """Run Delphix synthetic data generation; called from step 2 when user clicks Dry Run."""
    domain = get_domain(current_app, domain_id)
    if not domain:
        return jsonify({"ok": False, "error": "Domain not found"}), 404
    cfg = session.get(SESSION_FLOW_CONFIG) or {}
    temp_dir = session.get(SESSION_TEMP_DIR) or ""
    if not temp_dir:
        return jsonify({"ok": False, "error": "No temp data. Complete step 1 first."}), 400
    ok, result = run_delphix_flow(temp_dir, cfg, current_app.config["INSTANCE_PATH"])
    if not ok:
        return jsonify({"ok": False, "error": result or "Delphix failed"}), 400
    cfg["delphix"] = result
    session[SESSION_FLOW_CONFIG] = cfg
    return jsonify({"ok": True, "delphix": result})


@flows_bp.route("/new", methods=["GET", "POST"])
def new(domain_id):
    domain = get_domain(current_app, domain_id)
    if not domain:
        return "Domain not found", 404

    if request.method == "POST":
        step = request.form.get("step", type=int, default=1)
        if step == 1:
            # Skip to next page (local file — upload will be on step 2 later)
            if request.form.get("skip_source") == "1":
                session[SESSION_FLOW_CONFIG] = {
                    "source_type": "local",
                    "skip_source_config": True,
                }
                session[SESSION_TEMP_DIR] = ""
                return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
            # Local file: upload on submit (no config/temp_dir in form); Delphix runs on step 2 when user clicks Dry Run
            if _handle_step1_local_upload(domain_id):
                return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
            # Local file: back from step 2 with no new file — keep existing session and go to step 2
            cfg = session.get(SESSION_FLOW_CONFIG) or {}
            if cfg.get("source_type") == "local" and session.get(SESSION_TEMP_DIR):
                return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
            # SQL/Blob: config and temp_dir from form; Delphix runs on step 2 when user clicks Dry Run
            config_json = request.form.get("config")
            temp_dir = request.form.get("temp_dir", "").strip()
            if config_json and temp_dir:
                try:
                    session[SESSION_FLOW_CONFIG] = json.loads(config_json)
                    session[SESSION_TEMP_DIR] = temp_dir
                except json.JSONDecodeError:
                    pass
            return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
        if step == 2:
            return redirect(url_for("flows_bp.new", domain_id=domain_id, step=3), code=303)
        if step == 3:
            # Prefer config from session so save always works
            cfg = session.get(SESSION_FLOW_CONFIG)
            if not cfg and request.form.get("config"):
                try:
                    cfg = json.loads(request.form["config"])
                except json.JSONDecodeError:
                    cfg = None
            if cfg is not None:
                create_flow(current_app, domain_id, request.form.get("name"), cfg)
            session.pop(SESSION_FLOW_CONFIG, None)
            session.pop(SESSION_TEMP_DIR, None)
            return redirect(url_for("main_bp.index"), code=303)

    step = request.args.get("step", type=int, default=1)
    flow_config = session.get(SESSION_FLOW_CONFIG) or {}
    temp_dir = session.get(SESSION_TEMP_DIR) or ""
    default_flow_name = "Flow {}".format(get_flow_count(current_app, domain_id) + 1)
    return render_template(
        "flow_create.html",
        domain=domain,
        step=step,
        flow_config=flow_config,
        temp_dir=temp_dir,
        edit_flow_id=None,
        flow=None,
        default_flow_name=default_flow_name,
        delphix_error=None,
    )


@flows_bp.route("/<int:flow_id>/edit", methods=["GET", "POST"])
def edit(domain_id, flow_id):
    domain = get_domain(current_app, domain_id)
    if not domain:
        return "Domain not found", 404
    flow = get_flow(current_app, flow_id)
    if not flow or flow["domain_id"] != domain_id:
        return "Flow not found", 404

    if request.method == "POST":
        step = request.form.get("step", type=int, default=1)
        if step == 1:
            if request.form.get("skip_source") == "1":
                session[SESSION_FLOW_CONFIG] = {
                    "source_type": "local",
                    "skip_source_config": True,
                }
                session[SESSION_TEMP_DIR] = ""
                return redirect(url_for("flows_bp.edit", domain_id=domain_id, flow_id=flow_id, step=2), code=303)
            if _handle_step1_local_upload(domain_id):
                return redirect(url_for("flows_bp.edit", domain_id=domain_id, flow_id=flow_id, step=2), code=303)
            cfg = session.get(SESSION_FLOW_CONFIG) or {}
            if cfg.get("source_type") == "local" and session.get(SESSION_TEMP_DIR):
                return redirect(url_for("flows_bp.edit", domain_id=domain_id, flow_id=flow_id, step=2), code=303)
            config_json = request.form.get("config")
            temp_dir = request.form.get("temp_dir", "").strip()
            if config_json and temp_dir:
                try:
                    session[SESSION_FLOW_CONFIG] = json.loads(config_json)
                    session[SESSION_TEMP_DIR] = temp_dir
                except json.JSONDecodeError:
                    pass
            return redirect(url_for("flows_bp.edit", domain_id=domain_id, flow_id=flow_id, step=2), code=303)
        if step == 2:
            return redirect(url_for("flows_bp.edit", domain_id=domain_id, flow_id=flow_id, step=3), code=303)
        if step == 3:
            cfg = session.get(SESSION_FLOW_CONFIG)
            if not cfg and request.form.get("config"):
                try:
                    cfg = json.loads(request.form["config"])
                except json.JSONDecodeError:
                    cfg = None
            if cfg is not None:
                name = request.form.get("name", "").strip()
                if not name:
                    name = flow.get("name")
                update_flow(current_app, flow_id, name, cfg)
            session.pop(SESSION_FLOW_CONFIG, None)
            session.pop(SESSION_TEMP_DIR, None)
            return redirect(url_for("main_bp.index"), code=303)

    step = request.args.get("step", type=int, default=1)
    flow_config = session.get(SESSION_FLOW_CONFIG) or {}
    temp_dir = session.get(SESSION_TEMP_DIR) or ""
    default_flow_name = "Flow {}".format(get_flow_count(current_app, domain_id) + 1)
    return render_template(
        "flow_create.html",
        domain=domain,
        step=step,
        flow_config=flow_config,
        temp_dir=temp_dir,
        edit_flow_id=flow_id,
        flow=flow,
        default_flow_name=default_flow_name,
        delphix_error=None,
    )


@flows_bp.route("/<int:flow_id>/delete", methods=["POST"])
def delete_flow_route(domain_id, flow_id):
    domain = get_domain(current_app, domain_id)
    if domain:
        flow = get_flow(current_app, flow_id)
        if flow and flow["domain_id"] == domain_id:
            delete_flow(current_app, flow_id)
    return redirect(url_for("main_bp.index"), code=303)
