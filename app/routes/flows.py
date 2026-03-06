import json
import os
import uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app, session

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
    }
    session[SESSION_TEMP_DIR] = subdir
    return True


@flows_bp.route("/new", methods=["GET", "POST"])
def new(domain_id):
    domain = get_domain(current_app, domain_id)
    if not domain:
        return "Domain not found", 404

    if request.method == "POST":
        step = request.form.get("step", type=int, default=1)
        if step == 1:
            # Local file: upload on submit (no config/temp_dir in form)
            if _handle_step1_local_upload(domain_id):
                cfg = session.get(SESSION_FLOW_CONFIG) or {}
                temp_dir = session.get(SESSION_TEMP_DIR) or ""
                if cfg.get("file_type") == "csv":
                    ok, result = run_delphix_flow(temp_dir, cfg, current_app.config["INSTANCE_PATH"])
                    if not ok:
                        return _render_flow_step1(current_app, domain_id, cfg, temp_dir, result)
                    cfg["delphix"] = result
                    session[SESSION_FLOW_CONFIG] = cfg
                return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
            # Local file: back from step 2 with no new file — keep existing session and go to step 2
            cfg = session.get(SESSION_FLOW_CONFIG) or {}
            if cfg.get("source_type") == "local" and session.get(SESSION_TEMP_DIR):
                return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
            # SQL/Blob: config and temp_dir from form
            config_json = request.form.get("config")
            temp_dir = request.form.get("temp_dir", "").strip()
            if config_json and temp_dir:
                try:
                    session[SESSION_FLOW_CONFIG] = json.loads(config_json)
                    session[SESSION_TEMP_DIR] = temp_dir
                    cfg = session.get(SESSION_FLOW_CONFIG) or {}
                    ok, result = run_delphix_flow(temp_dir, cfg, current_app.config["INSTANCE_PATH"])
                    if not ok:
                        return _render_flow_step1(current_app, domain_id, cfg, temp_dir, result)
                    cfg["delphix"] = result
                    session[SESSION_FLOW_CONFIG] = cfg
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
            if _handle_step1_local_upload(domain_id):
                cfg = session.get(SESSION_FLOW_CONFIG) or {}
                temp_dir = session.get(SESSION_TEMP_DIR) or ""
                ok, result = run_delphix_flow(temp_dir, cfg, current_app.config["INSTANCE_PATH"])
                if not ok:
                    return _render_flow_step1(
                        current_app, domain_id, cfg, temp_dir, result,
                        edit_flow_id=flow_id, flow=flow,
                    )
                cfg["delphix"] = result
                session[SESSION_FLOW_CONFIG] = cfg
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
                    cfg = session.get(SESSION_FLOW_CONFIG) or {}
                    ok, result = run_delphix_flow(temp_dir, cfg, current_app.config["INSTANCE_PATH"])
                    if not ok:
                        return _render_flow_step1(
                            current_app, domain_id, cfg, temp_dir, result,
                            edit_flow_id=flow_id, flow=flow,
                        )
                    cfg["delphix"] = result
                    session[SESSION_FLOW_CONFIG] = cfg
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
