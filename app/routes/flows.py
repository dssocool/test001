import copy
import json
import os
import uuid
from flask import Blueprint, render_template, request, redirect, url_for, current_app, session, jsonify

from app.auth import current_user_oid
from app.models import get_domain, get_flow, get_flow_count, create_flow, update_flow, delete_flow
from app.services.delphix_flow import run_delphix_flow
from app.services.flow_config_persist import persist_flow_config

flows_bp = Blueprint("flows_bp", __name__)
SESSION_FLOW_CONFIG = "flow_config"
SESSION_TEMP_DIR = "flow_temp_dir"


def _render_flow_step1(app, domain_id, flow_config, temp_dir, delphix_error=None, edit_flow_id=None, flow=None):
    """Re-render step 1 (e.g. when Delphix fails)."""
    domain = get_domain(app, domain_id, user_oid=current_user_oid())
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
    cfg = session.get(SESSION_FLOW_CONFIG) or {}
    cfg["local"] = {
        "upload_name": f.filename,
        "file_type": file_type,
        "delimiter": delimiter,
        "has_header": has_header,
        "end_of_record": end_of_record,
        "temp_dir": subdir,
    }
    cfg["source_type"] = "multi" if (cfg.get("sql") or cfg.get("blob")) else "local"
    session[SESSION_FLOW_CONFIG] = cfg
    session[SESSION_TEMP_DIR] = subdir
    return True


@flows_bp.route("/upload-local", methods=["POST"])
def upload_local(domain_id):
    """Upload a local file (Local file source tab); sets session and returns JSON. Does not redirect."""
    domain = get_domain(current_app, domain_id, user_oid=current_user_oid())
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
    domain = get_domain(current_app, domain_id, user_oid=current_user_oid())
    if not domain:
        return jsonify({"ok": False, "error": "Domain not found"}), 404
    cfg = session.get(SESSION_FLOW_CONFIG) or {}
    if not cfg.get("local") and cfg.get("source_type") != "local":
        return jsonify({"ok": False, "error": "Not a local file flow"}), 400
    data = request.get_json(silent=True) or {}
    loc = cfg.get("local") if isinstance(cfg.get("local"), dict) else cfg
    if "file_type" in data:
        v = (data.get("file_type") or "csv").strip().lower()
        if v in ("csv", "json", "xml", "parquet"):
            if isinstance(cfg.get("local"), dict):
                cfg["local"]["file_type"] = v
            cfg["file_type"] = v
    if "delimiter" in data:
        d = data["delimiter"] if data["delimiter"] is not None else ","
        if isinstance(cfg.get("local"), dict):
            cfg["local"]["delimiter"] = d
        cfg["delimiter"] = d
    if "has_header" in data:
        h = data["has_header"] in (True, "true", "1", "yes")
        if isinstance(cfg.get("local"), dict):
            cfg["local"]["has_header"] = h
        cfg["has_header"] = h
    if "end_of_record" in data:
        e = (data.get("end_of_record") or "\n")
        e = "\r\n" if e in ("\r\n", "crlf", "windows") else "\n"
        if isinstance(cfg.get("local"), dict):
            cfg["local"]["end_of_record"] = e
        cfg["end_of_record"] = e
    session[SESSION_FLOW_CONFIG] = cfg
    return jsonify({"ok": True})


@flows_bp.route("/run-dry-run", methods=["POST"])
def run_dry_run(domain_id):
    """Run Delphix synthetic data generation; called from step 2 when user clicks Dry Run.
    Supports multi-source: SQL + Blob + Local aggregated into one temp_dir with prefixed CSV names."""
    domain = get_domain(current_app, domain_id, user_oid=current_user_oid())
    if not domain:
        return jsonify({"ok": False, "error": "Domain not found"}), 404
    cfg = session.get(SESSION_FLOW_CONFIG) or {}
    session_temp_dir = session.get(SESSION_TEMP_DIR) or ""

    max_rows = 10
    if request.get_data():
        try:
            body = request.get_json(silent=True) or {}
            max_rows = body.get("max_rows", 10)
            if max_rows is not None:
                max_rows = max(1, min(10, int(max_rows)))
        except (TypeError, ValueError):
            pass

    from app.services.flow_config_sources import (
        get_source_blocks,
        has_any_source,
        copy_local_csvs_into_dir,
    )

    sql_block, blob_block, local_block = get_source_blocks(cfg, session_temp_dir)
    if not has_any_source(cfg, session_temp_dir):
        return jsonify({"ok": False, "error": "No data source configured. Complete step 1 first."}), 400

    temp_base = current_app.config["TEMP_BASE"]
    os.makedirs(temp_base, exist_ok=True)
    combined_dir = os.path.join(temp_base, str(uuid.uuid4()))
    os.makedirs(combined_dir, exist_ok=True)

    if sql_block:
        from app.services.sql_source import export_sql_into_dir
        tables_or_query = (
            sql_block["tables"] if sql_block.get("export_mode") == "tables" else sql_block.get("query", "")
        )
        ok, err = export_sql_into_dir(
            sql_block["server"],
            sql_block["database"],
            sql_block.get("export_mode") or "tables",
            tables_or_query,
            max_rows,
            combined_dir,
        )
        if not ok:
            return jsonify({"ok": False, "error": err or "SQL export failed"}), 400

    if blob_block:
        from app.services.blob_source import export_blob_into_dir
        ok, err = export_blob_into_dir(
            blob_block["account_name"],
            blob_block["container"],
            blob_block["key"],
            blob_block["selected_blobs"],
            blob_block.get("delimiter") or ",",
            max_rows,
            combined_dir,
        )
        if not ok:
            return jsonify({"ok": False, "error": err or "Blob export failed"}), 400

    if local_block:
        local_td = local_block.get("temp_dir") or session_temp_dir
        ok, err = copy_local_csvs_into_dir(local_td, combined_dir, prefix="local")
        if not ok:
            return jsonify({"ok": False, "error": err or "Local copy failed"}), 400

    if not os.listdir(combined_dir):
        return jsonify({"ok": False, "error": "No CSV files produced. Complete step 1 first."}), 400

    temp_dir = combined_dir
    session[SESSION_TEMP_DIR] = temp_dir
    if sql_block and not cfg.get("sql"):
        cfg["sql"] = {k: v for k, v in sql_block.items() if k != "key"}
    if blob_block and not cfg.get("blob"):
        cfg["blob"] = {k: v for k, v in blob_block.items() if k != "key"}
    if local_block and not cfg.get("local"):
        cfg["local"] = {k: v for k, v in local_block.items() if k != "temp_dir"}
    cfg["source_type"] = "multi" if sum(1 for b in (sql_block, blob_block, local_block) if b) > 1 else (
        "sql" if sql_block else "blob" if blob_block else "local"
    )
    delimiter = ","
    if blob_block:
        delimiter = blob_block.get("delimiter") or ","
    elif cfg.get("delimiter"):
        delimiter = cfg.get("delimiter")
    cfg["delimiter"] = delimiter

    data_generation_key = domain.get("data_generation_key") or ""
    ok, result = run_delphix_flow(
        temp_dir,
        cfg,
        current_app.config["INSTANCE_PATH"],
        data_generation_key=data_generation_key,
    )
    if not ok:
        return jsonify({"ok": False, "error": result or "Delphix failed"}), 400
    cfg["delphix"] = result
    session[SESSION_FLOW_CONFIG] = cfg
    return jsonify({"ok": True, "delphix": result, "temp_dir": temp_dir})


@flows_bp.route("/new", methods=["GET", "POST"])
def new(domain_id):
    domain = get_domain(current_app, domain_id, user_oid=current_user_oid())
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
            if (cfg.get("source_type") == "local" or cfg.get("local")) and session.get(SESSION_TEMP_DIR):
                return redirect(url_for("flows_bp.new", domain_id=domain_id, step=2), code=303)
            # SQL/Blob: config and temp_dir from form; SQL may have empty temp_dir (fetched on step 2)
            config_json = request.form.get("config")
            temp_dir = request.form.get("temp_dir", "").strip()
            if config_json:
                try:
                    incoming = json.loads(config_json)
                    cfg = session.get(SESSION_FLOW_CONFIG) or {}
                    if incoming.get("sql"):
                        cfg["sql"] = incoming["sql"]
                    if incoming.get("blob"):
                        cfg["blob"] = incoming["blob"]
                    if incoming.get("source_type") == "sql" and not incoming.get("sql"):
                        cfg["sql"] = {k: incoming[k] for k in ("server", "database", "export_mode", "tables", "query", "delimiter") if k in incoming}
                    if incoming.get("source_type") == "blob" and not incoming.get("blob"):
                        cfg["blob"] = {k: incoming[k] for k in ("account_name", "container", "key", "selected_blobs", "prefix", "delimiter", "file_type") if k in incoming}
                    if cfg.get("sql") or cfg.get("blob") or cfg.get("local"):
                        cfg["source_type"] = "multi" if sum(1 for k in ("sql", "blob", "local") if cfg.get(k)) > 1 else (
                            "sql" if cfg.get("sql") else "blob" if cfg.get("blob") else "local"
                        )
                    session[SESSION_FLOW_CONFIG] = cfg
                    if not cfg.get("local"):
                        session[SESSION_TEMP_DIR] = temp_dir if temp_dir else ""
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
                to_save = persist_flow_config(cfg, existing=None)
                create_flow(current_app, domain_id, request.form.get("name"), to_save)
            session.pop(SESSION_FLOW_CONFIG, None)
            session.pop(SESSION_TEMP_DIR, None)
            return redirect(url_for("main_bp.index"), code=303)

    # Starting a brand-new flow from main page: clear session so step 1 is blank (no prior upload/config)
    if request.args.get("fresh"):
        session.pop(SESSION_FLOW_CONFIG, None)
        session.pop(SESSION_TEMP_DIR, None)
        return redirect(url_for("flows_bp.new", domain_id=domain_id), code=303)

    step = request.args.get("step", type=int, default=1)
    # New flow step 1 without resume: always start blank (session otherwise keeps prior upload/config).
    # Step 2 "Back" uses ?step=1&resume=1 so wizard state is preserved.
    if step == 1 and not request.args.get("resume"):
        session.pop(SESSION_FLOW_CONFIG, None)
        session.pop(SESSION_TEMP_DIR, None)
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
    domain = get_domain(current_app, domain_id, user_oid=current_user_oid())
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
            if (cfg.get("source_type") == "local" or cfg.get("local")) and session.get(SESSION_TEMP_DIR):
                return redirect(url_for("flows_bp.edit", domain_id=domain_id, flow_id=flow_id, step=2), code=303)
            config_json = request.form.get("config")
            temp_dir = request.form.get("temp_dir", "").strip()
            if config_json:
                try:
                    incoming = json.loads(config_json)
                    cfg = session.get(SESSION_FLOW_CONFIG) or {}
                    if incoming.get("sql"):
                        cfg["sql"] = incoming["sql"]
                    if incoming.get("blob"):
                        cfg["blob"] = incoming["blob"]
                    if incoming.get("source_type") == "sql" and not incoming.get("sql"):
                        cfg["sql"] = {k: incoming[k] for k in ("server", "database", "export_mode", "tables", "query", "delimiter") if k in incoming}
                    if incoming.get("source_type") == "blob" and not incoming.get("blob"):
                        cfg["blob"] = {k: incoming[k] for k in ("account_name", "container", "key", "selected_blobs", "prefix", "delimiter", "file_type") if k in incoming}
                    if cfg.get("sql") or cfg.get("blob") or cfg.get("local"):
                        cfg["source_type"] = "multi" if sum(1 for k in ("sql", "blob", "local") if cfg.get(k)) > 1 else (
                            "sql" if cfg.get("sql") else "blob" if cfg.get("blob") else "local"
                        )
                    session[SESSION_FLOW_CONFIG] = cfg
                    if not cfg.get("local"):
                        session[SESSION_TEMP_DIR] = temp_dir if temp_dir else ""
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
                to_save = persist_flow_config(cfg, existing=flow.get("config"))
                update_flow(current_app, flow_id, name, to_save)
            session.pop(SESSION_FLOW_CONFIG, None)
            session.pop(SESSION_TEMP_DIR, None)
            return redirect(url_for("main_bp.index"), code=303)

    step = request.args.get("step", type=int, default=1)
    # Seed session from saved flow so step 1/2/3 show configured source without re-entering
    if not session.get(SESSION_FLOW_CONFIG) and flow.get("config"):
        session[SESSION_FLOW_CONFIG] = copy.deepcopy(flow["config"])
    if session.get(SESSION_FLOW_CONFIG) and not session.get(SESSION_TEMP_DIR):
        session[SESSION_TEMP_DIR] = ""
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
    domain = get_domain(current_app, domain_id, user_oid=current_user_oid())
    if domain:
        flow = get_flow(current_app, flow_id)
        if flow and flow["domain_id"] == domain_id:
            delete_flow(current_app, flow_id)
    return redirect(url_for("main_bp.index"), code=303)
