import json
import logging
from typing import Any, Dict, List, Optional

import azure.functions as func

from backend.shared.storage import load_json, save_json


def _user_id_from_req(req: func.HttpRequest) -> str:
    user_id = (req.headers.get("X-User-Id") or "").strip()
    if not user_id:
        raise ValueError("X-User-Id header is required")
    return user_id


def _domains_blob_path() -> str:
    return "config/domains.json"


def _load_domains(user_id: str) -> List[Dict[str, Any]]:
    doc = load_json(user_id, _domains_blob_path()) or {}
    domains = doc.get("domains")
    if isinstance(domains, list):
        return domains
    return []


def _save_domains(user_id: str, domains: List[Dict[str, Any]]) -> None:
    save_json(user_id, _domains_blob_path(), {"domains": domains})


def _find_domain(domains: List[Dict[str, Any]], domain_id: int) -> Optional[Dict[str, Any]]:
    for d in domains:
        if int(d.get("id") or 0) == domain_id:
            return d
    return None


def _find_flow(domain: Dict[str, Any], flow_id: int) -> Optional[Dict[str, Any]]:
    flows = domain.get("flows") or []
    for f in flows:
        if int(f.get("id") or 0) == flow_id:
            return f
    return None


def main(req: func.HttpRequest) -> func.HttpResponse:  # type: ignore[override]
    """
    Dispatcher for:
      - /domains/{domainId}/flows
      - /flows/{flowId}
    Route templates should be defined in function.json for this function, for example:
      "route": "{*segments}"
    and dispatch based on segments.
    """
    logging.info("Flows function processed a request.")
    try:
        user_id = _user_id_from_req(req)
    except ValueError as e:
        return func.HttpResponse(json.dumps({"ok": False, "error": str(e)}), status_code=400, mimetype="application/json")

    path = (req.route_params.get("segments") or "").strip("/")
    method = req.method.upper()

    try:
        if path.startswith("domains/") and path.endswith("/flows"):
            # domains/{domainId}/flows
            parts = path.split("/")
            if len(parts) != 3:
                return func.HttpResponse(json.dumps({"ok": False, "error": "Invalid route"}), status_code=404, mimetype="application/json")
            try:
                domain_id = int(parts[1])
            except ValueError:
                return func.HttpResponse(json.dumps({"ok": False, "error": "Invalid domain id"}), status_code=400, mimetype="application/json")
            if method == "GET":
                return _handle_list_flows(user_id, domain_id)
            if method == "POST":
                return _handle_create_flow(user_id, domain_id, req)
            return func.HttpResponse(json.dumps({"ok": False, "error": "Method not allowed"}), status_code=405, mimetype="application/json")

        if path.startswith("flows/"):
            # flows/{flowId}
            parts = path.split("/")
            if len(parts) != 2:
                return func.HttpResponse(json.dumps({"ok": False, "error": "Invalid route"}), status_code=404, mimetype="application/json")
            try:
                flow_id = int(parts[1])
            except ValueError:
                return func.HttpResponse(json.dumps({"ok": False, "error": "Invalid flow id"}), status_code=400, mimetype="application/json")
            if method == "GET":
                return _handle_get_flow(user_id, flow_id)
            if method == "PUT":
                return _handle_update_flow(user_id, flow_id, req)
            if method == "DELETE":
                return _handle_delete_flow(user_id, flow_id)
            return func.HttpResponse(json.dumps({"ok": False, "error": "Method not allowed"}), status_code=405, mimetype="application/json")

        return func.HttpResponse(json.dumps({"ok": False, "error": "Not found"}), status_code=404, mimetype="application/json")
    except Exception as e:  # pragma: no cover
        logging.exception("Flows function error")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(e)}), status_code=500, mimetype="application/json")


def _handle_list_flows(user_id: str, domain_id: int) -> func.HttpResponse:
    domains = _load_domains(user_id)
    d = _find_domain(domains, domain_id)
    if not d:
        return func.HttpResponse(json.dumps({"ok": False, "error": "Domain not found"}), status_code=404, mimetype="application/json")
    flows = d.get("flows") or []
    return func.HttpResponse(json.dumps({"ok": True, "domain": d, "flows": flows}), mimetype="application/json")


def _handle_create_flow(user_id: str, domain_id: int, req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json() or {}
    except ValueError:
        data = {}
    name = (data.get("name") or "").strip()
    config = data.get("config") or {}
    if not isinstance(config, dict):
        config = {}

    domains = _load_domains(user_id)
    d = _find_domain(domains, domain_id)
    if not d:
        return func.HttpResponse(json.dumps({"ok": False, "error": "Domain not found"}), status_code=404, mimetype="application/json")

    flows = d.get("flows") or []
    next_id = max((f.get("id") or 0 for f in flows), default=0) + 1
    flow = {
        "id": next_id,
        "domain_id": domain_id,
        "name": name or None,
        "config": config,
    }
    flows.append(flow)
    d["flows"] = flows
    _save_domains(user_id, domains)
    return func.HttpResponse(json.dumps({"ok": True, "id": next_id}), status_code=201, mimetype="application/json")


def _handle_get_flow(user_id: str, flow_id: int) -> func.HttpResponse:
    domains = _load_domains(user_id)
    for d in domains:
        f = _find_flow(d, flow_id)
        if f:
            return func.HttpResponse(json.dumps({"ok": True, "flow": f}), mimetype="application/json")
    return func.HttpResponse(json.dumps({"ok": False, "error": "Flow not found"}), status_code=404, mimetype="application/json")


def _handle_update_flow(user_id: str, flow_id: int, req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json() or {}
    except ValueError:
        data = {}
    name = (data.get("name") or "").strip()
    config = data.get("config") or {}
    if not isinstance(config, dict):
        config = {}

    domains = _load_domains(user_id)
    for d in domains:
        f = _find_flow(d, flow_id)
        if not f:
            continue
        if name:
            f["name"] = name
        f["config"] = config
        _save_domains(user_id, domains)
        return func.HttpResponse(json.dumps({"ok": True}), mimetype="application/json")
    return func.HttpResponse(json.dumps({"ok": False, "error": "Flow not found"}), status_code=404, mimetype="application/json")


def _handle_delete_flow(user_id: str, flow_id: int) -> func.HttpResponse:
    domains = _load_domains(user_id)
    updated = False
    for d in domains:
        flows = d.get("flows") or []
        new_flows: List[Dict[str, Any]] = [f for f in flows if int(f.get("id") or 0) != flow_id]
        if len(new_flows) != len(flows):
            d["flows"] = new_flows
            updated = True
    if not updated:
        return func.HttpResponse(json.dumps({"ok": False, "error": "Flow not found"}), status_code=404, mimetype="application/json")
    _save_domains(user_id, domains)
    return func.HttpResponse(json.dumps({"ok": True}), mimetype="application/json")

