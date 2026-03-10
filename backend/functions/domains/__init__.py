import json
import logging
from typing import Any, Dict, List

import azure.functions as func

from backend.shared.storage import load_json, save_json


def _user_id_from_req(req: func.HttpRequest) -> str:
    user_id = (req.headers.get("X-User-Id") or "").strip()
    if not user_id:
        raise ValueError("X-User-Id header is required")
    return user_id


def _domains_blob_path() -> str:
    # Single JSON document per user listing domains and embedded flows.
    return "config/domains.json"


def _load_domains(user_id: str) -> List[Dict[str, Any]]:
    doc = load_json(user_id, _domains_blob_path()) or {}
    domains = doc.get("domains")
    if isinstance(domains, list):
        return domains
    return []


def _save_domains(user_id: str, domains: List[Dict[str, Any]]) -> None:
    save_json(user_id, _domains_blob_path(), {"domains": domains})


def main(req: func.HttpRequest) -> func.HttpResponse:  # type: ignore[override]
    """
    Dispatcher for /domains and /domains/{id} based on method and route parameters.
    The route template should be defined in function.json as:
      "route": "domains/{domainId?}"
    """
    logging.info("Domains function processed a request.")
    try:
        user_id = _user_id_from_req(req)
    except ValueError as e:
        return func.HttpResponse(json.dumps({"ok": False, "error": str(e)}), status_code=400, mimetype="application/json")

    domain_id_raw = req.route_params.get("domainId")
    method = req.method.upper()

    try:
        if not domain_id_raw:
            if method == "GET":
                return _handle_list_domains(user_id)
            if method == "POST":
                return _handle_create_domain(user_id, req)
            return func.HttpResponse(json.dumps({"ok": False, "error": "Method not allowed"}), status_code=405, mimetype="application/json")

        # domainId specified
        try:
            domain_id = int(domain_id_raw)
        except ValueError:
            return func.HttpResponse(json.dumps({"ok": False, "error": "Invalid domain id"}), status_code=400, mimetype="application/json")

        if method == "GET":
            return _handle_get_domain(user_id, domain_id)
        if method == "PUT":
            return _handle_update_domain(user_id, domain_id, req)
        if method == "DELETE":
            return _handle_delete_domain(user_id, domain_id)

        return func.HttpResponse(json.dumps({"ok": False, "error": "Method not allowed"}), status_code=405, mimetype="application/json")
    except Exception as e:  # pragma: no cover - generic safety net
        logging.exception("Domains function error")
        return func.HttpResponse(
            json.dumps({"ok": False, "error": str(e)}),
            status_code=500,
            mimetype="application/json",
        )


def _handle_list_domains(user_id: str) -> func.HttpResponse:
    domains = _load_domains(user_id)
    return func.HttpResponse(json.dumps({"ok": True, "domains": domains}), mimetype="application/json")


def _handle_create_domain(user_id: str, req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json() or {}
    except ValueError:
        data = {}
    name = (data.get("name") or "").strip()
    description = (data.get("description") or "").strip()
    data_generation_key = (data.get("data_generation_key") or "").strip()
    if not name:
        return func.HttpResponse(json.dumps({"ok": False, "error": "Name is required"}), status_code=400, mimetype="application/json")

    domains = _load_domains(user_id)
    next_id = max((d.get("id") or 0 for d in domains), default=0) + 1
    domain = {
        "id": next_id,
        "name": name,
        "description": description,
        "data_generation_key": data_generation_key,
        "flows": [],
    }
    domains.append(domain)
    _save_domains(user_id, domains)
    return func.HttpResponse(json.dumps({"ok": True, "id": next_id}), status_code=201, mimetype="application/json")


def _find_domain(domains, domain_id: int):
    for d in domains:
        if int(d.get("id") or 0) == domain_id:
            return d
    return None


def _handle_get_domain(user_id: str, domain_id: int) -> func.HttpResponse:
    domains = _load_domains(user_id)
    d = _find_domain(domains, domain_id)
    if not d:
        return func.HttpResponse(json.dumps({"ok": False, "error": "Domain not found"}), status_code=404, mimetype="application/json")
    return func.HttpResponse(json.dumps({"ok": True, "domain": d}), mimetype="application/json")


def _handle_update_domain(user_id: str, domain_id: int, req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json() or {}
    except ValueError:
        data = {}
    name = (data.get("name") or "").strip()
    description = (data.get("description") or "").strip()
    data_generation_key = (data.get("data_generation_key") or "").strip()

    domains = _load_domains(user_id)
    d = _find_domain(domains, domain_id)
    if not d:
        return func.HttpResponse(json.dumps({"ok": False, "error": "Domain not found"}), status_code=404, mimetype="application/json")
    if name:
        d["name"] = name
    d["description"] = description
    d["data_generation_key"] = data_generation_key
    _save_domains(user_id, domains)
    return func.HttpResponse(json.dumps({"ok": True}), mimetype="application/json")


def _handle_delete_domain(user_id: str, domain_id: int) -> func.HttpResponse:
    domains = _load_domains(user_id)
    new_domains = [d for d in domains if int(d.get("id") or 0) != domain_id]
    if len(new_domains) == len(domains):
        return func.HttpResponse(json.dumps({"ok": False, "error": "Domain not found"}), status_code=404, mimetype="application/json")
    _save_domains(user_id, new_domains)
    return func.HttpResponse(json.dumps({"ok": True}), mimetype="application/json")

