import os
from typing import Any, Dict, List, Optional

import requests
from flask import current_app


def _base_url() -> str:
    url = current_app.config.get("BACKEND_BASE_URL") or os.environ.get("BACKEND_BASE_URL") or ""
    return url.rstrip("/")


def _headers(user_oid: Optional[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {"Content-Type": "application/json"}
    if user_oid:
        headers["X-User-Id"] = str(user_oid)
    return headers


def list_domains(user_oid: Optional[str]) -> List[Dict[str, Any]]:
    """
    Call backend /domains and return the domains list. Falls back to empty list on error.
    """
    base = _base_url()
    if not base or not user_oid:
        # Until backend is fully deployed, keep returning empty list when no user or no backend URL.
        return []
    url = f"{base}/domains"
    try:
        resp = requests.get(url, headers=_headers(user_oid), timeout=10)
        data = resp.json()
        if resp.ok and isinstance(data, dict) and data.get("ok"):
            return data.get("domains") or []
    except Exception:
        pass
    return []

