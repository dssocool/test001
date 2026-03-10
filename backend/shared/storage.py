import json
import os
from typing import Any, Dict, List, Optional, Tuple

from azure.storage.blob import BlobServiceClient


def _blob_service_client() -> BlobServiceClient:
    account_name = os.environ.get("BACKEND_BLOB_ACCOUNT_NAME")
    account_key = os.environ.get("BACKEND_BLOB_ACCOUNT_KEY")
    if not account_name or not account_key:
        raise RuntimeError("BACKEND_BLOB_ACCOUNT_NAME and BACKEND_BLOB_ACCOUNT_KEY must be set")
    conn_str = (
        f"DefaultEndpointsProtocol=https;AccountName={account_name};"
        f"AccountKey={account_key};EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(conn_str)


def _container_client():
    svc = _blob_service_client()
    container = os.environ.get("BACKEND_BLOB_CONTAINER") or "synthetic-data"
    return svc.get_container_client(container)


def _user_prefix(user_id: str) -> str:
    user_id = (user_id or "").strip()
    if not user_id:
        raise ValueError("user_id is required")
    return f"{user_id.strip().replace('/', '_')}/"


def load_json(user_id: str, path: str) -> Optional[Dict[str, Any]]:
    """
    Load a JSON document from Blob under the given user prefix.
    path is relative to the user root, e.g. 'config/domains.json'.
    """
    blob_name = _user_prefix(user_id) + path.lstrip("/")
    cc = _container_client()
    try:
        blob = cc.get_blob_client(blob_name)
        data = blob.download_blob().readall()
    except Exception:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        return None


def save_json(user_id: str, path: str, payload: Dict[str, Any]) -> None:
    blob_name = _user_prefix(user_id) + path.lstrip("/")
    cc = _container_client()
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    cc.upload_blob(name=blob_name, data=data, overwrite=True)


def list_blobs_with_prefix(user_id: str, prefix: str) -> List[str]:
    cc = _container_client()
    user_root = _user_prefix(user_id)
    full_prefix = user_root + prefix.lstrip("/")
    names: List[str] = []
    for b in cc.list_blobs(name_starts_with=full_prefix):
        names.append(b.name[len(user_root) :])
    return names


def upload_temp_file(user_id: str, draft_id: str, local_path: str, logical_name: str) -> str:
    """
    Upload a temp CSV into the per-user temp area.
    Returns the relative blob name (under the user root) for later reference.
    """
    cc = _container_client()
    rel = f"temp/{draft_id}/{logical_name}"
    blob_name = _user_prefix(user_id) + rel
    with open(local_path, "rb") as f:
        cc.upload_blob(name=blob_name, data=f, overwrite=True)
    return rel


def download_temp_file(user_id: str, rel_path: str) -> bytes:
    cc = _container_client()
    blob_name = _user_prefix(user_id) + rel_path.lstrip("/")
    blob = cc.get_blob_client(blob_name)
    return blob.download_blob().readall()


