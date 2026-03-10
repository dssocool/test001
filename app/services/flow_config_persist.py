"""
Build a minimal, safe-to-store flow config for data_flow.config JSON.
- SQL: connection + export options only (no secrets).
- Blob: account/container/prefix/delimiter/selected_blobs only; key is never persisted.
- Local: source_type + upload basename only.
Preserves delphix / file_rules from existing config when updating so step 2 re-edit keeps working.
"""
import copy
import os


def persist_flow_config(cfg, existing=None):
    """
    Return a dict suitable for JSON storage. cfg is the in-memory session config;
    existing is the current flow.config when editing (to merge delphix, file_rules).
    """
    if not cfg or not isinstance(cfg, dict):
        return {}
    source_type = cfg.get("source_type")
    existing = existing if isinstance(existing, dict) else {}

    if source_type == "sql":
        out = {
            "source_type": "sql",
            "server": (cfg.get("server") or "").strip(),
            "database": (cfg.get("database") or "").strip(),
            "export_mode": cfg.get("export_mode") or "tables",
        }
        if out["export_mode"] == "tables":
            out["tables"] = list(cfg.get("tables") or [])
            out["query"] = ""
        else:
            out["query"] = (cfg.get("query") or "").strip()
            out["tables"] = []
        if cfg.get("delimiter") is not None:
            out["delimiter"] = cfg.get("delimiter")
        if "has_header" in cfg:
            out["has_header"] = cfg.get("has_header")
        if cfg.get("end_of_record") is not None:
            out["end_of_record"] = cfg.get("end_of_record")
        _merge_preserved(out, cfg, existing)
        return out

    if source_type == "blob":
        out = {
            "source_type": "blob",
            "account_name": (cfg.get("account_name") or "").strip(),
            "container": (cfg.get("container") or "").strip(),
            "prefix": (cfg.get("prefix") or "").strip(),
            "file_type": cfg.get("file_type") or "csv",
            "delimiter": cfg.get("delimiter") if cfg.get("delimiter") is not None else ",",
            "selected_blobs": list(cfg.get("selected_blobs") or []),
        }
        # Never persist account key
        _merge_preserved(out, cfg, existing)
        return out

    if source_type == "local":
        name = cfg.get("upload_name") or ""
        if name:
            name = os.path.basename(str(name))
        out = {"source_type": "local", "upload_name": name}
        if cfg.get("skip_source_config"):
            out["skip_source_config"] = True
        _merge_preserved(out, cfg, existing)
        return out

    # Unknown source_type: persist minimal + preserved only
    out = {"source_type": source_type} if source_type else {}
    _merge_preserved(out, cfg, existing)
    return out


def _merge_preserved(out, cfg, existing):
    """Attach delphix and file_rules from cfg (current session) or existing flow so re-edit keeps dry-run state."""
    for key in ("delphix", "file_rules"):
        if isinstance(cfg, dict) and key in cfg and cfg[key] is not None:
            out[key] = copy.deepcopy(cfg[key])
        elif isinstance(existing, dict) and key in existing and existing[key] is not None:
            out[key] = copy.deepcopy(existing[key])
