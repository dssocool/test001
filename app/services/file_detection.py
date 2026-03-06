"""
Detect file type and CSV attributes from a small chunk of bytes (no full-file read).
Supports: CSV, JSON, XML, Parquet.
For CSV also detects: delimiter, has_header, end_of_record.
"""
import csv
from io import StringIO


CHUNK_SIZE = 8192


def _decode_chunk(chunk):
    try:
        return chunk.decode("utf-8")
    except Exception:
        return chunk.decode("utf-8", errors="replace")


def _detect_end_of_record(raw_bytes):
    """Scan first part of raw bytes for \\r\\n vs \\n. Return '\\r\\n' or '\\n'."""
    sample = raw_bytes[:4096]
    if b"\r\n" in sample:
        return "\r\n"
    return "\n"


def _detect_csv_attributes(sample_text, raw_bytes):
    """
    Use csv.Sniffer on sample_text for delimiter and has_header.
    Use raw_bytes for end_of_record.
    Returns dict with delimiter, has_header, end_of_record.
    """
    result = {"delimiter": ",", "has_header": True, "end_of_record": _detect_end_of_record(raw_bytes)}
    if not sample_text or not sample_text.strip():
        return result
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample_text[:4096])
        result["delimiter"] = dialect.delimiter
        result["has_header"] = sniffer.has_header(sample_text[:8192])
    except (csv.Error, Exception):
        pass
    return result


def detect_file(chunk):
    """
    Detect file type and optionally CSV attributes from first chunk (e.g. 8KB).
    Does not read the entire file.

    Returns dict:
      - file_type: "csv" | "json" | "xml" | "parquet"
      - For CSV only: delimiter, has_header (bool), end_of_record ("\\r\\n" or "\\n")
    """
    if not chunk or len(chunk) < 4:
        return {"file_type": "csv", "delimiter": ",", "has_header": True, "end_of_record": "\n"}

    # Parquet: magic bytes PAR1
    if chunk[:4] == b"PAR1":
        return {"file_type": "parquet"}

    text = _decode_chunk(chunk)
    stripped = text.lstrip()

    if not stripped:
        return {"file_type": "csv", "delimiter": ",", "has_header": True, "end_of_record": _detect_end_of_record(chunk)}

    first = stripped[0]
    # JSON: starts with { or [
    if first == "{" or first == "[":
        return {"file_type": "json"}
    # XML: starts with < or <?xml
    if first == "<" or stripped.startswith("<?xml"):
        return {"file_type": "xml"}

    # Default: CSV
    out = {"file_type": "csv"}
    out.update(_detect_csv_attributes(text, chunk))
    return out
