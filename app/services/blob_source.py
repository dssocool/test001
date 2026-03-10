"""
Azure Blob Storage: validate, list blobs, download and trim to N rows per file.
"""
import csv
import os
import uuid
from io import StringIO


def _blob_client(account_name, key):
    from azure.storage.blob import BlobServiceClient
    conn_str = (
        f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={key};EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(conn_str)


def validate_and_list(account_name, container, key, prefix=""):
    try:
        client = _blob_client(account_name, key)
        container_client = client.get_container_client(container)
        blobs = list(container_client.list_blobs(name_starts_with=prefix or None))
        names = [b.name for b in blobs]
        return True, names
    except Exception as e:
        return False, str(e)


def download_top10_rows(account_name, container, key, blob_names, delimiter, temp_dir):
    return download_top_n_rows(account_name, container, key, blob_names, delimiter, temp_dir, 10)


def download_top_n_rows(account_name, container, key, blob_names, delimiter, temp_dir, n, filename_prefix=""):
    """Download each blob and write up to n+1 rows per file to temp_dir. Optional filename_prefix for multi-source merge."""
    if not blob_names:
        return False, "No blobs selected"
    if n is None or n < 1:
        n = 1
    max_rows_to_read = n + 1  # header + n data rows
    try:
        client = _blob_client(account_name, key)
        container_client = client.get_container_client(container)
        files = []
        for blob_name in blob_names:
            blob_client = container_client.get_blob_client(blob_name)
            data = blob_client.download_blob().readall()
            try:
                text = data.decode("utf-8")
            except Exception:
                text = data.decode("utf-8", errors="replace")
            reader = csv.reader(StringIO(text), delimiter=delimiter)
            rows = []
            for i, row in enumerate(reader):
                if i >= max_rows_to_read:
                    break
                rows.append(row)
            safe_name = os.path.basename(blob_name) or blob_name.replace("/", "_")
            if not safe_name.lower().endswith(".csv"):
                safe_name += ".csv"
            prefix = (filename_prefix or "").strip()
            if prefix and not prefix.endswith("_"):
                prefix = prefix + "_"
            fpath = os.path.join(temp_dir, prefix + safe_name)
            with open(fpath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=delimiter)
                for row in rows:
                    writer.writerow(row)
            files.append({"name": os.path.basename(fpath), "path": fpath})
        return True, files
    except Exception as e:
        return False, str(e)


def fetch_blob_dry_run(account_name, container, key, selected_blobs, delimiter, max_rows, temp_base):
    """
    Create a temp dir, download top max_rows rows per selected blob, write CSVs.
    Returns (True, temp_dir) or (False, error_message).
    """
    if not selected_blobs:
        return False, "No files selected"
    os.makedirs(temp_base, exist_ok=True)
    subdir = os.path.join(temp_base, str(uuid.uuid4()))
    os.makedirs(subdir, exist_ok=True)
    ok, result = download_top_n_rows(
        account_name, container, key, selected_blobs, delimiter or ",", subdir, max_rows, filename_prefix=""
    )
    if not ok:
        return False, result
    return True, subdir


def export_blob_into_dir(account_name, container, key, selected_blobs, delimiter, max_rows, temp_dir):
    """
    Download blobs into existing temp_dir with blob_ filename prefix.
    Returns (True, None) or (False, error_message).
    """
    if not selected_blobs:
        return False, "No files selected"
    ok, result = download_top_n_rows(
        account_name, container, key, selected_blobs, delimiter or ",", temp_dir, max_rows, filename_prefix="blob"
    )
    if not ok:
        return False, result
    return True, None
