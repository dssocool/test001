"""
Azure Blob Storage: validate, list blobs, download and trim to 10 rows per file.
"""
import csv
import os
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
    if not blob_names:
        return False, "No blobs selected"
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
                if i >= 11:
                    break
                rows.append(row)
            safe_name = os.path.basename(blob_name) or blob_name.replace("/", "_")
            if not safe_name.lower().endswith(".csv"):
                safe_name += ".csv"
            fpath = os.path.join(temp_dir, safe_name)
            with open(fpath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=delimiter)
                for row in rows:
                    writer.writerow(row)
            files.append({"name": os.path.basename(fpath), "path": fpath})
        return True, files
    except Exception as e:
        return False, str(e)
