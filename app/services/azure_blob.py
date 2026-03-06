"""
Upload a local file to Azure Blob Storage. Used by Delphix flow to upload temp data
files before creating file metadata. Config (account_name, container_name, access_key)
comes from Delphix config azure section.
"""
import os
import uuid


def _blob_client(account_name, access_key):
    from azure.storage.blob import BlobServiceClient
    conn_str = (
        f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={access_key};EndpointSuffix=core.windows.net"
    )
    return BlobServiceClient.from_connection_string(conn_str)


def upload_file(account_name, container_name, access_key, local_path, blob_name=None):
    """
    Upload the file at local_path to the given container. If blob_name is None,
    use a unique name (prefix + original filename or UUID). Returns the blob name used.
    """
    if blob_name is None:
        base = os.path.basename(local_path)
        if not base or base == ".":
            base = "data.csv"
        blob_name = f"flow_{uuid.uuid4().hex}_{base}"
    client = _blob_client(account_name, access_key)
    container_client = client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    return blob_name


def download_blob(account_name, container_name, access_key, blob_name):
    """
    Download the blob by name from the given container. Returns the blob content as bytes.
    """
    client = _blob_client(account_name, access_key)
    container_client = client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    download = blob_client.download_blob()
    return download.readall()
