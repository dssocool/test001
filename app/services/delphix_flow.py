"""
Orchestrate Delphix CC API and Azure Blob for the create-data-flow flow.
Given temp_dir and flow_config, builds header files, creates file formats (one per file),
one ruleset, multiple file metadata, one profile job and one masking job, runs both jobs,
and returns all IDs to store in flow config.
"""
import csv
import os
import uuid

from app.services.azure_blob import upload_file
from app.services.delphix_client import DelphixClient, DelphixClientError, load_delphix_config


def _list_csv_files(temp_dir):
    """Return sorted list of (name, path) for CSV files in temp_dir."""
    if not temp_dir or not os.path.isdir(temp_dir):
        return []
    out = []
    for name in sorted(os.listdir(temp_dir)):
        path = os.path.join(temp_dir, name)
        if not os.path.isfile(path):
            continue
        if not name.lower().endswith(".csv"):
            continue
        out.append((name, path))
    return out


def _write_header_file(data_path, temp_dir, delimiter):
    """
    Read first row (column names) from data_path and write a header file in temp_dir
    with one column name per line.
    Returns path to the header file, or None on error.
    """
    try:
        with open(data_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            first_row = next(reader, None)
        if not first_row:
            return None
        base = os.path.basename(data_path)
        name = base.rsplit(".", 1)[0] if "." in base else base
        header_name = f"{name}_header.csv"
        header_path = os.path.join(temp_dir, header_name)
        with open(header_path, "w", newline="", encoding="utf-8") as f:
            for col in first_row:
                f.write(col + "\n")
        return header_path
    except Exception:
        return None


def run_delphix_flow(temp_dir, flow_config, instance_path):
    """
    Run the full Delphix sequence: build header files, create file formats (one per CSV),
    one ruleset, upload files to Azure, create file metadata for each, create profile and
    masking jobs, run both jobs. All IDs are returned for storing in flow config.

    Returns (True, result_dict) on success, or (False, error_message) on failure.
    result_dict has keys: file_format_ids, file_ruleset_id, profile_job_id, masking_job_id,
    profile_execution_id, masking_execution_id (and optionally file_metadata_ids).
    """
    csv_files = _list_csv_files(temp_dir)
    if not csv_files:
        return False, "No CSV files found in temp directory"

    delimiter = flow_config.get("delimiter") or ","
    end_of_record = "\r\n"

    config = load_delphix_config(instance_path)
    if not config:
        return False, "Delphix not configured. Add instance/delphix_config.json (see delphix_config.example.json)."

    try:
        client = DelphixClient(config)
    except Exception as e:
        return False, f"Delphix config invalid: {e}"

    # 1) Build header file per CSV
    header_paths = []
    for _name, data_path in csv_files:
        hp = _write_header_file(data_path, temp_dir, delimiter)
        if not hp:
            return False, f"Could not create header file for {data_path}"
        header_paths.append(hp)

    # 2) Create file format per file
    file_format_ids = []
    try:
        for hp in header_paths:
            out = client.create_file_format(hp, file_format_type="DELIMITED")
            ff_id = out.get("file_format_id")
            if ff_id is None:
                return False, "Delphix file format response missing id"
            file_format_ids.append(ff_id)
    except DelphixClientError as e:
        return False, f"Delphix file format: {e}"

    # 3) One file ruleset
    ruleset_name = f"ruleset_{uuid.uuid4().hex[:12]}"
    try:
        out = client.create_file_ruleset(ruleset_name, config["file_connector_id"])
        file_ruleset_id = out.get("file_ruleset_id")
        if file_ruleset_id is None:
            return False, "Delphix file ruleset response missing id"
    except DelphixClientError as e:
        return False, f"Delphix file ruleset: {e}"

    # 4) Upload each data file to Azure Blob
    azure = config["azure"]
    blob_names = []
    try:
        for _name, data_path in csv_files:
            blob_name = upload_file(
                azure["account_name"],
                azure["container_name"],
                azure["access_key"],
                data_path,
            )
            blob_names.append(blob_name)
    except Exception as e:
        return False, f"Azure Blob upload: {e}"

    # 5) Create file metadata for each file (same ruleset, different fileFormatId and fileName)
    file_metadata_ids = []
    try:
        for i in range(len(csv_files)):
            out = client.create_file_metadata(
                file_name=blob_names[i],
                ruleset_id=file_ruleset_id,
                file_format_id=file_format_ids[i],
                delimiter=delimiter,
                end_of_record=end_of_record,
            )
            fm_id = out.get("file_metadata_id")
            if fm_id is not None:
                file_metadata_ids.append(fm_id)
    except DelphixClientError as e:
        return False, f"Delphix file metadata: {e}"

    # 6) Create profile job and masking job
    job_name_base = f"flow_{uuid.uuid4().hex[:8]}"
    try:
        profile_out = client.create_profile_job(
            job_name=f"{job_name_base}_profile",
            profile_set_id=config["profile_set_id"],
            ruleset_id=file_ruleset_id,
            job_description="",
        )
        profile_job_id = profile_out.get("profile_job_id")
        if profile_job_id is None:
            return False, "Delphix profile job response missing id"

        masking_out = client.create_masking_job(
            job_name=f"{job_name_base}_masking",
            ruleset_id=file_ruleset_id,
        )
        masking_job_id = masking_out.get("masking_job_id")
        if masking_job_id is None:
            return False, "Delphix masking job response missing id"
    except DelphixClientError as e:
        return False, f"Delphix jobs: {e}"

    # 7) Run profile job then masking job
    profile_execution_id = None
    masking_execution_id = None
    try:
        run_profile = client.run_job(profile_job_id)
        profile_execution_id = run_profile.get("execution_id")
        run_masking = client.run_job(masking_job_id)
        masking_execution_id = run_masking.get("execution_id")
    except DelphixClientError as e:
        return False, f"Delphix run job: {e}"

    result = {
        "file_format_ids": file_format_ids,
        "file_ruleset_id": file_ruleset_id,
        "file_metadata_ids": file_metadata_ids,
        "profile_job_id": profile_job_id,
        "masking_job_id": masking_job_id,
        "profile_execution_id": profile_execution_id,
        "masking_execution_id": masking_execution_id,
    }
    return True, result
