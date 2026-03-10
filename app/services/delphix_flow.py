"""
Orchestrate Delphix CC API and Azure Blob for the create-data-flow flow.
Given temp_dir and flow_config, builds header files, creates file formats (one per file),
one ruleset, multiple file metadata, one profile job and one masking job, runs both jobs,
and returns all IDs to store in flow config.
"""
import csv
import os
import re
import time
import uuid

from app.services.azure_blob import upload_file
from app.services.data_generation_key import masking_rounds_from_key
from app.services.delphix_client import DelphixClient, DelphixClientError, load_delphix_config

# Temp header files created for Delphix file format have names like {base}_{8 hex}.csv
_HEADER_FILE_PATTERN = re.compile(r"^.*_[0-9a-f]{8}\.csv$", re.IGNORECASE)

# Max seconds to wait for profile or masking job before timing out
_EXECUTION_TIMEOUT_SECONDS = 600


def _is_header_file(name):
    """Return True if name matches the temp header file pattern (exclude from data list)."""
    return bool(_HEADER_FILE_PATTERN.match(name))


def _list_csv_files(temp_dir):
    """Return sorted list of (name, path) for data CSV files in temp_dir. Excludes temp header files."""
    if not temp_dir or not os.path.isdir(temp_dir):
        return []
    out = []
    for name in sorted(os.listdir(temp_dir)):
        path = os.path.join(temp_dir, name)
        if not os.path.isfile(path):
            continue
        if not name.lower().endswith(".csv"):
            continue
        if _is_header_file(name):
            continue
        out.append((name, path))
    return out


def _write_header_file(data_path, temp_dir, delimiter, has_header=True):
    """
    Build a header file in temp_dir with one column name per line.
    If has_header: first row of data_path = column names.
    If not has_header: use col1, col2, ... from first row column count.
    Returns path to the header file, or None on error.
    """
    try:
        with open(data_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            first_row = next(reader, None)
        if not first_row:
            return None
        if has_header:
            col_names = first_row
        else:
            col_names = [f"col{i + 1}" for i in range(len(first_row))]
        base = os.path.basename(data_path)
        name = base.rsplit(".", 1)[0] if "." in base else base
        header_name = f"{name}_{uuid.uuid4().hex[:8]}.csv"
        header_path = os.path.join(temp_dir, header_name)
        with open(header_path, "w", newline="", encoding="utf-8") as f:
            for col in col_names:
                f.write(col + "\n")
        return header_path
    except Exception:
        return None


def run_delphix_flow(temp_dir, flow_config, instance_path, data_generation_key=None):
    """
    Run the full Delphix sequence: build header files, create file formats (one per CSV),
    one ruleset, upload files to Azure, create file metadata for each, create profile and
    masking jobs, run profile once then run masking job N times (N from data_generation_key).

    Returns (True, result_dict) on success, or (False, error_message) on failure.
    result_dict has keys: file_format_ids, file_ruleset_id, profile_job_id, masking_job_id,
    profile_execution_id, masking_execution_id (last run), masking_execution_ids (all runs),
    masking_rounds, and optionally file_metadata_ids.
    """
    csv_files = _list_csv_files(temp_dir)
    if not csv_files:
        return False, "No CSV files found in temp directory"

    delimiter = flow_config.get("delimiter") or ","
    end_of_record = flow_config.get("end_of_record") or "\r\n"
    has_header = flow_config.get("has_header", True)

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
        hp = _write_header_file(data_path, temp_dir, delimiter, has_header=has_header)
        if not hp:
            return False, f"Could not create header file for {data_path}"
        header_paths.append(hp)

    # 2) Create file format per file; then set header=0 if no header row
    file_format_ids = []
    try:
        for hp in header_paths:
            out = client.create_file_format(hp, file_format_type="DELIMITED")
            ff_id = out.get("file_format_id")
            if ff_id is None:
                return False, "Delphix file format response missing id"
            file_format_ids.append(ff_id)
            if not has_header:
                client.update_file_format(ff_id, header=False)
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

    # 7) Run profile job, poll until SUCCEEDED, then run masking job and poll until SUCCEEDED
    profile_execution_id = None
    masking_execution_id = None
    try:
        run_profile = client.run_job(profile_job_id)
        profile_execution_id = run_profile.get("execution_id")
        if not profile_execution_id:
            return False, "Delphix profile run response missing executionId"
    except DelphixClientError as e:
        return False, f"Delphix run profile job: {e}"

    deadline = time.monotonic() + _EXECUTION_TIMEOUT_SECONDS
    while True:
        if time.monotonic() > deadline:
            return False, "Profile job timed out"
        time.sleep(1)
        try:
            exec_resp = client.get_execution(profile_execution_id)
        except DelphixClientError as e:
            return False, f"Delphix get profile execution: {e}"
        status = (exec_resp.get("status") or "").strip().upper()
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ERROR", "CANCELLED", "CANCELED"):
            return False, f"Profile job failed with status: {status}"

    # 8) Run masking job N rounds (same job id; each run_job starts a new execution).
    # N is derived from data_generation_key via deterministic hash mod 4 (see data_generation_key module).
    masking_rounds = masking_rounds_from_key(data_generation_key)
    masking_execution_ids = []
    masking_execution_id = None

    for round_index in range(masking_rounds):
        try:
            run_masking = client.run_job(masking_job_id)
            masking_execution_id = run_masking.get("execution_id")
            if not masking_execution_id:
                return False, "Delphix masking run response missing executionId"
        except DelphixClientError as e:
            return False, f"Delphix run masking job: {e}"

        masking_execution_ids.append(masking_execution_id)
        deadline = time.monotonic() + _EXECUTION_TIMEOUT_SECONDS
        while True:
            if time.monotonic() > deadline:
                return False, "Masking job timed out"
            time.sleep(1)
            try:
                exec_resp = client.get_execution(masking_execution_id)
            except DelphixClientError as e:
                return False, f"Delphix get masking execution: {e}"
            status = (exec_resp.get("status") or "").strip().upper()
            if status in ("SUCCEEDED", "WARNING"):
                break
            if status in ("FAILED", "ERROR", "CANCELLED", "CANCELED"):
                return False, f"Masking job failed with status: {status}"

    result = {
        "file_format_ids": file_format_ids,
        "file_ruleset_id": file_ruleset_id,
        "file_metadata_ids": file_metadata_ids,
        "profile_job_id": profile_job_id,
        "masking_job_id": masking_job_id,
        "profile_execution_id": profile_execution_id,
        "masking_execution_id": masking_execution_id,
        "masking_execution_ids": masking_execution_ids,
        "masking_rounds": masking_rounds,
        "blob_names": blob_names,
    }
    return True, result
