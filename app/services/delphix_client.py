"""
Delphix CC API client. All configuration (base_url, auth_token, etc.) is read from
instance/delphix_config.json. Copy delphix_config.example.json to instance/delphix_config.json
and fill in your values.
"""
import json
import os

import requests
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

# Suppress warning when SSL verification is disabled for Delphix API
disable_warnings(InsecureRequestWarning)


DELPHIX_CONFIG_FILENAME = "delphix_config.json"


def load_delphix_config(instance_path):
    """
    Load Delphix config from instance_path/delphix_config.json.
    Returns dict with base_url, auth_token, file_connector_id, profile_set_id, azure.
    Returns None if file is missing or invalid.
    """
    path = os.path.join(instance_path, DELPHIX_CONFIG_FILENAME)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    required = ("base_url", "auth_token", "file_connector_id", "profile_set_id", "azure")
    azure_required = ("account_name", "container_name", "access_key")
    if not all(data.get(k) for k in required):
        return None
    azure = data.get("azure")
    if not isinstance(azure, dict) or not all(azure.get(k) for k in azure_required):
        return None
    return data


class DelphixClientError(Exception):
    """Raised when a Delphix API call fails."""
    def __init__(self, message, status_code=None, response_body=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class DelphixClient:
    """Client for Delphix CC Masking API."""

    def __init__(self, config):
        self.base_url = config["base_url"].rstrip("/")
        self.auth_token = config["auth_token"]
        self._session = requests.Session()
        self._session.verify = False  # Disable SSL cert verification for Delphix API
        self._session.headers["Accept"] = "application/json"
        self._session.headers["Authorization"] = self.auth_token

    def _post_json(self, path, json_body):
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self._session.post(url, json=json_body, timeout=60)
        try:
            body = resp.json() if resp.text else {}
        except ValueError:
            body = None
        if not resp.ok:
            raise DelphixClientError(
                f"Delphix API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=body or resp.text,
            )
        return body

    def _put_json(self, path, json_body):
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self._session.put(url, json=json_body, timeout=60)
        try:
            body = resp.json() if resp.text else {}
        except ValueError:
            body = None
        if not resp.ok:
            raise DelphixClientError(
                f"Delphix API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=body or resp.text,
            )
        return body

    def _post_multipart(self, path, files, data):
        url = f"{self.base_url}/{path.lstrip('/')}"
        # Don't set Content-Type; requests sets it with boundary for multipart
        headers = {k: v for k, v in self._session.headers.items() if k.lower() != "content-type"}
        resp = requests.post(url, headers=headers, files=files, data=data, timeout=60, verify=False)
        try:
            body = resp.json() if resp.text else {}
        except ValueError:
            body = None
        if not resp.ok:
            raise DelphixClientError(
                f"Delphix API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=body or resp.text,
            )
        return body

    def create_file_format(self, header_file_path, file_format_type="DELIMITED"):
        """
        POST /file-formats. header_file_path is path to a CSV file (header row only).
        Returns dict with fileFormatId (normalized to 'file_format_id' in response).
        """
        with open(header_file_path, "rb") as f:
            files = {"fileFormat": (os.path.basename(header_file_path), f, "text/csv")}
            data = {"fileFormatType": file_format_type}
            out = self._post_multipart("file-formats", files, data)
        # Normalize response key
        ff_id = out.get("fileFormatId") or out.get("id")
        return {**out, "file_format_id": ff_id}

    def update_file_format(self, file_format_id, header):
        """
        PUT /file-formats/{id}. Set header=1 or 0 for CSV file format.
        """
        body = {"fileFormatId": str(file_format_id), "header": 1 if header else 0}
        return self._put_json(f"file-formats/{file_format_id}", body)

    def create_file_ruleset(self, ruleset_name, file_connector_id):
        """
        POST /file-rulesets. Returns dict with fileRulesetId (normalized to 'file_ruleset_id').
        """
        body = {
            "rulesetName": ruleset_name,
            "fileConnectorId": int(file_connector_id),
        }
        out = self._post_json("file-rulesets", body)
        fr_id = out.get("fileRulesetId") or out.get("id")
        return {**out, "file_ruleset_id": fr_id}

    def create_file_metadata(self, file_name, ruleset_id, file_format_id, delimiter=",", end_of_record="\r\n"):
        """
        POST /file-metadata. file_name is the blob name (after upload to Azure).
        Returns dict; may contain fileMetadataId or id.
        """
        body = {
            "fileName": file_name,
            "rulesetId": int(ruleset_id),
            "fileFormatId": int(file_format_id),
            "delimiter": delimiter,
            "endOfRecord": end_of_record,
        }
        out = self._post_json("file-metadata", body)
        fm_id = out.get("fileMetadataId") or out.get("id")
        return {**out, "file_metadata_id": fm_id}

    def create_profile_job(self, job_name, profile_set_id, ruleset_id, job_description=""):
        """
        POST /profile-jobs. Returns dict with profileJobId (normalized to 'profile_job_id').
        """
        body = {
            "jobName": job_name,
            "profileSetId": int(profile_set_id),
            "rulesetId": int(ruleset_id),
            "jobDescription": job_description or "",
        }
        out = self._post_json("profile-jobs", body)
        pj_id = out.get("profileJobId") or out.get("id")
        return {**out, "profile_job_id": pj_id}

    def create_masking_job(
        self,
        job_name,
        ruleset_id,
        ruleset_type="delimitedFile",
        feedback_size=50000,
        max_memory=1024,
        min_memory=1024,
        multi_tenant=False,
        num_input_streams=1,
        on_the_fly_masking=False,
        fail_immediately=False,
        stream_row_limit=10000,
    ):
        """
        POST /masking-jobs. Returns dict with maskingJobId (normalized to 'masking_job_id').
        """
        body = {
            "jobName": job_name,
            "rulesetId": int(ruleset_id),
            "rulesetType": ruleset_type,
            "feedbackSize": feedback_size,
            "maxMemory": max_memory,
            "minMemory": min_memory,
            "multiTenant": multi_tenant,
            "numInputStreams": num_input_streams,
            "onTheFlyMasking": on_the_fly_masking,
            "failImmediately": fail_immediately,
            "streamRowLimit": stream_row_limit,
        }
        out = self._post_json("masking-jobs", body)
        mj_id = out.get("maskingJobId") or out.get("id")
        return {**out, "masking_job_id": mj_id}

    def run_job(self, job_id):
        """
        POST /executions. job_id is profile job id or masking job id.
        Returns dict; may contain execution id or status.
        """
        body = {"jobId": int(job_id)}
        out = self._post_json("executions", body)
        exec_id = out.get("executionId") or out.get("id")
        return {**out, "execution_id": exec_id}

    def get_execution(self, execution_id):
        """
        GET /executions/{executionId}. Returns execution details including status,
        rowsMasked, rowsTotal, startTime, endTime.
        """
        url = f"{self.base_url}/executions/{int(execution_id)}"
        resp = self._session.get(url, timeout=30)
        try:
            body = resp.json() if resp.text else {}
        except ValueError:
            body = None
        if not resp.ok:
            raise DelphixClientError(
                f"Delphix API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=body or resp.text,
            )
        return body
