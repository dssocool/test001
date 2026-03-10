"""
RPC over Azure Storage Queue for DelphixClient when App Service cannot reach Delphix.
Request/response envelopes are JSON. Local bridge runs DelphixClient and posts responses.
"""
import base64
import json
import time
import uuid

from app.services.delphix_client import DelphixClientError

# Lazy import to avoid requiring azure-storage-queue unless queue proxy is used
_queue_client = None


def _get_queue_service(connection_string):
    global _queue_client
    if _queue_client is None:
        from azure.storage.queue import QueueServiceClient
        _queue_client = QueueServiceClient.from_connection_string(connection_string)
    return _queue_client


def _queue_client_from_config(azure_queue):
    """Build QueueServiceClient from azure_queue dict (connection_string or account_name+account_key)."""
    if azure_queue.get("connection_string"):
        return _get_queue_service(azure_queue["connection_string"])
    account = azure_queue.get("account_name")
    key = azure_queue.get("account_key")
    if not account or not key:
        raise ValueError("azure_queue requires connection_string or account_name+account_key")
    conn = (
        f"DefaultEndpointsProtocol=https;AccountName={account};"
        f"AccountKey={key};EndpointSuffix=core.windows.net"
    )
    return _get_queue_service(conn)


def default_visibility_timeout(azure_queue):
    return int(azure_queue.get("visibility_timeout_seconds") or 600)


def default_rpc_timeout_seconds(azure_queue):
    return int(azure_queue.get("rpc_timeout_seconds") or 120)


def send_request(azure_queue, method, args, kwargs):
    """
    Enqueue a single RPC. args/kwargs must be JSON-serializable.
    For create_file_format, kwargs should include file_b64 and file_name instead of header_file_path.
    Returns correlation id.
    """
    corr_id = str(uuid.uuid4())
    envelope = {
        "id": corr_id,
        "method": method,
        "args": list(args),
        "kwargs": kwargs,
    }
    svc = _queue_client_from_config(azure_queue)
    q = svc.get_queue_client(queue=azure_queue["request_queue"])
    q.send_message(json.dumps(envelope))
    return corr_id


def wait_for_response(azure_queue, corr_id, timeout_seconds=None):
    """
    Poll response queue until a message with matching id arrives or timeout.
    Returns parsed response dict. Raises DelphixClientError on error envelope or timeout.
    """
    if timeout_seconds is None:
        timeout_seconds = default_rpc_timeout_seconds(azure_queue)
    svc = _queue_client_from_config(azure_queue)
    resp_q = svc.get_queue_client(queue=azure_queue["response_queue"])
    deadline = time.monotonic() + timeout_seconds
    visibility = default_visibility_timeout(azure_queue)

    while time.monotonic() < deadline:
        messages = resp_q.receive_messages(messages_per_page=32, visibility_timeout=visibility)
        for msg in messages:
            try:
                body = json.loads(msg.content)
            except (json.JSONDecodeError, TypeError):
                resp_q.delete_message(msg)
                continue
            if body.get("id") != corr_id:
                # Not ours; put back by not deleting — but then message is invisible until visibility timeout.
                # Better: peek-only not available for filter; use single response queue per worker and
                # short visibility, or delete others... Actually other ids are from other requests;
                # we must not delete them. So we need to receive with visibility and if wrong id, skip delete
                # and let it reappear — but then we block. Alternative: one response queue per correlation id
                # is not practical. Standard pattern: receive messages, if id mismatch re-queue or extend visibility.
                # Simplest: receive, if wrong id delete and... no, that loses the message.
                # Correct pattern: peek_messages doesn't exist in same way. Use receive; if id mismatch,
                # update_message with same content to extend visibility... complex.
                # Simpler: single worker, sequential responses — still can reorder if worker slow.
                # Practical approach: receive_messages, collect all, find matching id, delete only that message;
                # for others, we can't delete. So use update_message to set visibility_timeout=0 to make visible again?
                # Azure Queue update_message can set visibility to 0 to make immediately visible.
                try:
                    resp_q.update_message(msg, visibility_timeout=0)
                except Exception:
                    pass
                continue
            resp_q.delete_message(msg)
            if body.get("ok"):
                return body.get("result")
            err = body.get("error") or "Delphix queue RPC failed"
            de = body.get("delphix_error") or {}
            raise DelphixClientError(
                err,
                status_code=de.get("status_code"),
                response_body=de.get("response_body"),
            )
        time.sleep(0.5)

    raise DelphixClientError(f"Delphix queue RPC timeout after {timeout_seconds}s")


class DelphixClientViaQueue:
    """
    Same public methods as DelphixClient; each call is serialized to the request queue
    and blocks until the response queue delivers the result.
    """

    def __init__(self, config):
        self._config = config
        self._aq = config["azure_queue"]
        self._timeout = default_rpc_timeout_seconds(self._aq)

    def create_file_format(self, header_file_path, file_format_type="DELIMITED"):
        with open(header_file_path, "rb") as f:
            raw = f.read()
        file_b64 = base64.standard_b64encode(raw).decode("ascii")
        file_name = os.path.basename(header_file_path)
        corr_id = send_request(
            self._aq,
            "create_file_format",
            [],
            {
                "file_b64": file_b64,
                "file_name": file_name,
                "file_format_type": file_format_type,
            },
        )
        return wait_for_response(self._aq, corr_id, self._timeout)

    def update_file_format(self, file_format_id, header):
        corr_id = send_request(
            self._aq, "update_file_format", [], {"file_format_id": file_format_id, "header": header}
        )
        return wait_for_response(self._aq, corr_id, self._timeout)

    def create_file_ruleset(self, ruleset_name, file_connector_id):
        corr_id = send_request(
            self._aq,
            "create_file_ruleset",
            [],
            {"ruleset_name": ruleset_name, "file_connector_id": file_connector_id},
        )
        return wait_for_response(self._aq, corr_id, self._timeout)

    def create_file_metadata(
        self, file_name, ruleset_id, file_format_id, delimiter=",", end_of_record="\r\n"
    ):
        corr_id = send_request(
            self._aq,
            "create_file_metadata",
            [],
            {
                "file_name": file_name,
                "ruleset_id": ruleset_id,
                "file_format_id": file_format_id,
                "delimiter": delimiter,
                "end_of_record": end_of_record,
            },
        )
        return wait_for_response(self._aq, corr_id, self._timeout)

    def create_profile_job(self, job_name, profile_set_id, ruleset_id, job_description=""):
        corr_id = send_request(
            self._aq,
            "create_profile_job",
            [],
            {
                "job_name": job_name,
                "profile_set_id": profile_set_id,
                "ruleset_id": ruleset_id,
                "job_description": job_description,
            },
        )
        return wait_for_response(self._aq, corr_id, self._timeout)

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
        corr_id = send_request(
            self._aq,
            "create_masking_job",
            [],
            {
                "job_name": job_name,
                "ruleset_id": ruleset_id,
                "ruleset_type": ruleset_type,
                "feedback_size": feedback_size,
                "max_memory": max_memory,
                "min_memory": min_memory,
                "multi_tenant": multi_tenant,
                "num_input_streams": num_input_streams,
                "on_the_fly_masking": on_the_fly_masking,
                "fail_immediately": fail_immediately,
                "stream_row_limit": stream_row_limit,
            },
        )
        return wait_for_response(self._aq, corr_id, self._timeout)

    def run_job(self, job_id):
        corr_id = send_request(self._aq, "run_job", [], {"job_id": job_id})
        return wait_for_response(self._aq, corr_id, self._timeout)

    def get_execution(self, execution_id):
        corr_id = send_request(self._aq, "get_execution", [], {"execution_id": execution_id})
        return wait_for_response(self._aq, corr_id, self._timeout)

    def get_file_field_metadata(self, file_format_id, page_number=1):
        corr_id = send_request(
            self._aq,
            "get_file_field_metadata",
            [],
            {"file_format_id": file_format_id, "page_number": page_number},
        )
        return wait_for_response(self._aq, corr_id, self._timeout)
