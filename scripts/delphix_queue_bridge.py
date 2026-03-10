#!/usr/bin/env python3
"""
Local Delphix queue bridge: run on a machine that can reach Delphix API.
Polls Azure Storage Queue for RPC requests from App Service, executes DelphixClient,
and posts responses to the response queue.

Usage:
  unset DELPHIX_QUEUE_PROXY   # bridge must use direct Delphix
  export INSTANCE_PATH=/path/to/instance   # folder containing delphix_config.json
  python scripts/delphix_queue_bridge.py

Or:
  python scripts/delphix_queue_bridge.py /path/to/instance
"""
import base64
import json
import os
import sys
import tempfile
import time

# Project root on path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Bridge must not use queue proxy when instantiating DelphixClient
os.environ.pop("DELPHIX_QUEUE_PROXY", None)

from app.services.delphix_client import (  # noqa: E402
    DelphixClient,
    DelphixClientError,
    azure_queue_config_valid,
    load_delphix_config,
)


def _queue_service(azure_queue):
    from azure.storage.queue import QueueServiceClient

    if azure_queue.get("connection_string"):
        return QueueServiceClient.from_connection_string(azure_queue["connection_string"])
    account = azure_queue["account_name"]
    key = azure_queue["account_key"]
    conn = (
        f"DefaultEndpointsProtocol=https;AccountName={account};"
        f"AccountKey={key};EndpointSuffix=core.windows.net"
    )
    return QueueServiceClient.from_connection_string(conn)


def _send_response(resp_q, envelope_id, ok, result=None, error=None, delphix_error=None):
    body = {"id": envelope_id, "ok": ok}
    if ok:
        body["result"] = result
    else:
        body["error"] = error or "error"
        if delphix_error:
            body["delphix_error"] = delphix_error
    resp_q.send_message(json.dumps(body))


def _dispatch(client, method, kwargs):
    if method == "create_file_format":
        file_b64 = kwargs.pop("file_b64")
        file_name = kwargs.pop("file_name")
        file_format_type = kwargs.pop("file_format_type", "DELIMITED")
        raw = base64.standard_b64decode(file_b64)
        fd, path = tempfile.mkstemp(suffix="_" + file_name, prefix="delphix_hdr_")
        try:
            os.write(fd, raw)
            os.close(fd)
            return client.create_file_format(path, file_format_type=file_format_type)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    if method == "update_file_format":
        return client.update_file_format(kwargs["file_format_id"], kwargs["header"])
    if method == "create_file_ruleset":
        return client.create_file_ruleset(kwargs["ruleset_name"], kwargs["file_connector_id"])
    if method == "create_file_metadata":
        return client.create_file_metadata(
            kwargs["file_name"],
            kwargs["ruleset_id"],
            kwargs["file_format_id"],
            delimiter=kwargs.get("delimiter", ","),
            end_of_record=kwargs.get("end_of_record", "\r\n"),
        )
    if method == "create_profile_job":
        return client.create_profile_job(
            kwargs["job_name"],
            kwargs["profile_set_id"],
            kwargs["ruleset_id"],
            job_description=kwargs.get("job_description", ""),
        )
    if method == "create_masking_job":
        return client.create_masking_job(
            kwargs["job_name"],
            kwargs["ruleset_id"],
            ruleset_type=kwargs.get("ruleset_type", "delimitedFile"),
            feedback_size=kwargs.get("feedback_size", 50000),
            max_memory=kwargs.get("max_memory", 1024),
            min_memory=kwargs.get("min_memory", 1024),
            multi_tenant=kwargs.get("multi_tenant", False),
            num_input_streams=kwargs.get("num_input_streams", 1),
            on_the_fly_masking=kwargs.get("on_the_fly_masking", False),
            fail_immediately=kwargs.get("fail_immediately", False),
            stream_row_limit=kwargs.get("stream_row_limit", 10000),
        )
    if method == "run_job":
        return client.run_job(kwargs["job_id"])
    if method == "get_execution":
        return client.get_execution(kwargs["execution_id"])
    if method == "get_file_field_metadata":
        return client.get_file_field_metadata(
            kwargs["file_format_id"], page_number=kwargs.get("page_number", 1)
        )
    raise ValueError(f"Unknown method: {method}")


def main():
    instance_path = os.environ.get("INSTANCE_PATH")
    if len(sys.argv) > 1:
        instance_path = sys.argv[1]
    if not instance_path:
        instance_path = os.path.join(ROOT, "instance")
    if not os.path.isdir(instance_path):
        print("INSTANCE_PATH must be a directory containing delphix_config.json", file=sys.stderr)
        sys.exit(1)

    config = load_delphix_config(instance_path)
    if not config or not config.get("base_url"):
        print(
            "delphix_config.json must include base_url and auth_token for the bridge.",
            file=sys.stderr,
        )
        sys.exit(1)
    if not azure_queue_config_valid(config):
        print("delphix_config.json must include valid azure_queue block.", file=sys.stderr)
        sys.exit(1)

    azure_queue = config["azure_queue"]
    visibility = int(azure_queue.get("visibility_timeout_seconds") or 600)

    try:
        client = DelphixClient(config)
    except Exception as e:
        print(f"DelphixClient init failed: {e}", file=sys.stderr)
        sys.exit(1)

    svc = _queue_service(azure_queue)
    req_q = svc.get_queue_client(queue=azure_queue["request_queue"])
    resp_q = svc.get_queue_client(queue=azure_queue["response_queue"])

    print("Delphix queue bridge listening on", azure_queue["request_queue"], flush=True)

    while True:
        messages = req_q.receive_messages(messages_per_page=1, visibility_timeout=visibility)
        for msg in messages:
            try:
                envelope = json.loads(msg.content)
            except (json.JSONDecodeError, TypeError):
                req_q.delete_message(msg)
                continue
            eid = envelope.get("id")
            method = envelope.get("method")
            kwargs = envelope.get("kwargs") or {}
            if not eid or not method:
                req_q.delete_message(msg)
                continue
            try:
                result = _dispatch(client, method, dict(kwargs))
                _send_response(resp_q, eid, True, result=result)
            except DelphixClientError as e:
                _send_response(
                    resp_q,
                    eid,
                    False,
                    error=str(e),
                    delphix_error={
                        "status_code": e.status_code,
                        "response_body": e.response_body,
                    },
                )
            except Exception as e:
                _send_response(resp_q, eid, False, error=str(e))
            req_q.delete_message(msg)
        time.sleep(0.3)


if __name__ == "__main__":
    main()
