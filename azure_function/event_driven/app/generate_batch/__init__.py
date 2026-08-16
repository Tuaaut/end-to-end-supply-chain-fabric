"""Classic Azure Functions HTTP entry point for the event-driven test path."""

from __future__ import annotations

import hashlib
import io
import json
import os
import csv
from datetime import UTC, datetime
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import azure.functions as func

from event_batch import HEADERS, build_event_rows


def _csv_bytes(entity: str, entity_rows: list[dict[str, str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=HEADERS[entity].split(","))
    writer.writeheader()
    writer.writerows(entity_rows)
    return buffer.getvalue().encode("utf-8")


def _managed_identity_token(resource: str) -> str:
    endpoint = os.environ["IDENTITY_ENDPOINT"]
    header = os.environ["IDENTITY_HEADER"]
    query = urlencode(
        {"api-version": "2019-08-01", "resource": resource}
    )
    request = Request(
        f"{endpoint}?{query}",
        headers={"X-IDENTITY-HEADER": header, "Metadata": "true"},
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read())["access_token"]


def _dfs_request(token: str, method: str, workspace_id: str, path: str, *, query: dict, data: bytes = b"") -> None:
    encoded_path = "/".join(quote(part, safe="") for part in path.split("/"))
    url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{encoded_path}?{urlencode(query)}"
    request = Request(
        url,
        data=data if method in {"PUT", "PATCH"} else None,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "x-ms-version": "2021-06-08",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(data)),
        },
    )
    with urlopen(request, timeout=60):
        pass


def _ensure_parent_directories(token: str, workspace_id: str, file_path: str) -> None:
    parts = file_path.rsplit("/", 1)[0].split("/")
    # OneLake owns the <lakehouse-id>/Files root. Create only subdirectories.
    for end in range(3, len(parts) + 1):
        try:
            _dfs_request(
                token,
                "PUT",
                workspace_id,
                "/".join(parts[:end]),
                query={"resource": "directory"},
            )
        except HTTPError as exc:
            if exc.code != 409:
                raise


def _upload_file(token: str, workspace_id: str, path: str, data: bytes) -> None:
    _ensure_parent_directories(token, workspace_id, path)
    _dfs_request(token, "PUT", workspace_id, path, query={"resource": "file"})
    _dfs_request(
        token,
        "PATCH",
        workspace_id,
        path,
        query={"action": "append", "position": "0"},
        data=data,
    )
    _dfs_request(
        token,
        "PATCH",
        workspace_id,
        path,
        query={"action": "flush", "position": str(len(data))},
    )


def _deliver(batch: dict, batch_date: str, batch_id: str, manifest: dict) -> list[str]:
    workspace_id = os.environ["ONELAKE_WORKSPACE_ID"]
    lakehouse_id = os.environ["ONELAKE_LAKEHOUSE_ID"]
    token = _managed_identity_token("https://storage.azure.com/")
    written: list[str] = []
    for entity, entity_rows in batch.items():
        path = f"{lakehouse_id}/Files/bronze/{entity}/{batch_date}/{entity}.csv"
        _upload_file(token, workspace_id, path, _csv_bytes(entity, entity_rows))
        written.append(path)

    ready_path = f"{lakehouse_id}/Files/event_batches/{batch_id}/_READY.json"
    manifest["delivery_status"] = "DELIVERED"
    manifest["written_file_count"] = len(written) + 1
    manifest["ready_manifest_path"] = ready_path
    manifest["next_action"] = "Set invoke_pipeline=true to submit the event-test pipeline."
    manifest.pop("manifest_sha256", None)
    manifest["manifest_sha256"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode("utf-8")
    ).hexdigest()
    _upload_file(
        token,
        workspace_id,
        ready_path,
        json.dumps(manifest, sort_keys=True).encode("utf-8"),
    )
    written.append(ready_path)
    return written


def _invoke_pipeline() -> str:
    """Submit the disconnected Fabric event-test pipeline using this Function's MI."""
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    pipeline_id = os.environ["FABRIC_EVENT_PIPELINE_ID"]
    token = _managed_identity_token("https://api.fabric.microsoft.com")
    # Submit the pipeline directly. Metadata updates are intentionally kept
    # out of the event path so each batch does not mutate the pipeline item.
    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        f"/dataPipelines/{pipeline_id}/jobs/execute/instances"
    )
    request = Request(
        url,
        data=b"{}",
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Content-Length": "2",
        },
    )
    with urlopen(request, timeout=60) as response:
        location = response.headers.get("Location", "")
    return location.rsplit("/", 1)[-1] if location else ""


def main(request: func.HttpRequest) -> func.HttpResponse:
    """Create a test batch, optionally deliver it, then optionally submit the test pipeline."""
    try:
        payload = request.get_json() if request.get_body() else {}
    except ValueError:
        return func.HttpResponse("Request body must be valid JSON.", status_code=400)

    batch_date = payload.get("batch_date", datetime.now(UTC).strftime("%Y/%m/%d"))
    if not isinstance(batch_date, str) or len(batch_date.split("/")) != 3:
        return func.HttpResponse("batch_date must use YYYY/MM/DD.", status_code=400)

    batch_id = payload.get("batch_id") or f"sc-event-{batch_date.replace('/', '')}"
    batch, source_updated_at = build_event_rows(batch_date, batch_id)
    manifest = {
        "batch_id": batch_id,
        "batch_date": batch_date,
        "source_updated_at": source_updated_at,
        "entity_count": len(batch),
        "row_count": sum(len(entity_rows) for entity_rows in batch.values()),
        "row_counts": {entity: len(entity_rows) for entity, entity_rows in batch.items()},
        "delivery_status": "NOT_REQUESTED",
        "next_action": "Set deliver=true to write OneLake.",
    }
    manifest["manifest_sha256"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode("utf-8")
    ).hexdigest()
    if payload.get("deliver") is True:
        try:
            written = _deliver(batch, batch_date, batch_id, manifest)
        except (KeyError, ValueError, HTTPError, OSError) as exc:
            return func.HttpResponse(
                json.dumps({"error": str(exc), "delivery_status": "FAILED"}),
                mimetype="application/json",
                status_code=500,
            )
        assert written[-1] == manifest["ready_manifest_path"]

    if payload.get("invoke_pipeline") is True:
        if manifest["delivery_status"] != "DELIVERED":
            return func.HttpResponse(
                json.dumps(
                    {
                        "error": "invoke_pipeline=true requires deliver=true in the same request.",
                        "delivery_status": manifest["delivery_status"],
                    }
                ),
                mimetype="application/json",
                status_code=400,
            )
        try:
            manifest["pipeline_job_instance_id"] = _invoke_pipeline()
            manifest["pipeline_invocation_status"] = "SUBMITTED"
            manifest["next_action"] = "Monitor the submitted Fabric pipeline run."
        except (KeyError, HTTPError, OSError) as exc:
            return func.HttpResponse(
                json.dumps(
                    {
                        "error": str(exc),
                        "delivery_status": manifest["delivery_status"],
                        "pipeline_invocation_status": "FAILED",
                    }
                ),
                mimetype="application/json",
                status_code=502,
            )

    return func.HttpResponse(
        json.dumps(manifest), mimetype="application/json", status_code=200
    )
