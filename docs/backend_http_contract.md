## Azure Functions backend HTTP contract

This document mirrors the existing Flask UI/API endpoints and defines the equivalent Azure Functions HTTP surface. All endpoints MUST carry a stable `user_id` that the frontend derives from `app/auth.py` (`current_user_oid` and related logic). Unless otherwise noted, responses use JSON and the backend MUST enforce that `user_id` is non-empty and scopes all reads/writes to per-user storage.

### Conventions

- **User identity**
  - Carried in header: `X-User-Id: <string>` (required).
  - Optionally echoed in responses for debugging, but never trusted from query/body.
- **Base URL**
  - The Functions app is exposed under `BACKEND_BASE_URL`, e.g. `https://<funcapp>.azurewebsites.net/api`.
  - All paths below are relative to this base.

---

### Domains

#### GET `/domains`

- **Description**: List domains and their flows for the current user (equivalent to `get_domains_with_flows`).
- **Headers**:
  - `X-User-Id` (required)
- **Response 200 JSON**:
  - `{ "ok": true, "domains": [ { "id": int, "name": str, "description": str, "data_generation_key": str, "created_at": str, "flows": [ { "id": int, "domain_id": int, "name": str, "created_at": str, "config": object } ] } ] }`

#### POST `/domains`

- **Description**: Create a new domain for the current user (equivalent to `create_domain`).
- **Headers**:
  - `X-User-Id` (required)
- **Body JSON**:
  - `{ "name": str, "description": str | null, "data_generation_key": str | null }`
- **Response**:
  - `201`: `{ "ok": true, "id": int }`
  - `400`: `{ "ok": false, "error": str }`

#### GET `/domains/{domainId}`

- **Description**: Get a single domain by id for the current user (equivalent to `get_domain`).
- **Headers**:
  - `X-User-Id` (required)
- **Response**:
  - `200`: `{ "ok": true, "domain": { ...same shape as in /domains list... } }`
  - `404`: `{ "ok": false, "error": "Domain not found" }`

#### PUT `/domains/{domainId}`

- **Description**: Update an existing domain (equivalent to `update_domain`).
- **Headers**:
  - `X-User-Id` (required)
- **Body JSON**:
  - `{ "name": str, "description": str | null, "data_generation_key": str | null }`
- **Response**:
  - `200`: `{ "ok": true }`
  - `404`: `{ "ok": false, "error": "Domain not found" }`

#### DELETE `/domains/{domainId}`

- **Description**: Delete a domain and its flows (equivalent to `delete_domain` + cascade).
- **Headers**:
  - `X-User-Id` (required)
- **Response**:
  - `200`: `{ "ok": true }`
  - `404`: `{ "ok": false, "error": "Domain not found" }`

---

### Flows

#### GET `/domains/{domainId}/flows`

- **Description**: List flows for a domain (equivalent to `get_domain` + domain["flows"]).
- **Headers**:
  - `X-User-Id` (required)
- **Response**:
  - `200`: `{ "ok": true, "domain": { ... }, "flows": [ { "id": int, "domain_id": int, "name": str, "created_at": str, "config": object } ] }`
  - `404`: `{ "ok": false, "error": "Domain not found" }`

#### POST `/domains/{domainId}/flows`

- **Description**: Create a new flow (step 3 save; wraps `persist_flow_config` + `create_flow`).
- **Headers**:
  - `X-User-Id` (required)
- **Body JSON**:
  - `{ "name": str, "config": object }` where `config` matches `persist_flow_config` output.
- **Response**:
  - `201`: `{ "ok": true, "id": int }`
  - `400`/`404` with `{ "ok": false, "error": str }` on validation/domain errors.

#### GET `/flows/{flowId}`

- **Description**: Get a single flow by id.
- **Headers**:
  - `X-User-Id` (required)
- **Response**:
  - `200`: `{ "ok": true, "flow": { "id": int, "domain_id": int, "name": str, "created_at": str, "config": object } }`
  - `404`: `{ "ok": false, "error": "Flow not found" }`

#### PUT `/flows/{flowId}`

- **Description**: Update an existing flow (step 3 save in edit mode).
- **Headers**:
  - `X-User-Id` (required)
- **Body JSON**:
  - `{ "name": str, "config": object }`
- **Response**:
  - `200`: `{ "ok": true }`
  - `404`: `{ "ok": false, "error": "Flow not found" }`

#### DELETE `/flows/{flowId}`

- **Description**: Delete a flow (equivalent to `delete_flow`).
- **Headers**:
  - `X-User-Id` (required)
- **Response**:
  - `200`: `{ "ok": true }`
  - `404`: `{ "ok": false, "error": "Flow not found" }`

---

### SQL source APIs

These mirror `app/routes/api/sql.py` but operate on per-user backend storage, writing preview CSVs into `temp/{user_id}/{draftId}/sql_*.csv` and returning an opaque `draftId` and logical file metadata instead of filesystem paths.

#### POST `/api/sql/validate`

- **Body JSON**: `{ "server": str, "database": str }`
- **Response**:
  - `200`: `{ "ok": true }`
  - `400`: `{ "ok": false, "error": str }`

#### GET `/api/sql/tables`

- **Query**: `server`, `database`
- **Response**:
  - `200`: `{ "ok": true, "tables": [ { "schema": str, "name": str } | str ] }`
  - `400`: `{ "ok": false, "error": str }`

#### POST `/api/sql/export-tables`

- **Body JSON**:
  - `{ "server": str, "database": str, "tables": [str], "draft_id": str | null }`
- **Response**:
  - `200`: `{ "ok": true, "draft_id": str, "files": [ { "name": str, "logical_name": str } ] }`
  - `400`: `{ "ok": false, "error": str }`

#### POST `/api/sql/export-query`

- **Body JSON**:
  - `{ "server": str, "database": str, "query": str, "draft_id": str | null }`
- **Response**:
  - `200`: `{ "ok": true, "draft_id": str, "files": [ { "name": str, "logical_name": str } ] }`
  - `400`: `{ "ok": false, "error": str }`

---

### Azure Blob source APIs

#### POST `/api/blob/validate`

- **Body JSON**: `{ "account_name": str, "container": str, "key": str }`
- **Response**:
  - `200`: `{ "ok": true, "blobs": [str] }`
  - `400`: `{ "ok": false, "error": str }`

#### POST `/api/blob/prepare-dry-run`

- **Body JSON**:
  - `{ "account_name": str, "container": str, "key": str, "prefix": str | null, "file_type": "csv", "delimiter": str, "selected_blobs": [str], "draft_id": str | null }`
- **Response**:
  - `200`: `{ "ok": true, "draft_id": str, "files": [ { "name": str, "logical_name": str } ] }`
  - `400`: `{ "ok": false, "error": str }`

---

### Local file source APIs

#### POST `/api/local/detect`

- **Multipart form-data**: `file` field with uploaded file.
- **Response**:
  - `200`: `{ "ok": true, "file_type": str, "delimiter": str, "has_header": bool, "end_of_record": str }`
  - `400`: `{ "ok": false, "error": str }`

#### POST `/api/local/upload`

- **Multipart form-data**:
  - `file`: uploaded file
  - `file_type`, `delimiter`, `has_header`, `end_of_record` (as in current API)
  - Optional `draft_id` to reuse an existing draft
- **Response**:
  - `200`: `{ "ok": true, "draft_id": str, "files": [ { "name": str, "logical_name": str } ] }`
  - `400`: `{ "ok": false, "error": str }`

---

### Dry-run preview APIs

These correspond to `app/routes/api/dry_run.py` but operate on per-user temp storage or pre-mask snapshots stored in Blob.

#### GET `/api/dry-run/files`

- **Query**:
  - `draft_id` (required)
  - `max_rows` (optional, default 10)
- **Response**:
  - `200`: `{ "ok": true, "files": [ { "name": str, "section": "sql"|"blob"|"local"|"other", "rows": [ [str] ] } ] }`
  - `400`: `{ "ok": false, "error": str }`

#### GET `/api/dry-run/masked-file`

- **Query**:
  - `blob_name` (required)
  - `delimiter` (optional, default ",")
  - `max_rows` (optional, default 10)
- **Response**:
  - `200`: `{ "ok": true, "name": str, "rows": [ [str] ] }`
  - `400` / `500` on errors as in current implementation.

---

### Delphix status and metadata APIs

These mirror `app/routes/api/delphix.py` and run against the backend’s Delphix client.

#### POST `/api/delphix/status`

- **Body JSON**:
  - `{ "profile_execution_id": str | null, "masking_execution_id": str | null }`
- **Response**:
  - `200`: `{ "ok": true, "profile": object | null, "masking": object | null }`
  - `400`: `{ "ok": false, "error": str }`

#### GET `/api/delphix/file-field-metadata`

- **Query**:
  - `file_format_id`: int (required)
- **Response**:
  - `200`: `{ "ok": true, "responseList": [object], "_pageInfo": object }`
  - `400`: `{ "ok": false, "error": str }`

---

### Flow dry-run orchestration

#### POST `/flows/{domainId}/run-dry-run`

- **Description**: Equivalent to `flows_bp.run_dry_run` + `run_delphix_flow`, but stateless and per-user.
- **Headers**:
  - `X-User-Id` (required)
- **Body JSON**:
  - `{ "draft_id": str, "max_rows": int | null, "flow_config": object }`
  - `flow_config` is the in-memory config from the wizard (including `sql`, `blob`, `local`, `source_type`, `delimiter`, `has_header`, `end_of_record` where applicable).
- **Response**:
  - `200`: `{ "ok": true, "delphix": { ...see run_delphix_flow result... }, "draft_id": str }`
  - `400`: `{ "ok": false, "error": str }` when no sources or Delphix fails.

The backend is responsible for:

- Rehydrating SQL/Blob/Local blocks and copying/combining CSVs into the correct temp location for this `draft_id`.
- Running `run_delphix_flow`-equivalent logic with config and `data_generation_key` from the domain (looked up via `domainId` and `user_id`).
- Storing the returned `delphix` block alongside the flow draft so that the frontend can later persist it via the `/domains/{domainId}/flows` POST/PUT.

