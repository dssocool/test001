# Synthetic Data Generator

Python Flask web app for managing synthetic data flows by domain. Supports:

- **Local Windows**: Pack as a single exe; starts the app and opens the browser at `http://127.0.0.1:5000`.
- **Azure App Service**: Deploy as a web app; users sign in with Azure AD (MSAL). No browser auto-open.

## Features

- **Main page**: Domains with nested data flows; create domain and create data flow (per domain).
- **Create domain**: Name and save.
- **Create data flow** (3 steps):
  1. **Data source**: Choose SQL Server (on-prem, Active Directory Integrated), Azure Blob, or local file upload. Validate connection, then export or select data (top 10 rows per table/file).
  2. **Dry run**: View first 10 rows from each file in an expandable list.
  3. **Confirm and save**: Review config and save the flow.

## Requirements

- Python 3.9+
- For **SQL Server** source: [ODBC Driver 17 or 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) and `pyodbc` (Windows: install driver on target machine).
- For **Azure Blob**: `azure-storage-blob`.
- For **Azure auth**: `msal`.

## Local run (development)

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
# or: source .venv/bin/activate   # Linux/macOS
pip install -r requirements.txt
python run_local.py
```

Browser opens at `http://127.0.0.1:5000`. No login.

## Build Windows single exe

1. Install PyInstaller: `pip install pyinstaller`
2. From project root (with venv active):

   ```bash
   pyinstaller synthetic_data_generator.spec
   ```

3. Exe is in `dist/SyntheticDataGenerator.exe`. Run it; it will start the server and open the default browser. Data (SQLite, temp files) is stored in an `instance` folder next to the exe.

To hide the console window, set `console=False` in the `.spec` file under `EXE(...)`.

## Azure App Service deployment

1. Create a Python 3.10 or 3.11 App Service (e.g. Linux).
2. **App settings** (Configuration → Application settings):
   - `RUNNING_ON_AZURE` = `1` (or rely on default `WEBSITE_SITE_NAME`)
   - `SECRET_KEY` = random secret for Flask session
   - **Option A – Easy Auth only (recommended if you use App Service Authentication):**  
     Enable **Authentication** in the portal (Microsoft identity platform). No MSAL env vars needed.  
     The app reads `X-MS-CLIENT-PRINCIPAL` and `/logout` redirects to `/.auth/logout`.
   - **Option B – MSAL in the app:**  
     `MSAL_CLIENT_ID`, `MSAL_CLIENT_SECRET`, `MSAL_TENANT_ID` — app handles `/login` and `/redirect` itself.
3. **Azure AD (only if using MSAL in app)**:
   - Register an app; add redirect URI: `https://<your-app>.azurewebsites.net/redirect`
   - Create a client secret; grant admin consent if needed.
4. **Startup command**:  
   `python run_azure.py`  
   or:  
   `gunicorn -w 4 -b 0.0.0.0:$PORT "app:create_app()"`
5. Deploy code (e.g. zip deploy, GitHub Actions, or VS Code). Ensure `requirements.txt` is at repo root.

With **Easy Auth**, Azure redirects unauthenticated users before requests hit the app.  
With **MSAL**, the app redirects to `/login` and Azure AD as before.

## Configuration

| Variable | Description |
|----------|-------------|
| `PORT` | Server port (default 5000 local, 8000 Azure). |
| `INSTANCE_PATH` | Folder for SQLite DB and temp files (default: `instance` next to exe or project). |
| `TEMP_BASE` | Base folder for dry-run temp files (default: `instance/temp`). |
| `SECRET_KEY` | Flask secret key (required for session on Azure). |

### Delphix via Azure Storage Queue (App Service without Delphix network)

When the web app runs in Azure App Service but **cannot reach Delphix** directly, you can tunnel Delphix API calls through Azure Storage Queues. A **local bridge** process (on a machine with Delphix access) consumes requests and runs the real `DelphixClient`.

1. **Create two queues** in a storage account (e.g. `delphix-requests`, `delphix-responses`).
2. **`delphix_config.json`** (in `INSTANCE_PATH`):
   - Always include **`azure`** (blob) so the app can upload/download CSVs.
   - Add **`azure_queue`** with `request_queue`, `response_queue`, and either `connection_string` or `account_name` + `account_key`.
   - On **App Service only**, you may omit `base_url` and `auth_token` if you enable the queue proxy (bridge holds full credentials).
3. **App Service**:
   - Set `USE_DELPHIX_QUEUE_TUNNEL=1` or `DELPHIX_QUEUE_PROXY=1`, or set `USE_DELPHIX_QUEUE_TUNNEL = True` in `run_azure.py`.
   - Use a long enough **request timeout** for the dry-run POST (profile/masking can run many minutes).
4. **Local bridge** (PC with Delphix access):

   ```bash
   unset DELPHIX_QUEUE_PROXY
   export INSTANCE_PATH=/path/to/instance   # contains delphix_config.json with base_url + auth_token + azure_queue
   python scripts/delphix_queue_bridge.py
   ```

   The bridge must use a **full** config (including `base_url` and `auth_token`). See [delphix_config.example.json](delphix_config.example.json) for the `azure_queue` shape.

## Data sources

- **SQL Server**: Server + database; auth = Active Directory Integrated; TrustServerCertificate=yes. Export multiple tables (top 10 rows each) or run a query (top 10 rows).
- **Azure Blob**: Account name, container, key; list blobs; choose file type (CSV), prefix, delimiter; prepare dry run (top 10 rows per selected file).
- **Local file**: Upload file; file type (CSV), delimiter; top 10 rows used for dry run.
