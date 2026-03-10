import os


class Config:
    # Only treat as Azure when the platform sets WEBSITE_SITE_NAME (Azure App Service).
    # Locally that variable is never set, so auth is never enabled and "access denied" cannot occur.
    IS_AZURE = bool(os.environ.get("WEBSITE_SITE_NAME"))

    if IS_AZURE:
        HOST = "0.0.0.0"
        PORT = int(os.environ.get("PORT", 8000))
        # MSAL - set in App Service configuration
        MSAL_CLIENT_ID = os.environ.get("MSAL_CLIENT_ID", "")
        MSAL_CLIENT_SECRET = os.environ.get("MSAL_CLIENT_SECRET", "")
        MSAL_TENANT_ID = os.environ.get("MSAL_TENANT_ID", "common")
        # Redirect URI for Azure AD (e.g. https://<app>.azurewebsites.net/redirect)
        MSAL_REDIRECT_PATH = "/redirect"
        SECRET_KEY = os.environ.get("SECRET_KEY", os.urandom(24).hex())
    else:
        HOST = "127.0.0.1"
        PORT = int(os.environ.get("PORT", 5000))
        SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")

    # SQLite: writable path for both exe and Azure (use exe dir when frozen so it's writable)
    import sys
    if os.environ.get("INSTANCE_PATH"):
        INSTANCE_PATH = os.environ.get("INSTANCE_PATH")
    elif getattr(sys, "frozen", False):
        _writable = os.path.dirname(os.path.abspath(sys.argv[0]))
        INSTANCE_PATH = os.path.join(_writable, "instance")
    else:
        BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        INSTANCE_PATH = os.path.join(BASE_DIR, "instance")
    SQLITE_DB = os.path.join(INSTANCE_PATH, "app.db")

    # Temp folder for flow dry runs
    TEMP_BASE = os.environ.get("TEMP_BASE", os.path.join(INSTANCE_PATH, "temp"))

    # When true and delphix_config has azure_queue, Delphix API calls go via Azure Queue to a local bridge
    DELPHIX_QUEUE_PROXY = os.environ.get("DELPHIX_QUEUE_PROXY", "").lower() in ("1", "true", "yes")
