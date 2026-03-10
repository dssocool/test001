"""
Entry for Azure App Service: bind to 0.0.0.0:PORT, no browser.

Set DELPHIX_QUEUE_PROXY=1 (and delphix_config.json azure_queue) so Delphix API calls
are proxied via Azure Storage Queue to a local bridge when App Service cannot reach Delphix.
"""
import os

# Set True to force queue proxy when deploying (overridable by env USE_DELPHIX_QUEUE_TUNNEL).
# Requires delphix_config.json with azure_queue (request_queue, response_queue, connection).
FORCE_DELPHIX_QUEUE_TUNNEL = False

USE_DELPHIX_QUEUE_TUNNEL = FORCE_DELPHIX_QUEUE_TUNNEL or os.environ.get(
    "USE_DELPHIX_QUEUE_TUNNEL", ""
).lower() in ("1", "true", "yes")
if USE_DELPHIX_QUEUE_TUNNEL:
    os.environ.setdefault("DELPHIX_QUEUE_PROXY", "1")

from app import create_app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
