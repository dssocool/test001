"""
Entry for Azure App Service: bind to 0.0.0.0:PORT, no browser.
"""
import os

from app import create_app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
