"""
Entry for local / Windows exe: start Flask then open browser.
Forces local mode (no Azure auth) so you never get 403 when running this.
"""
import os
import threading
import time
import webbrowser

# Force local mode so auth is never used and 403 cannot occur
os.environ.pop("RUNNING_ON_AZURE", None)
os.environ.pop("WEBSITE_SITE_NAME", None)

from app import create_app

app = create_app()


def open_browser():
    time.sleep(1.5)
    webbrowser.open(f"http://{app.config['HOST']}:{app.config['PORT']}/")


if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    app.run(host=app.config["HOST"], port=app.config["PORT"], debug=app.config.get("DEBUG", False), use_reloader=False)
