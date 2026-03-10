import os
import sys

from flask import Flask


def _resource_path(*parts):
    """When running as PyInstaller onefile, use extracted bundle path."""
    if getattr(sys, "frozen", False):
        return os.path.join(sys._MEIPASS, "app", *parts)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), *parts)


def create_app(config_overrides=None):
    template_dir = _resource_path("templates")
    static_dir = _resource_path("static")
    app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
    app.config.from_object("app.config.Config")
    if config_overrides:
        app.config.update(config_overrides)

    from app.models import init_db
    init_db(app)

    # Only enable Azure auth when running on Azure and MSAL is configured
    if app.config.get("IS_AZURE") and app.config.get("MSAL_CLIENT_ID"):
        from app.auth import init_auth
        init_auth(app)

    from app.routes.main import main_bp
    from app.routes.domains import domains_bp
    from app.routes.flows import flows_bp
    from app.routes.settings import settings_bp
    app.register_blueprint(main_bp)
    app.register_blueprint(domains_bp, url_prefix="/domains")
    app.register_blueprint(flows_bp, url_prefix="/domains/<int:domain_id>/flows")
    app.register_blueprint(settings_bp)

    from app.routes.api.sql import sql_bp
    from app.routes.api.blob import blob_bp
    from app.routes.api.local import local_bp
    from app.routes.api.dry_run import dry_run_bp
    from app.routes.api.delphix import delphix_bp
    app.register_blueprint(sql_bp, url_prefix="/api/sql")
    app.register_blueprint(blob_bp, url_prefix="/api/blob")
    app.register_blueprint(local_bp, url_prefix="/api/local")
    app.register_blueprint(dry_run_bp, url_prefix="/api/dry-run")
    app.register_blueprint(delphix_bp, url_prefix="/api/delphix")

    @app.context_processor
    def inject_local_username():
        """When running locally (not Azure), expose current OS user for display in UI."""
        if app.config.get("IS_AZURE"):
            return {}
        # Windows: USERNAME; Linux/macOS: USER (fallback USERNAME)
        local_username = os.environ.get("USERNAME") or os.environ.get("USER") or ""
        return {"local_username": local_username}

    return app
