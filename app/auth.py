"""
MSAL-based auth when running on Azure. No-op when running locally.
"""
import uuid
from flask import redirect, url_for, session, request, current_app


def _msal_app():
    import msal
    cfg = current_app.config
    return msal.ConfidentialClientApplication(
        cfg["MSAL_CLIENT_ID"],
        authority=f"https://login.microsoftonline.com/{cfg['MSAL_TENANT_ID']}",
        client_credential=cfg["MSAL_CLIENT_SECRET"],
    )


def _redirect_uri():
    return request.host_url.rstrip("/") + current_app.config["MSAL_REDIRECT_PATH"]


def init_auth(app):
    """Register auth routes and before_request when IS_AZURE."""
    from flask import Blueprint
    auth_bp = Blueprint("auth_bp", __name__)

    # Scopes for sign-in and ID token (openid, profile for user identity)
    SCOPES = ["User.Read", "openid", "profile"]

    @auth_bp.route("/login")
    def login():
        if not app.config.get("MSAL_CLIENT_ID"):
            return "MSAL not configured (set MSAL_CLIENT_ID, MSAL_CLIENT_SECRET, MSAL_TENANT_ID).", 500
        state = str(uuid.uuid4())
        session["auth_state"] = state
        session["auth_next"] = request.args.get("next") or url_for("main_bp.index")
        msal_app = _msal_app()
        auth_url = msal_app.get_authorization_request_url(
            SCOPES,
            state=state,
            redirect_uri=_redirect_uri(),
        )
        return redirect(auth_url)

    @auth_bp.route("/redirect")
    def redirect_uri():
        if request.args.get("state") != session.get("auth_state"):
            return redirect(url_for("auth_bp.login"))
        if "error" in request.args:
            return f"Auth error: {request.args.get('error_description', request.args.get('error'))}", 400
        code = request.args.get("code")
        if not code:
            return redirect(url_for("auth_bp.login"))
        msal_app = _msal_app()
        result = msal_app.acquire_token_by_authorization_code(
            code,
            scopes=SCOPES,
            redirect_uri=_redirect_uri(),
        )
        if "error" in result:
            return f"Token error: {result.get('error_description', result.get('error'))}", 400
        # Store user identity from ID token claims
        id_claims = result.get("id_token_claims") or {}
        session["user"] = {
            "oid": id_claims.get("oid"),
            "preferred_username": id_claims.get("preferred_username"),
            "name": id_claims.get("name"),
        }
        session.pop("auth_state", None)
        next_url = session.pop("auth_next", url_for("main_bp.index"))
        return redirect(next_url)

    @auth_bp.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("main_bp.index"))

    @app.before_request
    def require_auth():
        if not app.config.get("IS_AZURE") or not app.config.get("MSAL_CLIENT_ID"):
            return
        if request.endpoint and request.endpoint.startswith("auth_bp."):
            return
        if not session.get("user"):
            return redirect(url_for("auth_bp.login", next=request.url))

    app.register_blueprint(auth_bp)
    return auth_bp
