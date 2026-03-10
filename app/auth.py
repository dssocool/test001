"""
Azure auth when running on App Service.

Two modes:
1. **MSAL** – app handles OAuth redirect (/login, /redirect). Requires MSAL_CLIENT_ID etc.
2. **Easy Auth** – platform handles auth; app reads X-MS-CLIENT-PRINCIPAL and optional
   X-MS-CLIENT-PRINCIPAL-NAME / X-MS-CLIENT-PRINCIPAL-ID. No MSAL required.
"""
import base64
import json
import uuid
from flask import redirect, url_for, session, request, current_app


def current_user_oid():
    """Stable user id for scoping data (session['user']['oid']). None when not authenticated."""
    return (session.get("user") or {}).get("oid")


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


def _principal_claims_dict(principal_json):
    """Build typ -> val from Easy Auth principal claims list."""
    claims = principal_json.get("claims") if isinstance(principal_json, dict) else None
    if not isinstance(claims, list):
        return {}
    out = {}
    for c in claims:
        if isinstance(c, dict) and c.get("typ") is not None:
            out[c["typ"]] = c.get("val")
    return out


def _claim(claims, *keys):
    """First non-empty claim value for any of keys (GitHub/AAD use different typ names)."""
    for k in keys:
        v = claims.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


def _session_user_from_easy_auth():
    """
    Populate session['user'] from App Service Easy Auth headers.
    Works with Microsoft (aad), GitHub, and other providers; claim typ names vary by IdP.
    Returns True if user was set from headers.
    """
    # Optional short-circuit headers (when platform forwards them)
    principal_id = request.headers.get("X-MS-CLIENT-PRINCIPAL-ID")
    principal_name = request.headers.get("X-MS-CLIENT-PRINCIPAL-NAME")

    b64 = request.headers.get("X-MS-CLIENT-PRINCIPAL")
    if b64:
        try:
            raw = base64.b64decode(b64)
            principal = json.loads(raw.decode("utf-8"))
        except (ValueError, json.JSONDecodeError, OSError):
            principal = None
        if isinstance(principal, dict):
            claims = _principal_claims_dict(principal)
            # Stable user id: AAD oid / objectidentifier; GitHub often sub or nameidentifier
            oid = _claim(
                claims,
                "oid",
                "http://schemas.microsoft.com/identity/claims/objectidentifier",
                "sub",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier",
                "user_id",
            ) or principal_id
            # Login / UPN / email
            preferred_username = _claim(
                claims,
                "preferred_username",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn",
                "login",  # GitHub login
                "username",
                "email",
                "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
            ) or principal_name
            name = (
                _claim(claims, "name", "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name")
                or preferred_username
                or principal_name
            )
            session["user"] = {
                "oid": oid,
                "preferred_username": preferred_username,
                "name": name,
            }
            return True

    # Headers without full principal (some configs only forward ID/NAME)
    if principal_id or principal_name:
        session["user"] = {
            "oid": principal_id,
            "preferred_username": principal_name,
            "name": principal_name or principal_id,
        }
        return True

    return False


def easy_auth_login_redirect_url(app):
    """
    Platform login URL: EASY_AUTH_LOGIN_PATH if set, else /.auth/login/{EASY_AUTH_PROVIDER}.
    Provider examples: aad, github, google, twitter.
    """
    path = app.config.get("EASY_AUTH_LOGIN_PATH")
    if path:
        return path if path.startswith("/") else "/" + path
    provider = (app.config.get("EASY_AUTH_PROVIDER") or "aad").strip().lower()
    # Azure Easy Auth segment matches portal IdP id (github, aad, ...)
    return "/.auth/login/" + provider


def init_minimal_auth(app):
    """Register auth_bp with /login and /logout only (for local dev so base template url_for works)."""
    from flask import Blueprint

    auth_bp = Blueprint("auth_bp", __name__)

    @auth_bp.route("/login")
    def login():
        return redirect(url_for("main_bp.index"))

    @auth_bp.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("main_bp.index"))

    app.register_blueprint(auth_bp)
    return auth_bp


def init_easy_auth(app):
    """
    Easy Auth only: sync session from platform headers; logout goes to /.auth/logout.
    Call when IS_AZURE and MSAL is not configured.
    Login redirect uses EASY_AUTH_PROVIDER or EASY_AUTH_LOGIN_PATH.
    """
    from flask import Blueprint

    auth_bp = Blueprint("auth_bp", __name__)

    @auth_bp.route("/login")
    def login():
        return redirect(easy_auth_login_redirect_url(app))

    @auth_bp.route("/logout")
    def logout():
        session.clear()
        # Easy Auth logout; post_logout_redirect_uri must be allowed in App Service auth settings
        base = request.host_url.rstrip("/")
        return redirect(
            "/.auth/logout?post_logout_redirect_uri=" + base + "/"
        )

    @app.before_request
    def easy_auth_sync_session():
        if not app.config.get("IS_AZURE"):
            return
        if request.endpoint and request.endpoint.startswith("auth_bp."):
            return
        # Platform already rejected unauthenticated users if "require authentication" is on.
        # If request reached here with principal headers, mirror into session for app code.
        if not session.get("user"):
            _session_user_from_easy_auth()
        # Do not redirect to /login here — Easy Auth handles gate; missing user means
        # anonymous allowed at platform level or internal probe; leave as-is.

    app.register_blueprint(auth_bp)
    return auth_bp


def init_auth(app):
    """MSAL: register auth routes and before_request when IS_AZURE."""
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
