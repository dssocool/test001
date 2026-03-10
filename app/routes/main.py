from flask import Blueprint, render_template, current_app
from app.auth import current_user_oid
from app.services import backend_client

main_bp = Blueprint("main_bp", __name__)


@main_bp.route("/")
def index():
    user_oid = current_user_oid()
    domains = backend_client.list_domains(user_oid=user_oid)
    return render_template("main.html", domains=domains)
