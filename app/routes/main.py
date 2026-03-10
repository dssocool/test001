from flask import Blueprint, render_template, current_app
from app.auth import current_user_oid
from app.models import get_domains_with_flows

main_bp = Blueprint("main_bp", __name__)


@main_bp.route("/")
def index():
    domains = get_domains_with_flows(current_app, user_oid=current_user_oid())
    if domains is None:
        domains = []
    return render_template("main.html", domains=domains)
