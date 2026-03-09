from flask import Blueprint, render_template, request, redirect, url_for, current_app
from app.models import create_domain, get_domain, update_domain, delete_domain

domains_bp = Blueprint("domains_bp", __name__)


@domains_bp.route("/new", methods=["GET", "POST"])
def new():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        description = request.form.get("description", "").strip()
        if name:
            create_domain(current_app, name, description=description)
        return redirect(url_for("main_bp.index"), code=303)
    return render_template("domain_create.html")


@domains_bp.route("/<int:domain_id>/edit", methods=["GET", "POST"])
def edit(domain_id):
    domain = get_domain(current_app, domain_id)
    if not domain:
        return "Domain not found", 404
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        description = request.form.get("description", "").strip()
        if name:
            update_domain(current_app, domain_id, name, description=description)
        return redirect(url_for("main_bp.index"), code=303)
    return render_template("domain_edit.html", domain=domain)


@domains_bp.route("/<int:domain_id>/delete", methods=["POST"])
def delete(domain_id):
    domain = get_domain(current_app, domain_id)
    if domain:
        delete_domain(current_app, domain_id)
    return redirect(url_for("main_bp.index"), code=303)
