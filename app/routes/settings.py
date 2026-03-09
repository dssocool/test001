from flask import Blueprint, render_template

settings_bp = Blueprint("settings_bp", __name__)


@settings_bp.route("/settings")
def settings():
    return render_template("settings.html")
