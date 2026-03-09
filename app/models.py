import json
import os
import sqlite3
from contextlib import contextmanager
from flask import g


def get_db(app):
    if "db" not in g:
        os.makedirs(os.path.dirname(app.config["SQLITE_DB"]), exist_ok=True)
        g.db = sqlite3.connect(app.config["SQLITE_DB"], detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


@contextmanager
def db_connection(app):
    db = get_db(app)
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise


def init_db(app):
    app.teardown_appcontext(close_db)
    with app.app_context():
        db = get_db(app)
        db.executescript("""
            CREATE TABLE IF NOT EXISTS domain (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS data_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain_id INTEGER NOT NULL REFERENCES domain(id) ON DELETE CASCADE,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                config TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_data_flow_domain ON data_flow(domain_id);
        """)
        db.commit()
        # Add description column to existing domain table if missing
        cur = db.execute("PRAGMA table_info(domain)")
        columns = [row[1] for row in cur.fetchall()]
        if "description" not in columns:
            db.execute("ALTER TABLE domain ADD COLUMN description TEXT DEFAULT ''")
            db.commit()


def get_domains_with_flows(app):
    db = get_db(app)
    cur = db.execute(
        "SELECT id, name, description, created_at FROM domain ORDER BY name"
    )
    domains = [dict(row) for row in cur.fetchall()]
    for d in domains:
        cur = db.execute(
            "SELECT id, name, created_at, config FROM data_flow WHERE domain_id = ? ORDER BY created_at DESC",
            (d["id"],)
        )
        d["flows"] = [dict(row) for row in cur.fetchall()]
        for f in d["flows"]:
            if isinstance(f.get("config"), str):
                f["config"] = json.loads(f["config"]) if f["config"] else {}
    return domains


def create_domain(app, name, description=""):
    with db_connection(app) as db:
        cur = db.execute(
            "INSERT INTO domain (name, description) VALUES (?, ?)",
            (name, (description or "").strip()),
        )
        return cur.lastrowid


def create_flow(app, domain_id, name, config):
    with db_connection(app) as db:
        cur = db.execute(
            "INSERT INTO data_flow (domain_id, name, config) VALUES (?, ?, ?)",
            (domain_id, name or None, json.dumps(config))
        )
        return cur.lastrowid


def get_domain(app, domain_id):
    db = get_db(app)
    cur = db.execute(
        "SELECT id, name, description, created_at FROM domain WHERE id = ?",
        (domain_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_flow_count(app, domain_id):
    db = get_db(app)
    cur = db.execute("SELECT COUNT(*) FROM data_flow WHERE domain_id = ?", (domain_id,))
    return cur.fetchone()[0] or 0


def update_domain(app, domain_id, name, description=""):
    with db_connection(app) as db:
        db.execute(
            "UPDATE domain SET name = ?, description = ? WHERE id = ?",
            (name, (description or "").strip(), domain_id),
        )


def delete_domain(app, domain_id):
    with db_connection(app) as db:
        db.execute("DELETE FROM domain WHERE id = ?", (domain_id,))


def get_flow(app, flow_id):
    db = get_db(app)
    cur = db.execute(
        "SELECT id, domain_id, name, created_at, config FROM data_flow WHERE id = ?",
        (flow_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    d = dict(row)
    if isinstance(d.get("config"), str):
        d["config"] = json.loads(d["config"]) if d["config"] else {}
    return d


def update_flow(app, flow_id, name, config):
    with db_connection(app) as db:
        db.execute(
            "UPDATE data_flow SET name = ?, config = ? WHERE id = ?",
            (name or None, json.dumps(config), flow_id),
        )


def delete_flow(app, flow_id):
    with db_connection(app) as db:
        db.execute("DELETE FROM data_flow WHERE id = ?", (flow_id,))
