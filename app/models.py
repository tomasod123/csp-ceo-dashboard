import sqlite3
import os
import threading
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "/data/ceo-dashboard/data.db")
_lock = threading.Lock()


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db():
    conn = get_db()
    # Create tables one at a time (executescript grabs exclusive lock)
    tables = [
        """CREATE TABLE IF NOT EXISTS pipeline_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            pipeline_name TEXT,
            stage_name TEXT,
            stage_order INTEGER,
            opportunity_count INTEGER,
            total_value REAL,
            snapshot_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS email_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT,
            campaign_id TEXT,
            status TEXT,
            total_sent INTEGER,
            total_opened INTEGER,
            total_replied INTEGER,
            total_bounced INTEGER,
            open_rate REAL,
            reply_rate REAL,
            bounce_rate REAL,
            snapshot_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS client_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name TEXT UNIQUE,
            rag_status TEXT DEFAULT 'amber',
            constraint_pillar TEXT,
            constraint_detail TEXT,
            cpl REAL,
            cpb REAL,
            show_rate REAL,
            close_rate REAL,
            roas REAL,
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS growth_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT UNIQUE,
            revenue REAL,
            active_clients INTEGER,
            new_clients INTEGER,
            churned_clients INTEGER,
            ad_spend REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS data_refresh_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            status TEXT,
            message TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
    ]
    for sql in tables:
        conn.execute(sql)
    conn.commit()
    conn.close()


def log_refresh(source, status, message=""):
    with _lock:
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO data_refresh_log (source, status, message) VALUES (?, ?, ?)",
                (source, status, message)
            )
            conn.commit()
        finally:
            conn.close()


def get_latest_pipeline(location=None):
    conn = get_db()
    query = """
        SELECT * FROM pipeline_snapshot
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM pipeline_snapshot)
    """
    params = []
    if location:
        query += " AND location = ?"
        params.append(location)
    query += " ORDER BY pipeline_name, stage_order"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_email():
    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM email_snapshot
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM email_snapshot)
        ORDER BY total_sent DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_clients():
    conn = get_db()
    rows = conn.execute("SELECT * FROM client_status ORDER BY rag_status DESC, client_name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_growth_data():
    conn = get_db()
    rows = conn.execute("SELECT * FROM growth_tracker ORDER BY month DESC LIMIT 12").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_last_refresh():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM data_refresh_log ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_client(name, **kwargs):
    with _lock:
        conn = get_db()
        try:
            existing = conn.execute("SELECT id FROM client_status WHERE client_name = ?", (name,)).fetchone()
            if existing:
                sets = ", ".join(f"{k} = ?" for k in kwargs)
                vals = list(kwargs.values()) + [datetime.utcnow().isoformat(), name]
                conn.execute(f"UPDATE client_status SET {sets}, last_updated = ? WHERE client_name = ?", vals)
            else:
                kwargs["client_name"] = name
                kwargs["last_updated"] = datetime.utcnow().isoformat()
                cols = ", ".join(kwargs.keys())
                placeholders = ", ".join("?" for _ in kwargs)
                conn.execute(f"INSERT INTO client_status ({cols}) VALUES ({placeholders})", list(kwargs.values()))
            conn.commit()
        finally:
            conn.close()
