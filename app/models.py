import sqlite3
import os
import json
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
            ad_spend REAL,
            leads_count INTEGER,
            bookings_count INTEGER,
            source TEXT DEFAULT 'manual',
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS revenue_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            cash_collected REAL,
            active_clients INTEGER,
            new_clients INTEGER,
            churned_clients INTEGER,
            client_names TEXT,
            new_client_names TEXT,
            churned_client_names TEXT,
            snapshot_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS ceo_brief (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brief_text TEXT,
            priorities TEXT,
            red_flags TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS team_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            member_name TEXT,
            tasks_completed_week INTEGER DEFAULT 0,
            tasks_overdue INTEGER DEFAULT 0,
            tasks_in_progress INTEGER DEFAULT 0,
            last_activity TEXT,
            snapshot_date TEXT,
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


# --- Revenue ---

def save_revenue(month, cash_collected, active_clients, new_clients, churned_clients,
                 client_names=None, new_client_names=None, churned_client_names=None):
    with _lock:
        conn = get_db()
        try:
            conn.execute("DELETE FROM revenue_snapshot WHERE month = ?", (month,))
            conn.execute(
                """INSERT INTO revenue_snapshot
                   (month, cash_collected, active_clients, new_clients, churned_clients,
                    client_names, new_client_names, churned_client_names, snapshot_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (month, cash_collected, active_clients, new_clients, churned_clients,
                 json.dumps(client_names or []), json.dumps(new_client_names or []),
                 json.dumps(churned_client_names or []),
                 datetime.utcnow().strftime("%Y-%m-%d"))
            )
            conn.commit()
        finally:
            conn.close()


def get_revenue(months=2):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM revenue_snapshot ORDER BY month DESC LIMIT ?", (months,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# --- CEO Brief ---

def save_brief(brief_text, priorities, red_flags):
    with _lock:
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO ceo_brief (brief_text, priorities, red_flags) VALUES (?, ?, ?)",
                (brief_text, json.dumps(priorities), json.dumps(red_flags))
            )
            conn.commit()
        finally:
            conn.close()


def get_latest_brief():
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM ceo_brief ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if row:
        d = dict(row)
        d["priorities"] = json.loads(d["priorities"]) if d["priorities"] else []
        d["red_flags"] = json.loads(d["red_flags"]) if d["red_flags"] else []
        return d
    return None


# --- Team Activity ---

def save_team_activity(members):
    """members: list of dicts with member_name, tasks_completed_week, tasks_overdue, tasks_in_progress, last_activity"""
    with _lock:
        conn = get_db()
        try:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            conn.execute("DELETE FROM team_activity WHERE snapshot_date = ?", (today,))
            for m in members:
                conn.execute(
                    """INSERT INTO team_activity
                       (member_name, tasks_completed_week, tasks_overdue, tasks_in_progress,
                        last_activity, snapshot_date)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (m["member_name"], m.get("tasks_completed_week", 0),
                     m.get("tasks_overdue", 0), m.get("tasks_in_progress", 0),
                     m.get("last_activity", ""), today)
                )
            conn.commit()
        finally:
            conn.close()


def get_team_activity():
    conn = get_db()
    rows = conn.execute(
        """SELECT * FROM team_activity
           WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM team_activity)
           ORDER BY tasks_completed_week DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# --- Pipeline ---

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


# --- Email ---

def get_latest_email():
    conn = get_db()
    rows = conn.execute("""
        SELECT * FROM email_snapshot
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM email_snapshot)
        ORDER BY total_sent DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# --- Clients ---

def get_clients():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM client_status ORDER BY rag_status DESC, client_name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_client(name, **kwargs):
    with _lock:
        conn = get_db()
        try:
            existing = conn.execute(
                "SELECT id FROM client_status WHERE client_name = ?", (name,)
            ).fetchone()
            if existing:
                sets = ", ".join(f"{k} = ?" for k in kwargs)
                vals = list(kwargs.values()) + [datetime.utcnow().isoformat(), name]
                conn.execute(
                    f"UPDATE client_status SET {sets}, last_updated = ? WHERE client_name = ?",
                    vals
                )
            else:
                kwargs["client_name"] = name
                kwargs["last_updated"] = datetime.utcnow().isoformat()
                cols = ", ".join(kwargs.keys())
                placeholders = ", ".join("?" for _ in kwargs)
                conn.execute(
                    f"INSERT INTO client_status ({cols}) VALUES ({placeholders})",
                    list(kwargs.values())
                )
            conn.commit()
        finally:
            conn.close()


# --- Data Sources ---

def get_last_refresh():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM data_refresh_log ORDER BY created_at DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_source_status():
    """Get latest status per source for the data sources strip."""
    conn = get_db()
    rows = conn.execute("""
        SELECT source, status, message, created_at
        FROM data_refresh_log
        WHERE id IN (
            SELECT MAX(id) FROM data_refresh_log GROUP BY source
        )
        ORDER BY source
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]
