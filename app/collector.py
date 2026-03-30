"""Data collector -- pulls from Stripe, GHL, Instantly, ClickUp into SQLite."""

import os
import httpx
from datetime import datetime, date, timedelta
from app.models import (
    get_db, log_refresh, save_revenue, save_team_activity, upsert_client
)

GHL_BASE = "https://services.leadconnectorhq.com"
INSTANTLY_BASE = "https://api.instantly.ai/api/v2"
CLICKUP_BASE = "https://api.clickup.com/api/v2"
STRIPE_BASE = "https://api.stripe.com/v1"

LOCATIONS = {
    "CSP_UK": {
        "token": os.environ.get("GHL_TOKEN_CSP_UK", ""),
        "location_id": os.environ.get("GHL_LOCATION_CSP_UK", ""),
    },
    "CSP_LEGACY": {
        "token": os.environ.get("GHL_TOKEN_CSP_LEGACY", ""),
        "location_id": os.environ.get("GHL_LOCATION_CSP_LEGACY", ""),
    }
}


# ──────────────────────────────────────────────
# STRIPE
# ──────────────────────────────────────────────

async def collect_stripe():
    """Pull revenue data from Stripe."""
    api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if not api_key:
        log_refresh("Stripe", "skipped", "No STRIPE_SECRET_KEY configured")
        return

    import time
    now = datetime.utcnow()
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0)
    last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
    # Use 60-day window for "active" to handle upfront payments
    active_window_start = now - timedelta(days=60)

    ts_this = int(this_month_start.timestamp())
    ts_last = int(last_month_start.timestamp())
    ts_active = int(active_window_start.timestamp())
    ts_now = int(time.time())

    auth = (api_key, "")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Paginate charges for different windows
            this_charges = await _stripe_paginate_charges(client, auth, ts_this, ts_now)
            last_charges = await _stripe_paginate_charges(client, auth, ts_last, ts_this)
            # 60-day window for active client detection (catches upfront payments)
            active_charges = await _stripe_paginate_charges(client, auth, ts_active, ts_now)

            # Cash collected is strictly this calendar month
            this_total = sum(c["amount"] for c in this_charges if c.get("paid") and not c.get("refunded")) / 100
            last_total = sum(c["amount"] for c in last_charges if c.get("paid") and not c.get("refunded")) / 100

            # Active clients = anyone who paid in last 60 days (handles upfront payments)
            active_clients = set(c.get("billing_details", {}).get("name", "Unknown")
                                 for c in active_charges if c.get("paid") and not c.get("refunded")
                                 and c.get("billing_details", {}).get("name"))

            this_month_clients = set(c.get("billing_details", {}).get("name", "Unknown")
                                     for c in this_charges if c.get("paid") and not c.get("refunded")
                                     and c.get("billing_details", {}).get("name"))
            last_clients = set(c.get("billing_details", {}).get("name", "Unknown")
                               for c in last_charges if c.get("paid") and not c.get("refunded")
                               and c.get("billing_details", {}).get("name"))

            # New = paid this month but not in prior 60-day window before this month
            prior_window = await _stripe_paginate_charges(client, auth, ts_active, ts_this)
            prior_clients = set(c.get("billing_details", {}).get("name", "Unknown")
                                for c in prior_window if c.get("paid") and not c.get("refunded")
                                and c.get("billing_details", {}).get("name"))
            new_clients = this_month_clients - prior_clients

            # Churned = paid in 60-90 day window but NOT in last 60 days
            old_window_start = now - timedelta(days=90)
            ts_old = int(old_window_start.timestamp())
            old_charges = await _stripe_paginate_charges(client, auth, ts_old, ts_active)
            old_clients = set(c.get("billing_details", {}).get("name", "Unknown")
                              for c in old_charges if c.get("paid") and not c.get("refunded")
                              and c.get("billing_details", {}).get("name"))
            churned = old_clients - active_clients

            this_month_str = now.strftime("%Y-%m")
            last_month_str = last_month_start.strftime("%Y-%m")

            save_revenue(
                this_month_str, this_total, len(active_clients),
                len(new_clients), len(churned),
                sorted(active_clients), sorted(new_clients), sorted(churned)
            )
            save_revenue(
                last_month_str, last_total, len(last_clients),
                0, 0, sorted(last_clients), [], []
            )

            log_refresh("Stripe", "success",
                        f"${this_total:,.0f} this month, {len(active_clients)} active (60d), "
                        f"{len(new_clients)} new, {len(churned)} churned")

    except Exception as e:
        log_refresh("Stripe", "error", str(e)[:200])


async def _stripe_paginate_charges(client, auth, created_gte, created_lt):
    """Paginate through Stripe charges API."""
    all_charges = []
    starting_after = None

    while True:
        params = {"limit": 100, "created[gte]": created_gte, "created[lt]": created_lt}
        if starting_after:
            params["starting_after"] = starting_after

        resp = await client.get(f"{STRIPE_BASE}/charges", params=params, auth=auth)
        resp.raise_for_status()
        data = resp.json()
        charges = data.get("data", [])
        all_charges.extend(charges)

        if not data.get("has_more") or not charges:
            break
        starting_after = charges[-1]["id"]

    return all_charges


# ──────────────────────────────────────────────
# GHL
# ──────────────────────────────────────────────

async def collect_ghl():
    """Pull pipeline data from GHL."""
    today = date.today().isoformat()

    for loc_name, loc_config in LOCATIONS.items():
        token = loc_config["token"]
        location_id = loc_config["location_id"]

        if not token or not location_id:
            log_refresh("GHL", "skipped", f"{loc_name}: missing token or location_id")
            continue

        headers = {
            "Authorization": f"Bearer {token}",
            "Version": "2021-07-28",
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{GHL_BASE}/opportunities/pipelines",
                    params={"locationId": location_id},
                    headers=headers,
                )
                resp.raise_for_status()
                pipelines = resp.json().get("pipelines", [])

                rows = []
                for pipeline in pipelines:
                    pipeline_name = pipeline["name"]
                    opp_resp = await client.get(
                        f"{GHL_BASE}/opportunities/search",
                        params={
                            "locationId": location_id,
                            "pipeline_id": pipeline["id"],
                            "limit": 100,
                        },
                        headers=headers,
                    )

                    opportunities = []
                    if opp_resp.status_code == 200:
                        opportunities = opp_resp.json().get("opportunities", [])

                    stage_counts = {}
                    stage_values = {}
                    for opp in opportunities:
                        stage_id = opp.get("pipelineStageId", "unknown")
                        stage_counts[stage_id] = stage_counts.get(stage_id, 0) + 1
                        stage_values[stage_id] = stage_values.get(stage_id, 0) + (opp.get("monetaryValue", 0) or 0)

                    for i, stage in enumerate(pipeline.get("stages", [])):
                        count = stage_counts.get(stage["id"], 0)
                        value = stage_values.get(stage["id"], 0)
                        rows.append((loc_name, pipeline_name, stage["name"], i, count, value, today))

                conn = get_db()
                try:
                    for row in rows:
                        conn.execute(
                            """INSERT INTO pipeline_snapshot
                               (location, pipeline_name, stage_name, stage_order,
                                opportunity_count, total_value, snapshot_date)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""", row)
                    conn.commit()
                finally:
                    conn.close()

                log_refresh("GHL", "success",
                            f"{loc_name}: {len(pipelines)} pipelines, {len(rows)} stages")

        except Exception as e:
            log_refresh("GHL", "error", f"{loc_name}: {str(e)[:200]}")


# ──────────────────────────────────────────────
# INSTANTLY
# ──────────────────────────────────────────────

async def collect_instantly():
    """Pull campaign analytics from Instantly."""
    api_key = os.environ.get("INSTANTLY_API_KEY", "")
    if not api_key:
        log_refresh("Instantly", "skipped", "No API key configured")
        return

    today = date.today().isoformat()
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{INSTANTLY_BASE}/campaigns",
                headers=headers,
                params={"limit": 50},
            )
            resp.raise_for_status()
            raw = resp.json()
            campaigns = raw.get("items", raw if isinstance(raw, list) else [])

            rows = []
            for campaign in campaigns:
                cid = campaign.get("id", "")
                name = campaign.get("name", "Unknown")
                status = campaign.get("status", "unknown")

                analytics_resp = await client.get(
                    f"{INSTANTLY_BASE}/campaigns/{cid}/analytics",
                    headers=headers,
                )

                sent = opened = replied = bounced = 0
                if analytics_resp.status_code == 200:
                    data = analytics_resp.json()
                    sent = data.get("total_sent", 0) or 0
                    opened = data.get("total_opened", 0) or 0
                    replied = data.get("total_replied", 0) or 0
                    bounced = data.get("total_bounced", 0) or 0

                open_rate = (opened / sent * 100) if sent > 0 else 0
                reply_rate = (replied / sent * 100) if sent > 0 else 0
                bounce_rate = (bounced / sent * 100) if sent > 0 else 0

                rows.append((name, cid, status, sent, opened, replied, bounced,
                             open_rate, reply_rate, bounce_rate, today))

            conn = get_db()
            try:
                for row in rows:
                    conn.execute(
                        """INSERT INTO email_snapshot
                           (campaign_name, campaign_id, status, total_sent, total_opened,
                            total_replied, total_bounced, open_rate, reply_rate, bounce_rate,
                            snapshot_date)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", row)
                conn.commit()
            finally:
                conn.close()

            total_sent = sum(r[3] for r in rows)
            total_replied = sum(r[5] for r in rows)
            log_refresh("Instantly", "success",
                        f"{len(campaigns)} campaigns, {total_sent:,} sent, {total_replied} replies")

    except Exception as e:
        log_refresh("Instantly", "error", str(e)[:200])


# ──────────────────────────────────────────────
# CLICKUP
# ──────────────────────────────────────────────

async def collect_clickup():
    """Pull client status and team activity from ClickUp."""
    api_key = os.environ.get("CLICKUP_API_KEY", "")
    if not api_key:
        log_refresh("ClickUp", "skipped", "No CLICKUP_API_KEY configured")
        return

    workspace_id = os.environ.get("CLICKUP_WORKSPACE_ID", "20595659")
    board_id = os.environ.get("CLICKUP_CLIENT_BOARD_ID", "901800417604")
    headers = {"Authorization": api_key}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Pull client board tasks
            resp = await client.get(
                f"{CLICKUP_BASE}/list/{board_id}/task",
                headers=headers,
                params={"include_closed": "true", "subtasks": "true"},
            )

            client_count = 0
            if resp.status_code == 200:
                tasks = resp.json().get("tasks", [])
                for task in tasks:
                    name = task.get("name", "")
                    status = task.get("status", {}).get("status", "").lower()

                    # Map ClickUp status to RAG
                    rag = "amber"
                    if any(g in status for g in ["green", "active", "healthy", "good"]):
                        rag = "green"
                    elif any(r in status for r in ["red", "at risk", "critical", "churn"]):
                        rag = "red"
                    elif any(a in status for a in ["amber", "warning", "watch", "review"]):
                        rag = "amber"

                    if name:
                        upsert_client(name, rag_status=rag, source="clickup")
                        client_count += 1

            # Pull team activity - completed tasks this week
            seven_days_ago = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
            team_resp = await client.get(
                f"{CLICKUP_BASE}/team/{workspace_id}/task",
                headers=headers,
                params={
                    "date_updated_gt": str(seven_days_ago),
                    "statuses[]": ["complete", "closed"],
                    "include_closed": "true",
                },
            )

            members_data = {}
            if team_resp.status_code == 200:
                tasks = team_resp.json().get("tasks", [])
                for task in tasks:
                    for assignee in task.get("assignees", []):
                        name = assignee.get("username", assignee.get("email", "Unknown"))
                        if name not in members_data:
                            members_data[name] = {
                                "member_name": name,
                                "tasks_completed_week": 0,
                                "tasks_overdue": 0,
                                "tasks_in_progress": 0,
                                "last_activity": "",
                            }
                        members_data[name]["tasks_completed_week"] += 1

            # Pull overdue tasks
            overdue_resp = await client.get(
                f"{CLICKUP_BASE}/team/{workspace_id}/task",
                headers=headers,
                params={
                    "due_date_lt": str(int(datetime.utcnow().timestamp() * 1000)),
                    "statuses[]": ["open", "in progress", "to do"],
                },
            )

            if overdue_resp.status_code == 200:
                tasks = overdue_resp.json().get("tasks", [])
                for task in tasks:
                    for assignee in task.get("assignees", []):
                        name = assignee.get("username", assignee.get("email", "Unknown"))
                        if name not in members_data:
                            members_data[name] = {
                                "member_name": name,
                                "tasks_completed_week": 0,
                                "tasks_overdue": 0,
                                "tasks_in_progress": 0,
                                "last_activity": "",
                            }
                        members_data[name]["tasks_overdue"] += 1

            if members_data:
                save_team_activity(list(members_data.values()))

            log_refresh("ClickUp", "success",
                        f"{client_count} clients, {len(members_data)} team members tracked")

    except Exception as e:
        log_refresh("ClickUp", "error", str(e)[:200])


# ──────────────────────────────────────────────
# ORCHESTRATOR
# ──────────────────────────────────────────────

async def collect_all():
    """Run all collectors in order."""
    collectors = [
        ("Stripe", collect_stripe),
        ("GHL", collect_ghl),
        ("Instantly", collect_instantly),
        ("ClickUp", collect_clickup),
    ]

    for name, fn in collectors:
        try:
            await fn()
        except Exception as e:
            print(f"{name} collection error: {e}")

    # Generate CEO brief after all data is collected
    try:
        from app.brief import generate_brief
        await generate_brief()
    except Exception as e:
        print(f"Brief generation error: {e}")
