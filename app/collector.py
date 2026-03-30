"""Data collector -- pulls from GHL, Instantly, ClickUp into SQLite."""

import os
import httpx
from datetime import datetime, date
from app.models import get_db, log_refresh

GHL_BASE = "https://services.leadconnectorhq.com"
INSTANTLY_BASE = "https://api.instantly.ai/api/v2"

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

                # Collect all data first, then write to DB in one go
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

                # Write all rows in a single transaction
                conn = get_db()
                try:
                    for row in rows:
                        conn.execute(
                            """INSERT INTO pipeline_snapshot
                               (location, pipeline_name, stage_name, stage_order,
                                opportunity_count, total_value, snapshot_date)
                               VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            row
                        )
                    conn.commit()
                finally:
                    conn.close()

                log_refresh("GHL", "success", f"{loc_name}: {len(pipelines)} pipelines, {len(rows)} stages pulled")

        except Exception as e:
            log_refresh("GHL", "error", f"{loc_name}: {str(e)[:200]}")


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
            campaigns = resp.json().get("items", resp.json() if isinstance(resp.json(), list) else [])

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
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        row
                    )
                conn.commit()
            finally:
                conn.close()

            log_refresh("Instantly", "success", f"{len(campaigns)} campaigns pulled")

    except Exception as e:
        log_refresh("Instantly", "error", str(e)[:200])


async def collect_all():
    """Run all collectors."""
    try:
        await collect_ghl()
    except Exception as e:
        print(f"GHL collection error: {e}")
    try:
        await collect_instantly()
    except Exception as e:
        print(f"Instantly collection error: {e}")
