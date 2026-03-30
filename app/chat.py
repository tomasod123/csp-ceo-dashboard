"""Cameron England AI advisor chat. Answers questions using live data + Cameron's frameworks."""

import os
import json
import httpx
from app.models import (
    get_revenue, get_latest_email, get_clients, get_team_activity,
    get_source_status, get_latest_pipeline
)


CAMERON_SYSTEM = """You are Cameron England's AI advisor for Clinic Success Partners (CSP), a marketing agency for aesthetic clinics and med spas.

You have access to live business data (provided below) and Cameron England's frameworks:

## Cameron's Core Frameworks

### 4-Pillar Fulfillment: Leads → Bookings → Shows → Closes
- Agency controls left side (leads, bookings), client controls right side (shows, closes)
- Always diagnose LEFT to RIGHT. Fix the earliest broken pillar first.

### B2B Benchmarks (CSP's own acquisition):
- Meta CPL: $20-80 (red flag >$100)
- Booking rate: 30%+ (red flag <15%)
- Show rate: 60-80% (red flag <50%)
- Close rate: 20-30% (red flag <15%)
- Speed to lead: <5 min (red flag >30 min)
- Email reply rate: 3-8% (red flag <2%)
- FE ROAS: 3-4x (red flag <2x)

### B2C Benchmarks (client fulfillment):
- CPL: <$12 (red flag >$17)
- CPB: <$100 (red flag >$150)
- Show rate with deposit: 75%+ (red flag <60%)
- Show rate no deposit: 50%+ (red flag <40%)
- Close rate: 30-40% (red flag <25%)
- Upfront cash: 40%+ (red flag <30%)
- Avg sale: $1,000+ (red flag <$500)

### Scaling Stage: Stage 3 ($20-40K/month)
- Target: $200K/month by September 2026
- Key hire order: B2B Setter → CSM → VA → Media Buyer
- Marketing spend target: 25% of revenue
- Pod structure: 1 CSM per 15-20 clients

### The Andromeda Principle (Meta Ads):
- Creative volume is the #1 lever. Target: 50 creatives/week
- The creative IS the targeting. Broad audiences, diverse messaging.
- If CPL rising: diagnose creative fatigue FIRST, not budget or targeting.

### Decision Framework:
- Type 1 (irreversible): slow down, consult
- Type 2 (reversible): decide in minutes, bias toward action. 90%+ of decisions are Type 2.

Be direct, specific, and action-oriented. Use numbers from the live data. Frame everything in revenue impact. No fluff. Challenge assumptions when needed. If Tomas is about to make a mistake, say so clearly."""


async def chat_with_cameron(question: str) -> str:
    """Send a question to Cameron AI advisor with live data context."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _template_response(question)

    # Gather live data
    data_context = _gather_live_data()

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 1000,
                    "system": CAMERON_SYSTEM,
                    "messages": [
                        {
                            "role": "user",
                            "content": f"Here is CSP's current live data:\n\n```json\n{json.dumps(data_context, indent=2)}\n```\n\nQuestion: {question}"
                        }
                    ],
                },
            )
            resp.raise_for_status()
            return resp.json()["content"][0]["text"]

    except Exception as e:
        return f"Error connecting to AI: {str(e)[:200]}. Try again or check ANTHROPIC_API_KEY."


def _gather_live_data() -> dict:
    """Pull all live data for chat context."""
    data = {}

    revenue = get_revenue(2)
    if revenue:
        this = revenue[0]
        last = revenue[1] if len(revenue) > 1 else {}
        this_cash = this.get("cash_collected", 0)
        last_cash = last.get("cash_collected", 0)
        data["revenue"] = {
            "cash_this_month": this_cash,
            "cash_last_month": last_cash,
            "mom_change": f"{((this_cash - last_cash) / last_cash * 100) if last_cash else 0:.1f}%",
            "active_clients": this.get("active_clients", 0),
            "new_clients": this.get("new_clients", 0),
            "churned": this.get("churned_clients", 0),
            "active_names": json.loads(this.get("client_names", "[]")) if this.get("client_names") else [],
        }

    emails = get_latest_email()
    if emails:
        total_sent = sum(e.get("total_sent", 0) for e in emails)
        total_replied = sum(e.get("total_replied", 0) for e in emails)
        total_opened = sum(e.get("total_opened", 0) for e in emails)
        data["cold_email"] = {
            "campaigns": len(emails),
            "sent": total_sent,
            "opened": total_opened,
            "replied": total_replied,
            "open_rate": f"{(total_opened/total_sent*100) if total_sent else 0:.1f}%",
            "reply_rate": f"{(total_replied/total_sent*100) if total_sent else 0:.1f}%",
        }

    clients = get_clients()
    if clients:
        data["clients"] = [
            {
                "name": c["client_name"],
                "rag": c.get("rag_status", "amber"),
                "cpl": c.get("cpl"),
                "show_rate": c.get("show_rate"),
                "close_rate": c.get("close_rate"),
                "roas": c.get("roas"),
            }
            for c in clients
        ]

    team = get_team_activity()
    if team:
        data["team"] = [
            {"name": m["member_name"], "completed_this_week": m["tasks_completed_week"],
             "overdue": m["tasks_overdue"]}
            for m in team
        ]

    pipeline = get_latest_pipeline()
    active = [p for p in pipeline if p["opportunity_count"] > 0]
    if active:
        data["pipeline"] = [
            {"pipeline": p["pipeline_name"], "stage": p["stage_name"],
             "opps": p["opportunity_count"], "value": p["total_value"]}
            for p in active
        ]

    return data


def _template_response(question: str) -> str:
    """Fallback when no API key. Returns a helpful message."""
    return (
        "Cameron AI advisor requires an Anthropic API key to answer questions. "
        "Add ANTHROPIC_API_KEY to your environment variables to enable this feature.\n\n"
        f"Your question: {question}\n\n"
        "In the meantime, check Cameron's frameworks in the Obsidian vault at "
        "~/obsidian-vault/people/mentors/cameron-england-*.md"
    )
