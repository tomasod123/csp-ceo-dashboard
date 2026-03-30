"""AI CEO Brief generator. Uses Anthropic Claude for natural language summary, with template fallback."""

import os
import json
from datetime import datetime
from app.models import (
    get_revenue, get_latest_email, get_clients, get_team_activity,
    get_source_status, save_brief, get_latest_pipeline
)


async def generate_brief():
    """Generate the CEO brief from all collected data."""
    # Gather all data
    revenue = get_revenue(2)
    emails = get_latest_email()
    clients = get_clients()
    team = get_team_activity()
    sources = get_source_status()
    pipeline_uk = get_latest_pipeline("CSP_UK")
    pipeline_legacy = get_latest_pipeline("CSP_LEGACY")

    # Build data context
    ctx = _build_context(revenue, emails, clients, team, sources, pipeline_uk, pipeline_legacy)

    # Try Anthropic API first, fall back to template
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        try:
            brief_text, priorities, red_flags = await _generate_with_claude(api_key, ctx)
            save_brief(brief_text, priorities, red_flags)
            return
        except Exception as e:
            print(f"Claude brief error: {e}, falling back to template")

    # Template fallback
    brief_text, priorities, red_flags = _generate_template(ctx)
    save_brief(brief_text, priorities, red_flags)


def _build_context(revenue, emails, clients, team, sources, pipeline_uk, pipeline_legacy):
    """Build a structured context dict from all data."""
    ctx = {"generated_at": datetime.utcnow().isoformat()}

    # Revenue
    if revenue:
        this = revenue[0] if revenue else {}
        last = revenue[1] if len(revenue) > 1 else {}
        this_cash = this.get("cash_collected", 0)
        last_cash = last.get("cash_collected", 0)
        mom = ((this_cash - last_cash) / last_cash * 100) if last_cash > 0 else 0

        ctx["revenue"] = {
            "this_month": this_cash,
            "last_month": last_cash,
            "mom_pct": round(mom, 1),
            "active_clients": this.get("active_clients", 0),
            "new_clients": this.get("new_clients", 0),
            "churned_clients": this.get("churned_clients", 0),
            "new_names": json.loads(this.get("new_client_names", "[]")) if this.get("new_client_names") else [],
            "churned_names": json.loads(this.get("churned_client_names", "[]")) if this.get("churned_client_names") else [],
        }

    # Email
    if emails:
        total_sent = sum(e.get("total_sent", 0) for e in emails)
        total_replied = sum(e.get("total_replied", 0) for e in emails)
        total_opened = sum(e.get("total_opened", 0) for e in emails)
        ctx["email"] = {
            "campaigns": len(emails),
            "total_sent": total_sent,
            "total_replied": total_replied,
            "total_opened": total_opened,
            "open_rate": (total_opened / total_sent * 100) if total_sent > 0 else 0,
            "reply_rate": (total_replied / total_sent * 100) if total_sent > 0 else 0,
        }

    # Clients
    if clients:
        rag_counts = {"green": 0, "amber": 0, "red": 0}
        for c in clients:
            rag = c.get("rag_status", "amber")
            rag_counts[rag] = rag_counts.get(rag, 0) + 1
        red_clients = [c["client_name"] for c in clients if c.get("rag_status") == "red"]
        ctx["clients"] = {
            "total": len(clients),
            "green": rag_counts["green"],
            "amber": rag_counts["amber"],
            "red": rag_counts["red"],
            "red_names": red_clients,
        }

    # Team
    if team:
        ctx["team"] = {
            "members": len(team),
            "total_completed": sum(m.get("tasks_completed_week", 0) for m in team),
            "total_overdue": sum(m.get("tasks_overdue", 0) for m in team),
        }

    # Pipeline
    active_opps = sum(s.get("opportunity_count", 0) for s in pipeline_uk + pipeline_legacy)
    total_value = sum(s.get("total_value", 0) for s in pipeline_uk + pipeline_legacy)
    ctx["pipeline"] = {"active_opps": active_opps, "total_value": total_value}

    # Data sources
    connected = [s["source"] for s in sources if s.get("status") == "success"]
    disconnected = [s["source"] for s in sources if s.get("status") in ("error", "skipped")]
    ctx["sources"] = {"connected": connected, "disconnected": disconnected}

    return ctx


async def _generate_with_claude(api_key, ctx):
    """Generate brief using Anthropic Claude API."""
    import httpx

    prompt = f"""You are the AI advisor for Clinic Success Partners (CSP), a marketing agency for aesthetic clinics.
Generate a CEO brief based on this data. Be direct, specific, and action-oriented. No fluff.

Data:
{json.dumps(ctx, indent=2)}

Cameron England's framework context:
- Stage 3 agency ($20-40K/month), scaling to Stage 4
- Target: $200K/month by September 2026
- B2B benchmarks: Meta CPL $20-80, booking rate 30%+, show rate 60-80%, close rate 20-30%
- B2C benchmarks: CPL <$12, show rate 75%+, close rate 30-40%

Return EXACTLY this JSON format (no markdown, no code blocks):
{{"brief": "2-3 sentence overview of how the business is doing RIGHT NOW", "priorities": ["priority 1", "priority 2", "priority 3"], "red_flags": ["red flag 1 if any"]}}

Be specific with numbers. If something is disconnected, mention it as a blind spot. Frame everything in revenue impact."""

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 500,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"]
        data = json.loads(text)
        return data["brief"], data["priorities"], data.get("red_flags", [])


def _generate_template(ctx):
    """Template-based brief when no API key available."""
    parts = []
    priorities = []
    red_flags = []

    # Revenue
    rev = ctx.get("revenue", {})
    if rev:
        cash = rev.get("this_month", 0)
        mom = rev.get("mom_pct", 0)
        active = rev.get("active_clients", 0)
        churned = rev.get("churned_clients", 0)

        direction = "up" if mom > 0 else "down"
        parts.append(
            f"CSP collected ${cash:,.0f} this month ({direction} {abs(mom):.0f}% MoM) "
            f"from {active} active clients."
        )

        if churned > 0:
            names = ", ".join(rev.get("churned_names", []))
            red_flags.append(f"{churned} client(s) churned: {names}")
            priorities.append(f"Investigate churn: {names}")

        new = rev.get("new_clients", 0)
        if new > 0:
            names = ", ".join(rev.get("new_names", []))
            parts.append(f"{new} new client(s) signed: {names}.")
    else:
        parts.append("Revenue data not yet connected.")
        priorities.append("Connect Stripe to track cash flow")

    # Email
    email = ctx.get("email", {})
    if email:
        sent = email.get("total_sent", 0)
        reply_rate = email.get("reply_rate", 0)
        replied = email.get("total_replied", 0)
        if reply_rate < 2:
            red_flags.append(f"Cold email reply rate at {reply_rate:.1f}% (target: 3-8%)")
            priorities.append("Improve cold email copy/targeting")
        parts.append(f"Cold email: {sent:,} sent, {replied} replies ({reply_rate:.1f}% reply rate).")
    else:
        disconnected = ctx.get("sources", {}).get("disconnected", [])
        if "Instantly" in disconnected:
            priorities.append("Connect Instantly API for cold email tracking")

    # Clients
    cl = ctx.get("clients", {})
    if cl and cl.get("red", 0) > 0:
        names = ", ".join(cl.get("red_names", []))
        red_flags.append(f"{cl['red']} client(s) at risk: {names}")
        priorities.append(f"Urgent: address red-flagged clients ({names})")

    # Team
    team = ctx.get("team", {})
    if team and team.get("total_overdue", 0) > 3:
        red_flags.append(f"{team['total_overdue']} overdue tasks across team")

    # Disconnected sources
    disconnected = ctx.get("sources", {}).get("disconnected", [])
    if disconnected:
        parts.append(f"Blind spots: {', '.join(disconnected)} not connected.")

    # Fill priorities to 3
    defaults = [
        "Scale cold email volume to 1,350/day across 90 inboxes",
        "Add Meta Ads data for full acquisition cost visibility",
        "Connect QuickBooks for cash-in-bank and burn rate tracking",
    ]
    while len(priorities) < 3:
        for d in defaults:
            if d not in priorities:
                priorities.append(d)
                break
        else:
            break

    brief_text = " ".join(parts)
    return brief_text, priorities[:3], red_flags
