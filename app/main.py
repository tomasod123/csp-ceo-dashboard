"""CSP CEO Dashboard -- Private, password-protected business intelligence."""

import os
import json
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Form, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.models import (
    init_db, get_latest_pipeline, get_latest_email, get_clients,
    get_last_refresh, get_revenue, get_latest_brief, get_team_activity,
    get_source_status
)
from app.cameron import classify_rag, identify_constraint, growth_math, COLOR_FUNCTIONS
from app.collector import collect_all

load_dotenv()

DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "csp-ceo-2026")
SESSION_SECRET = os.environ.get("SESSION_SECRET", "change-me-in-production")
COOKIE_NAME = "csp_session"
COOKIE_MAX_AGE = 60 * 60 * 24 * 7  # 7 days

serializer = URLSafeTimedSerializer(SESSION_SECRET)
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    try:
        await collect_all()
    except Exception as e:
        print(f"Initial collection error: {e}")
    scheduler.add_job(collect_all, "interval", minutes=30, id="data_refresh")
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="CSP CEO Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Register Cameron threshold colors as Jinja2 globals
for name, fn in COLOR_FUNCTIONS.items():
    templates.env.globals[name] = fn

# Also register json.loads for template use
templates.env.globals["json_loads"] = json.loads


def verify_session(request: Request) -> bool:
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return False
    try:
        data = serializer.loads(token, max_age=COOKIE_MAX_AGE)
        return data == "authenticated"
    except Exception:
        return False


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})


@app.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    if password == DASHBOARD_PASSWORD:
        response = RedirectResponse(url="/", status_code=303)
        token = serializer.dumps("authenticated")
        response.set_cookie(COOKIE_NAME, token, max_age=COOKIE_MAX_AGE, httponly=True, samesite="lax")
        return response
    return templates.TemplateResponse(request=request, name="login.html", context={"error": "Wrong password"})


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not verify_session(request):
        return RedirectResponse(url="/login")

    # Revenue
    revenue_data = get_revenue(2)
    this_month = revenue_data[0] if revenue_data else {}
    last_month = revenue_data[1] if len(revenue_data) > 1 else {}

    this_cash = this_month.get("cash_collected", 0)
    last_cash = last_month.get("cash_collected", 0)
    mom_pct = ((this_cash - last_cash) / last_cash * 100) if last_cash > 0 else 0

    # Pipeline
    pipeline_uk = get_latest_pipeline("CSP_UK")
    pipeline_legacy = get_latest_pipeline("CSP_LEGACY")

    # Build pipeline summary (only pipelines with data)
    pipeline_summary = {}
    for p in pipeline_uk + pipeline_legacy:
        name = p["pipeline_name"]
        if name not in pipeline_summary:
            pipeline_summary[name] = {"stages": [], "total_opps": 0, "total_value": 0}
        pipeline_summary[name]["stages"].append(p)
        pipeline_summary[name]["total_opps"] += p["opportunity_count"]
        pipeline_summary[name]["total_value"] += p["total_value"]

    # Filter to pipelines with actual data
    active_pipelines = {k: v for k, v in pipeline_summary.items() if v["total_opps"] > 0}

    # Total pipeline stats
    total_pipeline_opps = sum(v["total_opps"] for v in pipeline_summary.values())
    total_pipeline_value = sum(v["total_value"] for v in pipeline_summary.values())

    # Email
    email_data = get_latest_email()
    email_totals = {"total_sent": 0, "total_opened": 0, "total_replied": 0,
                    "total_bounced": 0, "campaigns": 0,
                    "open_rate": 0, "reply_rate": 0, "bounce_rate": 0}
    if email_data:
        email_totals["total_sent"] = sum(e["total_sent"] for e in email_data)
        email_totals["total_opened"] = sum(e["total_opened"] for e in email_data)
        email_totals["total_replied"] = sum(e["total_replied"] for e in email_data)
        email_totals["total_bounced"] = sum(e["total_bounced"] for e in email_data)
        email_totals["campaigns"] = len(email_data)
        if email_totals["total_sent"] > 0:
            email_totals["open_rate"] = email_totals["total_opened"] / email_totals["total_sent"] * 100
            email_totals["reply_rate"] = email_totals["total_replied"] / email_totals["total_sent"] * 100
            email_totals["bounce_rate"] = email_totals["total_bounced"] / email_totals["total_sent"] * 100

    # Clients with Cameron analysis
    clients = get_clients()
    rag_counts = {"green": 0, "amber": 0, "red": 0}
    for c in clients:
        c["rag"] = c.get("rag_status", classify_rag(c))
        pillar, detail = identify_constraint(c)
        c["constraint_pillar"] = pillar
        c["constraint_detail"] = detail
        rag_counts[c["rag"]] = rag_counts.get(c["rag"], 0) + 1

    # Team
    team = get_team_activity()

    # Brief
    brief = get_latest_brief()

    # Data sources
    sources = get_source_status()
    refreshes = get_last_refresh()

    # Growth math
    active_clients = this_month.get("active_clients", len(clients)) or len(clients) or 10
    months_left = max(1, (datetime(2026, 9, 30) - datetime.now()).days // 30)
    growth = growth_math(active_clients, months_remaining=months_left)

    # Acquisition cost
    # If we have Stripe revenue and know ad spend, calculate CPA
    new_clients_count = this_month.get("new_clients", 0)

    return templates.TemplateResponse(request=request, name="dashboard.html", context={
        # Revenue
        "this_cash": this_cash,
        "last_cash": last_cash,
        "mom_pct": mom_pct,
        "this_month": this_month,
        "last_month": last_month,
        # Pipeline
        "active_pipelines": active_pipelines,
        "total_pipeline_opps": total_pipeline_opps,
        "total_pipeline_value": total_pipeline_value,
        # Email
        "email_data": email_data,
        "email_totals": email_totals,
        # Clients
        "clients": clients,
        "rag_counts": rag_counts,
        # Team
        "team": team,
        # Brief
        "brief": brief,
        # Sources
        "sources": sources,
        "refreshes": refreshes,
        # Growth
        "growth": growth,
        # Meta
        "now": datetime.now(),
    })


@app.get("/refresh")
async def manual_refresh(request: Request):
    if not verify_session(request):
        return RedirectResponse(url="/login")
    await collect_all()
    return RedirectResponse(url="/", status_code=303)


@app.post("/api/chat")
async def chat_endpoint(request: Request):
    if not verify_session(request):
        return {"error": "Not authenticated"}
    from app.chat import chat_with_cameron
    body = await request.json()
    question = body.get("question", "")
    if not question:
        return {"error": "No question provided"}
    response = await chat_with_cameron(question)
    return {"response": response}
