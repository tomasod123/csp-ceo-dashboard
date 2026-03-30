"""CSP CEO Dashboard -- Private, password-protected business intelligence."""

import os
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Form, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.models import init_db, get_latest_pipeline, get_latest_email, get_clients, get_growth_data, get_last_refresh
from app.cameron import classify_rag, identify_constraint, growth_math, B2B_BENCHMARKS, B2C_BENCHMARKS
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
    # Run initial data collection
    try:
        await collect_all()
    except Exception as e:
        print(f"Initial collection error: {e}")
    # Schedule data refresh every 30 minutes
    scheduler.add_job(collect_all, "interval", minutes=30, id="data_refresh")
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="CSP CEO Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


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

    # Get data
    pipeline_uk = get_latest_pipeline("CSP_UK")
    pipeline_legacy = get_latest_pipeline("CSP_LEGACY")
    email_data = get_latest_email()
    clients = get_clients()
    growth = get_growth_data()
    refreshes = get_last_refresh()

    # Calculate pipeline totals
    pipeline_summary = {}
    for p in pipeline_uk + pipeline_legacy:
        name = p["pipeline_name"]
        if name not in pipeline_summary:
            pipeline_summary[name] = {"stages": [], "total_opps": 0, "total_value": 0}
        pipeline_summary[name]["stages"].append(p)
        pipeline_summary[name]["total_opps"] += p["opportunity_count"]
        pipeline_summary[name]["total_value"] += p["total_value"]

    # Email totals
    email_totals = {
        "total_sent": sum(e["total_sent"] for e in email_data),
        "total_opened": sum(e["total_opened"] for e in email_data),
        "total_replied": sum(e["total_replied"] for e in email_data),
        "total_bounced": sum(e["total_bounced"] for e in email_data),
        "campaigns": len(email_data),
    }
    if email_totals["total_sent"] > 0:
        email_totals["open_rate"] = email_totals["total_opened"] / email_totals["total_sent"] * 100
        email_totals["reply_rate"] = email_totals["total_replied"] / email_totals["total_sent"] * 100
        email_totals["bounce_rate"] = email_totals["total_bounced"] / email_totals["total_sent"] * 100
    else:
        email_totals["open_rate"] = email_totals["reply_rate"] = email_totals["bounce_rate"] = 0

    # Growth math
    current_clients = len(clients) if clients else 10
    months_left = max(1, (datetime(2026, 9, 30) - datetime.now()).days // 30)
    gm = growth_math(current_clients, months_remaining=months_left)

    # Cameron analysis for each client
    for c in clients:
        c["rag"] = classify_rag(c)
        pillar, detail = identify_constraint(c)
        c["constraint_pillar"] = pillar
        c["constraint_detail"] = detail

    return templates.TemplateResponse(request=request, name="dashboard.html", context={
        "pipeline_summary": pipeline_summary,
        "pipeline_uk": pipeline_uk,
        "pipeline_legacy": pipeline_legacy,
        "email_data": email_data,
        "email_totals": email_totals,
        "clients": clients,
        "growth": gm,
        "refreshes": refreshes,
        "b2b_benchmarks": B2B_BENCHMARKS,
        "b2c_benchmarks": B2C_BENCHMARKS,
        "now": datetime.now(),
    })


@app.get("/refresh")
async def manual_refresh(request: Request):
    if not verify_session(request):
        return RedirectResponse(url="/login")
    await collect_all()
    return RedirectResponse(url="/", status_code=303)
