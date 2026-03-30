"""Cameron England doctrine: RAG classification, constraint ID, and inline threshold colors."""


# --- RAG Classification ---

def classify_rag(client_data: dict) -> str:
    roas = client_data.get("roas")
    cpl = client_data.get("cpl")
    show_rate = client_data.get("show_rate")
    close_rate = client_data.get("close_rate")
    booking_rate = client_data.get("booking_rate")

    if roas and roas >= 3.0:
        return "green"
    if roas and roas < 1.5:
        return "red"

    warnings = 0
    if cpl and cpl > 17:
        warnings += 1
    if show_rate is not None and show_rate < 0.50:
        warnings += 1
    if close_rate is not None and close_rate < 0.20:
        warnings += 1
    if booking_rate is not None and booking_rate < 0.10:
        warnings += 1

    if warnings >= 3:
        return "red"
    if warnings >= 1:
        return "amber"
    if not any([roas, cpl, show_rate, close_rate]):
        return "amber"
    return "green"


def identify_constraint(client_data: dict) -> tuple[str, str]:
    cpl = client_data.get("cpl")
    cpb = client_data.get("cpb")
    booking_rate = client_data.get("booking_rate")
    show_rate = client_data.get("show_rate")
    close_rate = client_data.get("close_rate")

    if cpb and cpb < 100:
        pass
    elif cpl and cpl > 17:
        return "LEADS", f"CPL ${cpl:.0f} (target <$12)"

    if booking_rate is not None and booking_rate < 0.10:
        return "BOOKINGS", f"Booking {booking_rate:.0%} (target 30%+)"

    if show_rate is not None and show_rate < 0.60:
        return "SHOWS", f"Show rate {show_rate:.0%} (target 75%+)"

    if close_rate is not None and close_rate < 0.25:
        return "CLOSES", f"Close rate {close_rate:.0%} (target 30%+)"

    return "OK", "All pillars within benchmarks"


def growth_math(current_clients: int, target_revenue: float = 200000,
                avg_client_value: float = 3000, months_remaining: int = 6,
                monthly_churn: float = 0.05) -> dict:
    target_clients = int(target_revenue / avg_client_value)
    gap = target_clients - current_clients
    monthly_churn_count = int(current_clients * monthly_churn)
    new_per_month = max(0, int(gap / max(1, months_remaining)) + monthly_churn_count)

    close_rate = 0.25
    show_rate = 0.70
    qualified_shows = int(new_per_month / close_rate) if close_rate else 0
    booked_calls = int(qualified_shows / show_rate) if show_rate else 0

    return {
        "target_clients": target_clients,
        "current_clients": current_clients,
        "gap": max(0, gap),
        "months_remaining": months_remaining,
        "new_per_month": new_per_month,
        "monthly_churn_count": monthly_churn_count,
        "qualified_shows_needed": qualified_shows,
        "booked_calls_needed": booked_calls,
        "on_track": gap <= 0,
    }


# --- Inline Threshold Colors (registered as Jinja2 globals) ---
# Every metric on the dashboard gets colored by Cameron's benchmarks.

def color_cpl(val):
    """B2C CPL: <$12 green, $12-17 amber, >$17 red"""
    if val is None:
        return ""
    if val <= 12:
        return "text-green"
    if val <= 17:
        return "text-amber"
    return "text-red"


def color_cpb(val):
    """B2C CPB: <$100 green, $100-150 amber, >$150 red"""
    if val is None:
        return ""
    if val <= 100:
        return "text-green"
    if val <= 150:
        return "text-amber"
    return "text-red"


def color_show_rate(val):
    """Show rate: >75% green, 50-75% amber, <50% red"""
    if val is None:
        return ""
    if val >= 75:
        return "text-green"
    if val >= 50:
        return "text-amber"
    return "text-red"


def color_close_rate(val):
    """Close rate: >30% green, 20-30% amber, <20% red"""
    if val is None:
        return ""
    if val >= 30:
        return "text-green"
    if val >= 20:
        return "text-amber"
    return "text-red"


def color_roas(val):
    """ROAS: >3x green, 1.5-3x amber, <1.5x red"""
    if val is None:
        return ""
    if val >= 3.0:
        return "text-green"
    if val >= 1.5:
        return "text-amber"
    return "text-red"


def color_open_rate(val):
    """Email open rate: >40% green, 30-40% amber, <30% red"""
    if val is None:
        return ""
    if val >= 40:
        return "text-green"
    if val >= 30:
        return "text-amber"
    return "text-red"


def color_reply_rate(val):
    """Email reply rate: >3% green, 2-3% amber, <2% red"""
    if val is None:
        return ""
    if val >= 3:
        return "text-green"
    if val >= 2:
        return "text-amber"
    return "text-red"


def color_bounce_rate(val):
    """Bounce rate: <3% green, 3-5% amber, >5% red"""
    if val is None:
        return ""
    if val <= 3:
        return "text-green"
    if val <= 5:
        return "text-amber"
    return "text-red"


def color_mom(val):
    """Month-over-month: >10% green, 0-10% amber, <0% red"""
    if val is None:
        return ""
    if val >= 10:
        return "text-green"
    if val >= 0:
        return "text-amber"
    return "text-red"


def color_b2b_cpl(val):
    """B2B Meta CPL: $20-80 green, $80-100 amber, >$100 red"""
    if val is None:
        return ""
    if val <= 80:
        return "text-green"
    if val <= 100:
        return "text-amber"
    return "text-red"


def color_speed_to_lead(val_minutes):
    """Speed to lead: <5min green, 5-30min amber, >30min red"""
    if val_minutes is None:
        return ""
    if val_minutes <= 5:
        return "text-green"
    if val_minutes <= 30:
        return "text-amber"
    return "text-red"


# All color functions for Jinja2 registration
COLOR_FUNCTIONS = {
    "color_cpl": color_cpl,
    "color_cpb": color_cpb,
    "color_show_rate": color_show_rate,
    "color_close_rate": color_close_rate,
    "color_roas": color_roas,
    "color_open_rate": color_open_rate,
    "color_reply_rate": color_reply_rate,
    "color_bounce_rate": color_bounce_rate,
    "color_mom": color_mom,
    "color_b2b_cpl": color_b2b_cpl,
    "color_speed_to_lead": color_speed_to_lead,
}
