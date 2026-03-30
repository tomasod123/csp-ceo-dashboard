"""Cameron England doctrine logic for RAG status and constraint identification."""

def classify_rag(client_data: dict) -> str:
    roas = client_data.get("roas")
    cpa = client_data.get("cpa")
    cpl = client_data.get("cpl")
    show_rate = client_data.get("show_rate")
    close_rate = client_data.get("close_rate")
    booking_rate = client_data.get("booking_rate")

    # Layer 1: If ROAS is profitable, it's green (Golden Rule)
    if roas and roas >= 3.0:
        return "green"

    if roas and roas < 1.5:
        return "red"

    # Layer 2 warning lights
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

    # No data = amber (can't assess)
    if not any([roas, cpl, show_rate, close_rate]):
        return "amber"

    return "green"


def identify_constraint(client_data: dict) -> tuple[str, str]:
    """Returns (pillar, detail) for the primary constraint."""
    cpl = client_data.get("cpl")
    cpb = client_data.get("cpb")
    booking_rate = client_data.get("booking_rate")
    show_rate = client_data.get("show_rate")
    close_rate = client_data.get("close_rate")

    # Check left to right: Leads > Bookings > Shows > Closes
    # Golden Rule: if CPB < $100, skip CPL (it's working)
    if cpb and cpb < 100:
        pass  # Skip lead flow, campaign is working
    elif cpl and cpl > 17:
        return "LEADS", f"CPL at ${cpl:.0f} (target: <$12). Creative/targeting/offer issue."

    if booking_rate is not None and booking_rate < 0.10:
        return "BOOKINGS", f"Booking rate at {booking_rate:.0%} (target: 30%+). Nurture/form friction/setter issue."

    if show_rate is not None and show_rate < 0.60:
        return "SHOWS", f"Show rate at {show_rate:.0%} (target: 75%+). Confirmation sequence/deposit/booking window issue."

    if close_rate is not None and close_rate < 0.25:
        return "CLOSES", f"Close rate at {close_rate:.0%} (target: 30%+). Sales process/offer/objection handling issue."

    return "NONE", "All pillars within benchmarks."


def growth_math(current_clients: int, target_revenue: float = 200000, avg_client_value: float = 3000, months_remaining: int = 6, monthly_churn: float = 0.05) -> dict:
    """Reverse-engineer what's needed to hit $200K/month."""
    target_clients = int(target_revenue / avg_client_value)
    gap = target_clients - current_clients
    monthly_churn_count = int(current_clients * monthly_churn)
    new_per_month = int(gap / months_remaining) + monthly_churn_count

    # Pipeline math (Cameron's benchmarks)
    close_rate = 0.25
    show_rate = 0.70
    qualified_shows = int(new_per_month / close_rate)
    booked_calls = int(qualified_shows / show_rate)

    return {
        "target_clients": target_clients,
        "current_clients": current_clients,
        "gap": gap,
        "months_remaining": months_remaining,
        "new_per_month": new_per_month,
        "monthly_churn_count": monthly_churn_count,
        "qualified_shows_needed": qualified_shows,
        "booked_calls_needed": booked_calls,
        "on_track": gap <= 0,
    }


# B2B benchmarks for display
B2B_BENCHMARKS = {
    "meta_cpl": {"target": "$20-80", "red_flag": ">$100"},
    "booking_rate": {"target": "30%+", "red_flag": "<15%"},
    "show_rate": {"target": "60-80%", "red_flag": "<50%"},
    "close_rate": {"target": "20-30%", "red_flag": "<15%"},
    "speed_to_lead": {"target": "<5 min", "red_flag": ">30 min"},
    "email_reply_rate": {"target": "3-8%", "red_flag": "<2%"},
    "email_positive_rate": {"target": "1-3%", "red_flag": "<0.5%"},
    "fe_roas": {"target": "3-4x", "red_flag": "<2x"},
}

# B2C client benchmarks
B2C_BENCHMARKS = {
    "cpl": {"target": "<$12", "red_flag": ">$17"},
    "cpb": {"target": "<$100", "red_flag": ">$150"},
    "show_rate_deposit": {"target": "75%+", "red_flag": "<60%"},
    "show_rate_no_deposit": {"target": "50%+", "red_flag": "<40%"},
    "close_rate": {"target": "30-40%", "red_flag": "<25%"},
    "upfront_cash": {"target": "40%+", "red_flag": "<30%"},
    "avg_sale": {"target": "$1,000+", "red_flag": "<$500"},
}
