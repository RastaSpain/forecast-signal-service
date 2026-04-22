from datetime import date, timedelta


def was_oos(product_id: str, marketplace: str,
            week_start: date, week_end: date,
            forecast_records: list[dict]) -> bool:
    """
    Returns True if the product was out of stock during the week.
    Uses Inventory Forecast Results: Projected Stock <= 0 or Days of Supply == 0.
    """
    relevant = [
        r for r in forecast_records
        if r.get("Product ID") == product_id
        and r.get("Marketplace") == marketplace
    ]
    if not relevant:
        return False

    for rec in relevant:
        target_raw = rec.get("Target Date")
        if not target_raw:
            continue
        try:
            target = date.fromisoformat(target_raw)
        except ValueError:
            continue
        if week_start <= target <= week_end:
            projected = rec.get("Projected Stock", 1)
            days_supply = rec.get("Days of Supply", 1)
            if (projected is not None and projected <= 0) or \
               (days_supply is not None and days_supply == 0):
                return True
    return False


def actual_is_suspiciously_zero(weeks: list) -> bool:
    """
    Heuristic: if the last 2+ consecutive weeks had actual_units == 0
    while planned > 0, likely OOS even without forecast data.
    """
    zero_streak = 0
    for w in reversed(weeks):
        if w.actual_units == 0 and w.planned_units > 0:
            zero_streak += 1
        else:
            break
    return zero_streak >= 2
