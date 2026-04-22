from datetime import date
from models import SeasonEvent


def _md_to_ordinal(md: str, year: int) -> int:
    """Convert MM-DD to day-of-year ordinal for comparison."""
    month, day = int(md[:2]), int(md[3:])
    return date(year, month, day).toordinal()


def classify_date(d: date, events: list[SeasonEvent], marketplace: str) -> tuple[str, str, float]:
    """
    Returns (event_name, phase, coefficient) for the given date and marketplace.
    Falls back to ("Regular", "Reg", 1.0) if no event matches.
    """
    market_events = [e for e in events if e.marketplace == marketplace or e.marketplace == ""]

    for event in market_events:
        if not event.start_md or not event.end_md:
            continue
        try:
            start_ord = _md_to_ordinal(event.start_md, d.year)
            end_ord = _md_to_ordinal(event.end_md, d.year)
            d_ord = d.toordinal()

            # handle year wrap (e.g. start in Dec, end in Jan next year)
            if start_ord > end_ord:
                if d_ord >= start_ord or d_ord <= end_ord:
                    return event.event_name, event.phase, event.coefficient
            else:
                if start_ord <= d_ord <= end_ord:
                    return event.event_name, event.phase, event.coefficient
        except (ValueError, AttributeError):
            continue

    return "Regular", "Reg", 1.0


def classify_week(week_start: date, events: list[SeasonEvent], marketplace: str) -> tuple[str, str, float]:
    """Use the middle of the week (Wednesday) for classification."""
    from datetime import timedelta
    mid = week_start + timedelta(days=3)
    return classify_date(mid, events, marketplace)
