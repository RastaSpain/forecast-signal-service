from datetime import date, timedelta
from airtable_client import AirtableClient
from models import WeekRecord, SeasonEvent, ProductFactor

# Airtable table IDs
TBL_PVA_SUMMARY = "tblq7q2k4yLkaIU4f"
TBL_SEASON_EVENTS = "tbl5vFbTJwP41MRlM"
TBL_PROD_SEASONALITY = "tblxB00XhJ2Nyf2x8"
TBL_INVENTORY_FORECAST = "tblU17E0bqiQ8PMfD"
TBL_RECOMMENDATIONS = "tbl9m0uQr0tcfARqt"

LOOKBACK_WEEKS = 12


def load_week_records(client: AirtableClient) -> list[WeekRecord]:
    cutoff = (date.today() - timedelta(weeks=LOOKBACK_WEEKS)).isoformat()
    formula = f"AND({{Period Type}}='week', {{Period}}>='{cutoff}')"
    raw = client.fetch_all(
        TBL_PVA_SUMMARY,
        fields=["Product ID", "ASIN", "Marketplace", "Period",
                "Planned Units", "Actual Units", "Delta Percent", "Status"],
        filter_formula=formula,
    )
    records = []
    for r in raw:
        f = r["fields"]
        period_raw = f.get("Period")
        if not period_raw:
            continue
        try:
            period = date.fromisoformat(period_raw)
        except ValueError:
            continue
        records.append(WeekRecord(
            product_id=f.get("Product ID", ""),
            asin=f.get("ASIN", ""),
            marketplace=f.get("Marketplace", ""),
            period=period,
            planned_units=float(f.get("Planned Units") or 0),
            actual_units=float(f.get("Actual Units") or 0),
            delta_pct=float(f.get("Delta Percent") or 0),
            status=f.get("Status", ""),
        ))
    return records


def load_season_events(client: AirtableClient) -> list[SeasonEvent]:
    raw = client.fetch_all(
        TBL_SEASON_EVENTS,
        fields=["Event name", "Phase", "Start (MM-DD)", "End (MM-DD)",
                "fldVUlOR7103tqF38",   # коэфициент
                "Default factor", "Marketplace"],
    )
    events = []
    for r in raw:
        f = r["fields"]
        coeff = f.get("fldVUlOR7103tqF38") or f.get("Default factor") or 1.0

        # Marketplace is a linked field — extract names from lookup
        marketplace_val = f.get("Marketplace", "")
        if isinstance(marketplace_val, list):
            # linked records — we'd need a lookup; skip per-marketplace for now
            marketplace = ""
        else:
            marketplace = marketplace_val

        events.append(SeasonEvent(
            record_id=r["id"],
            event_name=f.get("Event name", ""),
            phase=f.get("Phase", ""),
            start_md=f.get("Start (MM-DD)", ""),
            end_md=f.get("End (MM-DD)", ""),
            coefficient=float(coeff),
            marketplace=marketplace,
        ))
    return events


def load_product_factors(client: AirtableClient) -> dict[str, ProductFactor]:
    raw = client.fetch_all(
        TBL_PROD_SEASONALITY,
        fields=["KeyProductSeasonality", "Planned factor", "Actual factor last year"],
    )
    result = {}
    for r in raw:
        f = r["fields"]
        key = f.get("KeyProductSeasonality", "")
        if not key:
            continue
        result[key] = ProductFactor(
            key=key,
            planned_factor=float(f.get("Planned factor") or 1.0),
            actual_factor_last_year=float(f["Actual factor last year"])
            if f.get("Actual factor last year") else None,
        )
    return result


def load_inventory_forecast(client: AirtableClient) -> list[dict]:
    cutoff = (date.today() - timedelta(weeks=LOOKBACK_WEEKS)).isoformat()
    formula = f"{{Target Date}}>='{cutoff}'"
    raw = client.fetch_all(
        TBL_INVENTORY_FORECAST,
        fields=["Product ID", "Marketplace", "Target Date",
                "Projected Stock", "Days of Supply"],
        filter_formula=formula,
    )
    return [r["fields"] for r in raw]
