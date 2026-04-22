"""
Forecast Signal Service
Запуск: python main.py [--dry-run]
"""
import os
import sys
import hashlib
from datetime import date
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

from airtable_client import AirtableClient
from loader import (load_week_records, load_season_events,
                    load_product_factors, load_inventory_forecast,
                    TBL_RECOMMENDATIONS)
from period_classifier import classify_week
from signal_detectors import (detect_baseline_drift, detect_event_coeff_error,
                               detect_trend, detect_supply_gap)
from models import Signal

DRY_RUN = "--dry-run" in sys.argv


def make_rec_key(signal: Signal) -> str:
    """Stable dedup key: same product+market+signal+event won't create duplicates."""
    raw = f"{signal.product_id}|{signal.marketplace}|{signal.signal_type}|{signal.event_name}|{signal.phase}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def write_signal(client: AirtableClient, signal: Signal) -> None:
    rec_key = make_rec_key(signal)

    if not DRY_RUN:
        existing = client.find_existing(TBL_RECOMMENDATIONS, rec_key)
        if existing:
            print(f"  [SKIP] уже есть Pending: {rec_key} — {signal.signal_type} {signal.product_id}/{signal.marketplace}")
            return

    fields = {
        "Rec Key": rec_key,
        "Product ID": signal.product_id,
        "Marketplace": signal.marketplace,
        "Event Name": signal.event_name,
        "Phase": signal.phase,
        "Date Range": signal.date_range,
        "Signal Type": signal.signal_type,
        "Planned Value": signal.planned_value,
        "Actual Value": signal.actual_value,
        "Delta Pct": signal.delta_pct,
        "Confidence": signal.confidence,
        "Justification": signal.justification,
        "Recommendation": signal.recommendation,
        "Status": "Pending",
        "Weeks Analyzed": signal.weeks_analyzed,
    }

    if DRY_RUN:
        print(f"\n  [DRY-RUN] {signal.signal_type} | {signal.product_id} / {signal.marketplace}")
        print(f"    Event: {signal.event_name} / {signal.phase}")
        print(f"    Delta: {signal.delta_pct}% | Confidence: {signal.confidence}")
        print(f"    {signal.justification[:120]}...")
        print(f"    -> {signal.recommendation[:120]}")
    else:
        client.create_record(TBL_RECOMMENDATIONS, fields)
        print(f"  [CREATED] {signal.signal_type} | {signal.product_id} / {signal.marketplace} | {signal.confidence}")


def run():
    print(f"=== Forecast Signal Service {'[DRY-RUN]' if DRY_RUN else ''} {date.today()} ===\n")

    client = AirtableClient()

    print("Загружаю данные из Airtable...")
    week_records = load_week_records(client)
    season_events = load_season_events(client)
    product_factors = load_product_factors(client)
    forecast_inventory = load_inventory_forecast(client)

    print(f"  Недельных записей план/факт: {len(week_records)}")
    print(f"  Сезонных событий: {len(season_events)}")
    print(f"  Продуктовых коэффициентов: {len(product_factors)}")
    print(f"  Записей прогноза остатков: {len(forecast_inventory)}\n")

    # Группируем по Product+Marketplace
    groups: dict[tuple, list] = defaultdict(list)
    for w in week_records:
        groups[(w.product_id, w.marketplace)].append(w)

    # Сортируем каждую группу по дате
    for key in groups:
        groups[key].sort(key=lambda w: w.period)

    signals_found = 0
    print(f"Анализирую {len(groups)} комбинаций Product+Marketplace...\n")

    for (product_id, marketplace), weeks in groups.items():
        if not product_id or not marketplace:
            continue

        # Разбиваем недели по типу периода
        regular_weeks = []
        event_buckets: dict[tuple, list] = defaultdict(list)  # (event_name, phase) → weeks

        for w in weeks:
            event_name, phase, coeff = classify_week(w.period, season_events, marketplace)
            if event_name == "Regular":
                regular_weeks.append(w)
            else:
                event_buckets[(event_name, phase)].append(w)

        product_signals: list[Signal] = []

        # 1. SUPPLY GAP (приоритет — проверяем первым)
        sig = detect_supply_gap(weeks, forecast_inventory)
        if sig:
            product_signals.append(sig)

        # 2. BASELINE DRIFT (только если нет SUPPLY_GAP)
        if not any(s.signal_type == "SUPPLY_GAP" for s in product_signals):
            sig = detect_baseline_drift(regular_weeks, forecast_inventory)
            if sig:
                product_signals.append(sig)

        # 3. TREND
        if not any(s.signal_type == "SUPPLY_GAP" for s in product_signals):
            sig = detect_trend(regular_weeks, forecast_inventory)
            if sig:
                product_signals.append(sig)

        # 4. EVENT COEFFICIENT ERROR
        for (event_name, phase), ev_weeks in event_buckets.items():
            # Ищем плановый коэффициент для этого продукта
            factor_key = f"{product_id}-{event_name}-{marketplace}-{phase}"
            pf = product_factors.get(factor_key)
            planned_coeff = pf.planned_factor if pf else None

            # Fallback: берём коэффициент из Seasonality Events
            if planned_coeff is None:
                for e in season_events:
                    if e.event_name == event_name and e.phase == phase:
                        planned_coeff = e.coefficient
                        break

            if planned_coeff and planned_coeff > 1.0:
                sig = detect_event_coeff_error(
                    ev_weeks, event_name, phase, planned_coeff, forecast_inventory
                )
                if sig:
                    product_signals.append(sig)

        # Пишем сигналы
        for sig in product_signals:
            write_signal(client, sig)
            signals_found += 1

    print(f"\n=== Готово. Сигналов сгенерировано: {signals_found} ===")


if __name__ == "__main__":
    run()
