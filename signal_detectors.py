from datetime import timedelta
from models import WeekRecord, Signal, ProductFactor
from oos_checker import was_oos, actual_is_suspiciously_zero

BASELINE_THRESHOLD = 0.20   # 20% устойчивого отклонения
BASELINE_MIN_WEEKS = 3
EVENT_COEFF_THRESHOLD = 0.25  # 25% расхождение фактического lift от планового
TREND_MIN_WEEKS = 4


def _fmt_range(weeks: list[WeekRecord]) -> str:
    if not weeks:
        return ""
    start = min(w.period for w in weeks)
    end = max(w.period for w in weeks) + timedelta(days=6)
    return f"{start} – {end}"


def _avg_delta(weeks: list[WeekRecord]) -> float:
    deltas = [w.delta_pct for w in weeks if w.planned_units > 0]
    return sum(deltas) / len(deltas) if deltas else 0.0


def _std_delta(weeks: list[WeekRecord]) -> float:
    import math
    deltas = [w.delta_pct for w in weeks if w.planned_units > 0]
    if len(deltas) < 2:
        return 0.0
    mean = sum(deltas) / len(deltas)
    variance = sum((x - mean) ** 2 for x in deltas) / len(deltas)
    return math.sqrt(variance)


def _confidence(std: float, n_weeks: int) -> str:
    if n_weeks >= 5 and std < 0.15:
        return "High"
    if n_weeks >= 3 and std < 0.30:
        return "Medium"
    return "Low"


# ── BASELINE DRIFT ────────────────────────────────────────────────────────────

def detect_baseline_drift(regular_weeks: list[WeekRecord],
                          forecast_records: list[dict]) -> Signal | None:
    """
    Для Regular-периода: если rolling delta 3+ недель подряд стабильно > ±20%.
    Исключаем недели с OOS.
    """
    if len(regular_weeks) < BASELINE_MIN_WEEKS:
        return None

    clean_weeks = []
    for w in regular_weeks:
        week_end = w.period + timedelta(days=6)
        oos = was_oos(w.product_id, w.marketplace, w.period, week_end, forecast_records)
        if not oos:
            clean_weeks.append(w)

    if len(clean_weeks) < BASELINE_MIN_WEEKS:
        return None

    # Проверяем последние N недель подряд
    check = clean_weeks[-BASELINE_MIN_WEEKS:]
    avg = _avg_delta(check)
    std = _std_delta(check)

    if abs(avg) < BASELINE_THRESHOLD:
        return None

    # Все проверяемые недели должны быть в одном направлении
    direction = 1 if avg > 0 else -1
    if not all((w.delta_pct * direction) > 0 for w in check if w.planned_units > 0):
        return None

    product_id = check[0].product_id
    marketplace = check[0].marketplace
    avg_planned = sum(w.planned_units for w in check) / len(check)
    avg_actual = sum(w.actual_units for w in check) / len(check)
    direction_word = "завышен" if avg < 0 else "занижен"
    adj_pct = round(avg_actual / avg_planned, 2) if avg_planned else 1.0

    justification = (
        f"За последние {len(check)} регулярных недели ({_fmt_range(check)}) "
        f"средние фактические продажи: {avg_actual:.1f} ед/нед, "
        f"плановые: {avg_planned:.1f} ед/нед. "
        f"Устойчивое отклонение {avg*100:.1f}% (std={std*100:.1f}%). "
        f"Все {len(check)} недель отклонение в одном направлении. "
        f"OOS не обнаружен."
    )
    recommendation = (
        f"Базовый план для {product_id} / {marketplace} {direction_word}. "
        f"Рекомендуется скорректировать на коэффициент ×{adj_pct} "
        f"(~{avg_actual:.0f} ед/нед вместо ~{avg_planned:.0f} ед/нед)."
    )

    return Signal(
        signal_type="BASELINE_DRIFT",
        product_id=product_id,
        marketplace=marketplace,
        event_name="Regular",
        phase="Reg",
        date_range=_fmt_range(check),
        planned_value=round(avg_planned, 2),
        actual_value=round(avg_actual, 2),
        delta_pct=round(avg * 100, 1),
        confidence=_confidence(std, len(check)),
        weeks_analyzed=len(check),
        justification=justification,
        recommendation=recommendation,
        weeks=check,
    )


# ── EVENT COEFFICIENT ERROR ───────────────────────────────────────────────────

def detect_event_coeff_error(event_weeks: list[WeekRecord],
                              event_name: str, phase: str,
                              planned_coeff: float,
                              forecast_records: list[dict]) -> Signal | None:
    """
    Сравниваем фактический lift с плановым коэффициентом события.
    lift = (avg_actual / avg_regular_baseline) — нужен regular_baseline,
    поэтому принимаем его через planned_coeff: если planned_coeff != 1,
    считаем actual_lift = avg_actual_event / (avg_planned_event / planned_coeff).
    """
    if not event_weeks or planned_coeff <= 1.0:
        return None

    clean = [w for w in event_weeks
             if not was_oos(w.product_id, w.marketplace, w.period,
                            w.period + timedelta(days=6), forecast_records)]
    if not clean:
        return None

    avg_planned = sum(w.planned_units for w in clean) / len(clean)
    avg_actual = sum(w.actual_units for w in clean) / len(clean)
    if avg_planned == 0:
        return None

    implied_base = avg_planned / planned_coeff
    if implied_base == 0:
        return None
    actual_lift = avg_actual / implied_base
    lift_error = abs(actual_lift - planned_coeff) / planned_coeff

    if lift_error < EVENT_COEFF_THRESHOLD:
        return None

    product_id = clean[0].product_id
    marketplace = clean[0].marketplace
    std = _std_delta(clean)
    direction = "завышен" if actual_lift < planned_coeff else "занижен"

    justification = (
        f"Событие '{event_name}' / фаза '{phase}' ({_fmt_range(clean)}). "
        f"Плановый коэффициент: ×{planned_coeff}. "
        f"Фактический lift (по данным {len(clean)} нед): ×{actual_lift:.2f}. "
        f"Расхождение: {lift_error*100:.1f}%. "
        f"Средний план: {avg_planned:.1f} ед/нед, факт: {avg_actual:.1f} ед/нед. "
        f"OOS не обнаружен."
    )
    recommendation = (
        f"Коэффициент события '{event_name}' / '{phase}' для {product_id} / {marketplace} {direction}. "
        f"Рекомендуется пересмотреть с ×{planned_coeff} на ×{actual_lift:.2f} "
        f"в таблице Product Seasonality."
    )

    return Signal(
        signal_type="EVENT_COEFF_ERROR",
        product_id=product_id,
        marketplace=marketplace,
        event_name=event_name,
        phase=phase,
        date_range=_fmt_range(clean),
        planned_value=planned_coeff,
        actual_value=round(actual_lift, 2),
        delta_pct=round((actual_lift - planned_coeff) / planned_coeff * 100, 1),
        confidence=_confidence(std, len(clean)),
        weeks_analyzed=len(clean),
        justification=justification,
        recommendation=recommendation,
        weeks=clean,
    )


# ── TREND ─────────────────────────────────────────────────────────────────────

def detect_trend(all_weeks: list[WeekRecord],
                 forecast_records: list[dict]) -> Signal | None:
    """
    Нарастающий тренд: каждая следующая неделя отклонение растёт в одну сторону.
    Минимум 4 недели подряд с монотонным ростом/падением delta.
    """
    if len(all_weeks) < TREND_MIN_WEEKS:
        return None

    clean = [w for w in all_weeks
             if not was_oos(w.product_id, w.marketplace, w.period,
                            w.period + timedelta(days=6), forecast_records)]
    if len(clean) < TREND_MIN_WEEKS:
        return None

    recent = clean[-TREND_MIN_WEEKS:]
    deltas = [w.delta_pct for w in recent]

    # Монотонный рост или падение
    is_increasing = all(deltas[i] < deltas[i+1] for i in range(len(deltas)-1))
    is_decreasing = all(deltas[i] > deltas[i+1] for i in range(len(deltas)-1))

    if not (is_increasing or is_decreasing):
        return None

    product_id = recent[0].product_id
    marketplace = recent[0].marketplace
    direction_word = "нарастает вверх" if is_increasing else "нарастает вниз"
    slope = (deltas[-1] - deltas[0]) / (len(deltas) - 1)
    avg_planned = sum(w.planned_units for w in recent) / len(recent)
    avg_actual = sum(w.actual_units for w in recent) / len(recent)

    justification = (
        f"За {len(recent)} недели ({_fmt_range(recent)}) отклонение план/факт "
        f"монотонно {direction_word}: "
        f"{' → '.join(f'{d*100:.1f}%' for d in deltas)}. "
        f"Уклон: {slope*100:.1f}% в неделю. "
        f"OOS не обнаружен."
    )
    recommendation = (
        f"Базовая скорость продаж {product_id} / {marketplace} демонстрирует тренд. "
        f"Рекомендуется пересмотреть baseline: текущий факт ~{avg_actual:.0f} ед/нед "
        f"против плана ~{avg_planned:.0f} ед/нед."
    )

    return Signal(
        signal_type="TREND",
        product_id=product_id,
        marketplace=marketplace,
        event_name="Regular",
        phase="Reg",
        date_range=_fmt_range(recent),
        planned_value=round(avg_planned, 2),
        actual_value=round(avg_actual, 2),
        delta_pct=round(_avg_delta(recent) * 100, 1),
        confidence="Medium" if len(recent) >= 5 else "Low",
        weeks_analyzed=len(recent),
        justification=justification,
        recommendation=recommendation,
        weeks=recent,
    )


# ── SUPPLY GAP ────────────────────────────────────────────────────────────────

def detect_supply_gap(recent_weeks: list[WeekRecord],
                      forecast_records: list[dict]) -> Signal | None:
    """
    Недовыполнение плана при наличии OOS — сигнал о проблеме поставки.
    """
    oos_weeks = []
    for w in recent_weeks:
        if w.planned_units > 0 and w.actual_units < w.planned_units * 0.8:
            week_end = w.period + timedelta(days=6)
            if was_oos(w.product_id, w.marketplace, w.period, week_end, forecast_records) \
               or actual_is_suspiciously_zero(recent_weeks):
                oos_weeks.append(w)

    if not oos_weeks:
        return None

    product_id = oos_weeks[0].product_id
    marketplace = oos_weeks[0].marketplace
    lost_units = sum(w.planned_units - w.actual_units for w in oos_weeks)
    avg_planned = sum(w.planned_units for w in oos_weeks) / len(oos_weeks)
    avg_actual = sum(w.actual_units for w in oos_weeks) / len(oos_weeks)

    justification = (
        f"За период {_fmt_range(oos_weeks)} обнаружен OOS в {len(oos_weeks)} нед. "
        f"Недопродано ~{lost_units:.0f} ед суммарно. "
        f"Среднее фактическое: {avg_actual:.1f} ед/нед при плане {avg_planned:.1f} ед/нед. "
        f"Отклонение не является ошибкой прогноза — обусловлено отсутствием товара."
    )
    recommendation = (
        f"Supply Gap для {product_id} / {marketplace}. "
        f"Прогноз корректировать не нужно. "
        f"Рекомендуется проверить план закупок и сроки поставки."
    )

    return Signal(
        signal_type="SUPPLY_GAP",
        product_id=product_id,
        marketplace=marketplace,
        event_name="—",
        phase="—",
        date_range=_fmt_range(oos_weeks),
        planned_value=round(avg_planned, 2),
        actual_value=round(avg_actual, 2),
        delta_pct=round(_avg_delta(oos_weeks) * 100, 1),
        confidence="High" if len(oos_weeks) >= 2 else "Medium",
        weeks_analyzed=len(oos_weeks),
        justification=justification,
        recommendation=recommendation,
        weeks=oos_weeks,
    )
