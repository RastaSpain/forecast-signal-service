from dataclasses import dataclass, field
from datetime import date


@dataclass
class WeekRecord:
    product_id: str
    asin: str
    marketplace: str
    period: date
    planned_units: float
    actual_units: float
    delta_pct: float
    status: str


@dataclass
class SeasonEvent:
    record_id: str
    event_name: str
    phase: str
    start_md: str   # "MM-DD"
    end_md: str     # "MM-DD"
    coefficient: float
    marketplace: str


@dataclass
class ProductFactor:
    key: str        # ProductID-EventName-Marketplace-Phase
    planned_factor: float
    actual_factor_last_year: float | None


@dataclass
class Signal:
    signal_type: str      # BASELINE_DRIFT | EVENT_COEFF_ERROR | TREND | SUPPLY_GAP
    product_id: str
    marketplace: str
    event_name: str
    phase: str
    date_range: str
    planned_value: float
    actual_value: float
    delta_pct: float
    confidence: str       # Low | Medium | High
    weeks_analyzed: int
    justification: str
    recommendation: str
    weeks: list = field(default_factory=list)
