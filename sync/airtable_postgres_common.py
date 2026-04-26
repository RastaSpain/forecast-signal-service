from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from typing import Any, Iterable

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover - shown as a friendly runtime message
    psycopg = None
    dict_row = None
    Jsonb = None


BASE_ID_ENV = "AIRTABLE_BASE_ID"
API_KEY_ENV = "AIRTABLE_API_KEY"
DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_AIRTABLE_BASE_ID = "appHbiHFRAWtx2ErO"
SUPABASE_KEY_ENV = "supabase_KEY"
SUPABASE_PROJECT_REF_ENV = "SUPABASE_PROJECT_REF"


@dataclass(frozen=True)
class TableConfig:
    name: str
    airtable_table_env: str
    default_airtable_table: str
    postgres_table: str
    date_field: str
    unique_field: str
    date_filter_expr: str


TABLES: dict[str, TableConfig] = {
    "sales_daily_actual": TableConfig(
        name="sales_daily_actual",
        airtable_table_env="AIRTABLE_SALES_DAILY_ACTUAL_TABLE",
        default_airtable_table="tblBzk8zO0eOnRiDM",
        postgres_table="sales_daily_actual",
        date_field="Date",
        unique_field="sales_key",
        date_filter_expr="{Date}",
    ),
    "sales_plan_daily": TableConfig(
        name="sales_plan_daily",
        airtable_table_env="AIRTABLE_SALES_PLAN_DAILY_TABLE",
        default_airtable_table="tblRLB6E83lHg6h7b",
        postgres_table="sales_plan_daily",
        date_field="Created",
        unique_field="plan_key",
        date_filter_expr="DATETIME_FORMAT({Created}, 'YYYY-MM-DD')",
    ),
    "plan_vs_actual_summary": TableConfig(
        name="plan_vs_actual_summary",
        airtable_table_env="AIRTABLE_PLAN_VS_ACTUAL_SUMMARY_TABLE",
        default_airtable_table="tblq7q2k4yLkaIU4f",
        postgres_table="plan_vs_actual_summary",
        date_field="Period",
        unique_field="summary_key",
        date_filter_expr="DATETIME_FORMAT({Period}, 'YYYY-MM-DD')",
    ),
    "inventory_snapshots": TableConfig(
        name="inventory_snapshots",
        airtable_table_env="AIRTABLE_INVENTORY_SNAPSHOTS_TABLE",
        default_airtable_table="tblvdUXLGMbN5rVJL",
        postgres_table="inventory_snapshots",
        date_field="Created",
        unique_field="snapshot_date_asin_marketplace",
        date_filter_expr="DATETIME_FORMAT({Created}, 'YYYY-MM-DD')",
    ),
}


def load_dotenv() -> None:
    candidates: list[Path] = []
    env_file = os.getenv("ENV_FILE")
    if env_file:
        candidates.append(Path(env_file))
    candidates.append(Path.cwd() / ".env")
    candidates.append(Path.home() / ".env")

    for path in candidates:
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_table_config(table_name: str) -> TableConfig:
    try:
        return TABLES[table_name]
    except KeyError as exc:
        known = ", ".join(sorted(TABLES))
        raise RuntimeError(f"Unsupported table '{table_name}'. Supported tables: {known}") from exc


def parse_iso_date(value: str | None, field_name: str) -> date:
    if not value:
        raise ValueError(f"Missing required date field '{field_name}'")
    return date.fromisoformat(str(value)[:10])


def parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def default_recent_window() -> tuple[date, date]:
    anchor = datetime.now(timezone.utc).date() - timedelta(days=1)
    return anchor - timedelta(days=6), anchor


def airtable_request(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    api_key = require_env(API_KEY_ENV)
    base_id = os.getenv(BASE_ID_ENV, DEFAULT_AIRTABLE_BASE_ID)
    query = urllib.parse.urlencode(params or {}, doseq=True)
    url = f"https://api.airtable.com/v0/{base_id}/{path}"
    if query:
        url = f"{url}?{query}"

    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def airtable_formula_for_date_range(field_name: str, from_date: date | None, to_date: date | None) -> str | None:
    return airtable_formula_for_date_expr(f"{{{field_name}}}", from_date, to_date)


def airtable_formula_for_date_expr(date_expr: str, from_date: date | None, to_date: date | None) -> str | None:
    filters: list[str] = []
    if from_date:
        filters.append(f"{date_expr}>='{from_date.isoformat()}'")
    if to_date:
        filters.append(f"{date_expr}<='{to_date.isoformat()}'")
    if not filters:
        return None
    if len(filters) == 1:
        return filters[0]
    return f"AND({', '.join(filters)})"


def fetch_airtable_records(config: TableConfig, from_date: date | None = None, to_date: date | None = None) -> list[dict[str, Any]]:
    table_id = os.getenv(config.airtable_table_env, config.default_airtable_table)
    params: dict[str, Any] = {"pageSize": 100}
    formula = airtable_formula_for_date_expr(config.date_filter_expr, from_date, to_date)
    if formula:
        params["filterByFormula"] = formula

    records: list[dict[str, Any]] = []
    while True:
        payload = airtable_request(table_id, params)
        records.extend(payload.get("records", []))
        offset = payload.get("offset")
        if not offset:
            break
        params["offset"] = offset
    return records


def fetch_airtable_date_bounds(config: TableConfig) -> tuple[date, date]:
    table_id = os.getenv(config.airtable_table_env, config.default_airtable_table)
    non_blank_formula = f"NOT({{{config.date_field}}} = BLANK())"
    params_asc = {
        "pageSize": 1,
        "sort[0][field]": config.date_field,
        "sort[0][direction]": "asc",
        "filterByFormula": non_blank_formula,
    }
    params_desc = {
        "pageSize": 1,
        "sort[0][field]": config.date_field,
        "sort[0][direction]": "desc",
        "filterByFormula": non_blank_formula,
    }
    first_payload = airtable_request(table_id, params_asc)
    last_payload = airtable_request(table_id, params_desc)
    first_record = (first_payload.get("records") or [None])[0]
    last_record = (last_payload.get("records") or [None])[0]
    if not first_record or not last_record:
        raise RuntimeError(f"No Airtable records found for {config.name}")
    first_date = parse_iso_date(first_record["fields"].get(config.date_field), config.date_field)
    last_date = parse_iso_date(last_record["fields"].get(config.date_field), config.date_field)
    return first_date, last_date


def iter_date_chunks(start_date: date, end_date: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def to_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    return int(value)


def to_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))


def to_decimal(value: Any) -> Any:
    if value in (None, ""):
        return None
    return value


def first_non_empty(value: Any) -> Any:
    if isinstance(value, list):
        for item in value:
            if item not in (None, ""):
                return item
        return None
    if value in (None, ""):
        return None
    return value


def map_sales_daily_actual(record: dict[str, Any]) -> dict[str, Any]:
    fields = record.get("fields", {})
    row_date = parse_iso_date(fields.get("Date"), "Date")
    asin = fields.get("ASIN")
    marketplace = fields.get("Marketplace")
    if not asin:
        raise ValueError("Missing required field 'ASIN'")
    if not marketplace:
        raise ValueError("Missing required field 'Marketplace'")

    sales_key = fields.get("Key") or f"{row_date.isoformat()}-{asin}-{marketplace}"
    return {
        "sales_key": sales_key,
        "date": row_date,
        "asin": asin,
        "sku": fields.get("SKU"),
        "marketplace": marketplace,
        "listing_id": fields.get("Listing ID"),
        "product_id": fields.get("Product ID"),
        "key_product_market": fields.get("Key_ProductMarket"),
        "units_sold": to_int(fields.get("units")),
        "orders": to_int(fields.get("orders")),
        "gross_sales": to_decimal(fields.get("sales")),
        "net_sales": None,
        "price": to_decimal(fields.get("Price")),
        "currency": fields.get("currency"),
        "source": "airtable",
        "source_record_id": record.get("id"),
        "raw_payload": record,
    }


def build_summary_key(fields: dict[str, Any], period_start: date, period_type: str, asin: str, marketplace: str) -> str:
    return fields.get("Summary Key") or f"{period_type}-{period_start.isoformat()}-{asin}-{marketplace}"


def map_plan_vs_actual_summary(record: dict[str, Any]) -> dict[str, Any]:
    fields = record.get("fields", {})
    period_start = parse_iso_date(fields.get("Period"), "Period")
    period_type = fields.get("Period Type")
    asin = fields.get("ASIN")
    marketplace = fields.get("Marketplace")
    if not period_type:
        raise ValueError("Missing required field 'Period Type'")
    if not asin:
        raise ValueError("Missing required field 'ASIN'")
    if not marketplace:
        raise ValueError("Missing required field 'Marketplace'")

    actual_units = to_int(fields.get("Actual Units"))
    planned_units = to_decimal(fields.get("Planned Units")) or 0
    actual_revenue = None
    planned_revenue = None
    variance_units = fields.get("Delta Units")
    variance_pct = fields.get("Delta Percent")

    return {
        "period_type": period_type,
        "period_start": period_start,
        "period_end": period_start,
        "asin": asin,
        "sku": fields.get("SKU"),
        "marketplace": marketplace,
        "listing_id": fields.get("Listing ID"),
        "product_id": fields.get("Product ID"),
        "summary_key": build_summary_key(fields, period_start, period_type, asin, marketplace),
        "planned_units": planned_units,
        "actual_units": actual_units,
        "planned_revenue": planned_revenue,
        "actual_revenue": actual_revenue,
        "variance_units": variance_units,
        "variance_pct": variance_pct,
        "status": fields.get("Status"),
        "source": "airtable",
        "source_record_id": record.get("id"),
        "raw_payload": record,
    }


def map_sales_plan_daily(record: dict[str, Any]) -> dict[str, Any]:
    fields = record.get("fields", {})
    plan_date = parse_iso_date(fields.get("Date"), "Date")
    asin = (
        first_non_empty(fields.get("ASIN (from Listing ID)"))
        or first_non_empty(fields.get("ASIN (from Listing ID) 2"))
        or first_non_empty(fields.get("ASIN (from Listing on PoductMarket)"))
    )
    marketplace = (
        first_non_empty(fields.get("Marketplace (from Marketplace) (from Listing ID)"))
        or first_non_empty(fields.get("Marketplace (from Marketplace) (from KeyProductMarket)"))
        or first_non_empty(fields.get("Marketplace"))
    )
    if not asin:
        raise ValueError("Missing required field 'ASIN (from Listing ID)'")
    if not marketplace:
        raise ValueError("Missing required marketplace field")

    listing_id = first_non_empty(fields.get("Listing ID"))
    plan_key = fields.get("PlanKey") or f"{plan_date.isoformat()}-{asin}-{marketplace}"
    planned_units = to_decimal(fields.get("Planned units")) or 0
    price = to_decimal(first_non_empty(fields.get("Price (from Listing ID)")))
    planned_revenue = None
    if price not in (None, "") and planned_units not in (None, ""):
        planned_revenue = to_decimal(price) * to_decimal(planned_units)

    return {
        "plan_key": plan_key,
        "date": plan_date,
        "asin": asin,
        "sku": None,
        "marketplace": marketplace,
        "listing_id": listing_id,
        "key_product_market": first_non_empty(fields.get("KeyProductMarket (from Listing ID)")) or fields.get("KeyProductMarket"),
        "planned_units": planned_units,
        "planned_revenue": planned_revenue,
        "price": price,
        "currency": first_non_empty(fields.get("Валюта (from Listing ID)")) or first_non_empty(fields.get("ProductMarket Валюта Lookup")),
        "source": "airtable",
        "source_record_id": record.get("id"),
        "raw_payload": record,
    }


def extract_marketplace(value: Any) -> str | None:
    if isinstance(value, list):
        for item in value:
            if item not in (None, ""):
                return str(item)
        return None
    if value in (None, ""):
        return None
    return str(value)


def map_inventory_snapshots(record: dict[str, Any]) -> dict[str, Any]:
    fields = record.get("fields", {})
    snapshot_raw = fields.get("Created") or fields.get("lastUpdatedTime")
    snapshot_date = parse_iso_date(snapshot_raw, "Created")
    snapshot_timestamp = fields.get("Created") or fields.get("lastUpdatedTime")
    asin = fields.get("asin") or fields.get("ASIN")
    marketplace = extract_marketplace(fields.get("Marketplace (from Maketplace)")) or extract_marketplace(
        fields.get("Marketplace")
    )
    if not asin:
        raise ValueError("Missing required field 'asin'")
    if not marketplace:
        raise ValueError("Missing required field 'Marketplace (from Maketplace)'")

    return {
        "snapshot_date": snapshot_date,
        "snapshot_timestamp": snapshot_timestamp,
        "asin": asin,
        "sku": fields.get("fnSku") or fields.get("SKU"),
        "marketplace": marketplace,
        "listing_id": fields.get("Listing ID"),
        "fulfillable_quantity": to_optional_int(fields.get("SELLABLE_NOW")),
        "inbound_working_quantity": to_optional_int(fields.get("INBOUND_TOTAL")),
        "inbound_shipped_quantity": None,
        "inbound_receiving_quantity": None,
        "reserved_quantity": None,
        "total_warehouse_quantity": to_optional_int(fields.get("FBA_TOTAL_CONTROLLED")),
        "source": "airtable",
        "source_record_id": record.get("id"),
        "raw_payload": record,
    }


MAPPERS = {
    "sales_daily_actual": map_sales_daily_actual,
    "sales_plan_daily": map_sales_plan_daily,
    "plan_vs_actual_summary": map_plan_vs_actual_summary,
    "inventory_snapshots": map_inventory_snapshots,
}


def map_record(table_name: str, record: dict[str, Any]) -> dict[str, Any]:
    return MAPPERS[table_name](record)


def ensure_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("Missing dependency psycopg. Install with: pip install -r requirements.txt")


def connect_db():
    ensure_psycopg()
    return psycopg.connect(require_env(DATABASE_URL_ENV), row_factory=dict_row)


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return "'" + str(value).replace("'", "''") + "'"
    if isinstance(value, (dict, list)):
        return "'" + json.dumps(value, ensure_ascii=False).replace("'", "''") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def get_supabase_project_ref() -> str | None:
    explicit = os.getenv(SUPABASE_PROJECT_REF_ENV)
    if explicit:
        return explicit

    database_url = os.getenv(DATABASE_URL_ENV)
    if not database_url:
        return None

    hostname = urlparse(database_url).hostname or ""
    if hostname.startswith("db.") and hostname.endswith(".supabase.co"):
        parts = hostname.split(".")
        if len(parts) >= 3:
            return parts[1]
    return None


def run_sql_via_supabase(query: str) -> list[dict[str, Any]]:
    token = os.getenv(SUPABASE_KEY_ENV)
    project_ref = get_supabase_project_ref()
    if not token or not project_ref:
        raise RuntimeError("Supabase SQL API fallback is unavailable. Missing supabase_KEY or project ref.")

    req = urllib.request.Request(
        f"https://api.supabase.com/v1/projects/{project_ref}/database/query",
        data=json.dumps({"query": query}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code == 403 and os.name == "nt":
            return run_sql_via_supabase_powershell(query, token, project_ref)
        raise RuntimeError(f"Supabase SQL API failed: {detail}") from exc

    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    return []


def run_sql_via_supabase_powershell(query: str, token: str, project_ref: str) -> list[dict[str, Any]]:
    body = json.dumps({"query": query}, ensure_ascii=False).replace("'", "''")
    script = f"""
$headers = @{{ Authorization = "Bearer {token}"; "Content-Type" = "application/json" }}
$body = '{body}'
$resp = Invoke-RestMethod -Uri "https://api.supabase.com/v1/projects/{project_ref}/database/query" -Headers $headers -Method POST -Body $body -TimeoutSec 120
$resp | ConvertTo-Json -Depth 100 -Compress
""".strip()
    with tempfile.NamedTemporaryFile("w", suffix=".ps1", delete=False, encoding="utf-8") as handle:
        handle.write(script)
        script_path = handle.name
    try:
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-File", script_path],
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        try:
            os.unlink(script_path)
        except OSError:
            pass
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"Supabase SQL API PowerShell fallback failed: {stderr}")
    stdout = completed.stdout.strip()
    if not stdout:
        return []
    payload = json.loads(stdout)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    return []


def get_db_mode() -> str:
    database_url = os.getenv(DATABASE_URL_ENV)
    if database_url and psycopg is not None:
        try:
            with psycopg.connect(database_url, connect_timeout=5):
                return "direct"
        except Exception:
            pass

    if os.getenv(SUPABASE_KEY_ENV) and get_supabase_project_ref():
        return "supabase_sql_api"

    raise RuntimeError("No working database access found. Provide DATABASE_URL or a usable Supabase SQL API fallback.")


def run_select(sql: str) -> list[dict[str, Any]]:
    mode = get_db_mode()
    if mode == "direct":
        with connect_db() as conn:
            with conn.cursor() as cur:
                return list(cur.execute(sql).fetchall())
    return run_sql_via_supabase(sql)


def run_statement(sql: str) -> None:
    mode = get_db_mode()
    if mode == "direct":
        with connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        return
    run_sql_via_supabase(sql)


UPSERT_SALES_DAILY_ACTUAL_SQL = """
INSERT INTO sales_daily_actual (
  sales_key,
  date,
  asin,
  sku,
  marketplace,
  listing_id,
  product_id,
  key_product_market,
  units_sold,
  orders,
  gross_sales,
  net_sales,
  price,
  currency,
  source,
  source_record_id,
  raw_payload,
  source_deleted_at
) VALUES (
  %(sales_key)s,
  %(date)s,
  %(asin)s,
  %(sku)s,
  %(marketplace)s,
  %(listing_id)s,
  %(product_id)s,
  %(key_product_market)s,
  %(units_sold)s,
  %(orders)s,
  %(gross_sales)s,
  %(net_sales)s,
  %(price)s,
  %(currency)s,
  %(source)s,
  %(source_record_id)s,
  %(raw_payload)s,
  NULL
)
ON CONFLICT (sales_key) DO UPDATE SET
  date = EXCLUDED.date,
  asin = EXCLUDED.asin,
  sku = EXCLUDED.sku,
  marketplace = EXCLUDED.marketplace,
  listing_id = EXCLUDED.listing_id,
  product_id = EXCLUDED.product_id,
  key_product_market = EXCLUDED.key_product_market,
  units_sold = EXCLUDED.units_sold,
  orders = EXCLUDED.orders,
  gross_sales = EXCLUDED.gross_sales,
  net_sales = EXCLUDED.net_sales,
  price = EXCLUDED.price,
  currency = EXCLUDED.currency,
  source = EXCLUDED.source,
  source_record_id = EXCLUDED.source_record_id,
  raw_payload = EXCLUDED.raw_payload,
  source_deleted_at = NULL
RETURNING (xmax = 0) AS inserted
"""


UPSERT_PLAN_VS_ACTUAL_SUMMARY_SQL = """
INSERT INTO plan_vs_actual_summary (
  period_type,
  period_start,
  period_end,
  asin,
  sku,
  marketplace,
  listing_id,
  product_id,
  summary_key,
  planned_units,
  actual_units,
  planned_revenue,
  actual_revenue,
  variance_units,
  variance_pct,
  status,
  source,
  source_record_id,
  raw_payload,
  source_deleted_at
) VALUES (
  %(period_type)s,
  %(period_start)s,
  %(period_end)s,
  %(asin)s,
  %(sku)s,
  %(marketplace)s,
  %(listing_id)s,
  %(product_id)s,
  %(summary_key)s,
  %(planned_units)s,
  %(actual_units)s,
  %(planned_revenue)s,
  %(actual_revenue)s,
  %(variance_units)s,
  %(variance_pct)s,
  %(status)s,
  %(source)s,
  %(source_record_id)s,
  %(raw_payload)s,
  NULL
)
ON CONFLICT (summary_key) DO UPDATE SET
  period_type = EXCLUDED.period_type,
  period_start = EXCLUDED.period_start,
  period_end = EXCLUDED.period_end,
  asin = EXCLUDED.asin,
  sku = EXCLUDED.sku,
  marketplace = EXCLUDED.marketplace,
  listing_id = EXCLUDED.listing_id,
  product_id = EXCLUDED.product_id,
  planned_units = EXCLUDED.planned_units,
  actual_units = EXCLUDED.actual_units,
  planned_revenue = EXCLUDED.planned_revenue,
  actual_revenue = EXCLUDED.actual_revenue,
  variance_units = EXCLUDED.variance_units,
  variance_pct = EXCLUDED.variance_pct,
  status = EXCLUDED.status,
  source = EXCLUDED.source,
  source_record_id = EXCLUDED.source_record_id,
  raw_payload = EXCLUDED.raw_payload,
  source_deleted_at = NULL
RETURNING (xmax = 0) AS inserted
"""


def upsert_rows(table_name: str, rows: Iterable[dict[str, Any]], dry_run: bool = False) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    rows = list(rows)
    if dry_run:
        counts["skipped"] = len(rows)
        return counts

    if get_db_mode() == "supabase_sql_api":
        if table_name == "sales_daily_actual":
            return upsert_sales_daily_actual_via_supabase(rows)
        if table_name == "sales_plan_daily":
            return upsert_sales_plan_daily_via_supabase(rows)
        if table_name == "plan_vs_actual_summary":
            return upsert_plan_vs_actual_summary_via_supabase(rows)
        if table_name == "inventory_snapshots":
            return upsert_inventory_snapshots_via_supabase(rows)
        raise RuntimeError(f"Supabase upsert not implemented for {table_name}")

    if table_name == "sales_daily_actual":
        return upsert_sales_daily_actual_direct(rows)
    if table_name == "sales_plan_daily":
        return upsert_sales_plan_daily_direct(rows)
    if table_name == "plan_vs_actual_summary":
        return upsert_plan_vs_actual_summary_direct(rows)
    if table_name == "inventory_snapshots":
        return upsert_inventory_snapshots_direct(rows)
    raise RuntimeError(f"Upsert not implemented for {table_name}")


def upsert_sales_daily_actual_direct(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts

    chunk_size = 500
    with connect_db() as conn:
        with conn.cursor() as cur:
            for index in range(0, len(rows), chunk_size):
                chunk = rows[index:index + chunk_size]
                payload = []
                for row in chunk:
                    item = dict(row)
                    item["date"] = item["date"].isoformat()
                    payload.append(item)
                result = cur.execute(
                    """
                    WITH payload AS (
                      SELECT %s::jsonb AS data
                    ),
                    existing_keys AS (
                      SELECT sales_key
                      FROM sales_daily_actual
                      WHERE sales_key IN (
                        SELECT sales_key
                        FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                          sales_key text
                        )
                      )
                    ),
                    upserted AS (
                      INSERT INTO sales_daily_actual (
                        sales_key,
                        date,
                        asin,
                        sku,
                        marketplace,
                        listing_id,
                        product_id,
                        key_product_market,
                        units_sold,
                        orders,
                        gross_sales,
                        net_sales,
                        price,
                        currency,
                        source,
                        source_record_id,
                        raw_payload,
                        source_deleted_at
                      )
                      SELECT
                        src.sales_key,
                        src.date::date,
                        src.asin,
                        src.sku,
                        src.marketplace,
                        src.listing_id,
                        src.product_id,
                        src.key_product_market,
                        src.units_sold,
                        src.orders,
                        src.gross_sales,
                        src.net_sales,
                        src.price,
                        src.currency,
                        src.source,
                        src.source_record_id,
                        src.raw_payload,
                        NULL
                      FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                        sales_key text,
                        date text,
                        asin text,
                        sku text,
                        marketplace text,
                        listing_id text,
                        product_id text,
                        key_product_market text,
                        units_sold integer,
                        orders integer,
                        gross_sales numeric,
                        net_sales numeric,
                        price numeric,
                        currency text,
                        source text,
                        source_record_id text,
                        raw_payload jsonb
                      )
                      ON CONFLICT (sales_key) DO UPDATE SET
                        date = EXCLUDED.date,
                        asin = EXCLUDED.asin,
                        sku = EXCLUDED.sku,
                        marketplace = EXCLUDED.marketplace,
                        listing_id = EXCLUDED.listing_id,
                        product_id = EXCLUDED.product_id,
                        key_product_market = EXCLUDED.key_product_market,
                        units_sold = EXCLUDED.units_sold,
                        orders = EXCLUDED.orders,
                        gross_sales = EXCLUDED.gross_sales,
                        net_sales = EXCLUDED.net_sales,
                        price = EXCLUDED.price,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        source_record_id = EXCLUDED.source_record_id,
                        raw_payload = EXCLUDED.raw_payload,
                        source_deleted_at = NULL
                      RETURNING sales_key
                    )
                    SELECT
                      (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
                      (SELECT count(*) FROM existing_keys) AS existing_rows
                    """,
                    (Jsonb(payload),),
                ).fetchone()
                counts["updated"] += int(result["existing_rows"])
                counts["inserted"] += int(result["chunk_rows"]) - int(result["existing_rows"])
        conn.commit()
    return counts


def upsert_sales_plan_daily_direct(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts

    chunk_size = 500
    with connect_db() as conn:
        with conn.cursor() as cur:
            for index in range(0, len(rows), chunk_size):
                chunk = rows[index:index + chunk_size]
                payload = []
                for row in chunk:
                    item = dict(row)
                    item["date"] = item["date"].isoformat()
                    payload.append(item)
                result = cur.execute(
                    """
                    WITH payload AS (
                      SELECT %s::jsonb AS data
                    ),
                    existing_keys AS (
                      SELECT plan_key
                      FROM sales_plan_daily
                      WHERE plan_key IN (
                        SELECT plan_key
                        FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                          plan_key text
                        )
                      )
                    ),
                    upserted AS (
                      INSERT INTO sales_plan_daily (
                        plan_key,
                        date,
                        asin,
                        sku,
                        marketplace,
                        listing_id,
                        key_product_market,
                        planned_units,
                        planned_revenue,
                        price,
                        currency,
                        source,
                        source_record_id,
                        raw_payload,
                        source_deleted_at
                      )
                      SELECT
                        src.plan_key,
                        src.date::date,
                        src.asin,
                        src.sku,
                        src.marketplace,
                        src.listing_id,
                        src.key_product_market,
                        src.planned_units,
                        src.planned_revenue,
                        src.price,
                        src.currency,
                        src.source,
                        src.source_record_id,
                        src.raw_payload,
                        NULL
                      FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                        plan_key text,
                        date text,
                        asin text,
                        sku text,
                        marketplace text,
                        listing_id text,
                        key_product_market text,
                        planned_units numeric,
                        planned_revenue numeric,
                        price numeric,
                        currency text,
                        source text,
                        source_record_id text,
                        raw_payload jsonb
                      )
                      ON CONFLICT (plan_key) DO UPDATE SET
                        date = EXCLUDED.date,
                        asin = EXCLUDED.asin,
                        sku = EXCLUDED.sku,
                        marketplace = EXCLUDED.marketplace,
                        listing_id = EXCLUDED.listing_id,
                        key_product_market = EXCLUDED.key_product_market,
                        planned_units = EXCLUDED.planned_units,
                        planned_revenue = EXCLUDED.planned_revenue,
                        price = EXCLUDED.price,
                        currency = EXCLUDED.currency,
                        source = EXCLUDED.source,
                        source_record_id = EXCLUDED.source_record_id,
                        raw_payload = EXCLUDED.raw_payload,
                        source_deleted_at = NULL
                      RETURNING plan_key
                    )
                    SELECT
                      (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
                      (SELECT count(*) FROM existing_keys) AS existing_rows
                    """,
                    (Jsonb(payload),),
                ).fetchone()
                counts["updated"] += int(result["existing_rows"])
                counts["inserted"] += int(result["chunk_rows"]) - int(result["existing_rows"])
        conn.commit()
    return counts


def upsert_plan_vs_actual_summary_direct(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts

    chunk_size = 500
    with connect_db() as conn:
        with conn.cursor() as cur:
            for index in range(0, len(rows), chunk_size):
                chunk = rows[index:index + chunk_size]
                payload = []
                for row in chunk:
                    item = dict(row)
                    item["period_start"] = item["period_start"].isoformat()
                    item["period_end"] = item["period_end"].isoformat() if item["period_end"] else None
                    payload.append(item)
                result = cur.execute(
                    """
                    WITH payload AS (
                      SELECT %s::jsonb AS data
                    ),
                    existing_keys AS (
                      SELECT summary_key
                      FROM plan_vs_actual_summary
                      WHERE summary_key IN (
                        SELECT summary_key
                        FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                          summary_key text
                        )
                      )
                    ),
                    upserted AS (
                      INSERT INTO plan_vs_actual_summary (
                        period_type,
                        period_start,
                        period_end,
                        asin,
                        sku,
                        marketplace,
                        listing_id,
                        product_id,
                        summary_key,
                        planned_units,
                        actual_units,
                        planned_revenue,
                        actual_revenue,
                        variance_units,
                        variance_pct,
                        status,
                        source,
                        source_record_id,
                        raw_payload,
                        source_deleted_at
                      )
                      SELECT
                        src.period_type,
                        src.period_start::date,
                        src.period_end::date,
                        src.asin,
                        src.sku,
                        src.marketplace,
                        src.listing_id,
                        src.product_id,
                        src.summary_key,
                        src.planned_units,
                        src.actual_units,
                        src.planned_revenue,
                        src.actual_revenue,
                        src.variance_units,
                        src.variance_pct,
                        src.status,
                        src.source,
                        src.source_record_id,
                        src.raw_payload,
                        NULL
                      FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                        period_type text,
                        period_start text,
                        period_end text,
                        asin text,
                        sku text,
                        marketplace text,
                        listing_id text,
                        product_id text,
                        summary_key text,
                        planned_units numeric,
                        actual_units integer,
                        planned_revenue numeric,
                        actual_revenue numeric,
                        variance_units numeric,
                        variance_pct numeric,
                        status text,
                        source text,
                        source_record_id text,
                        raw_payload jsonb
                      )
                      ON CONFLICT (summary_key) DO UPDATE SET
                        period_type = EXCLUDED.period_type,
                        period_start = EXCLUDED.period_start,
                        period_end = EXCLUDED.period_end,
                        asin = EXCLUDED.asin,
                        sku = EXCLUDED.sku,
                        marketplace = EXCLUDED.marketplace,
                        listing_id = EXCLUDED.listing_id,
                        product_id = EXCLUDED.product_id,
                        planned_units = EXCLUDED.planned_units,
                        actual_units = EXCLUDED.actual_units,
                        planned_revenue = EXCLUDED.planned_revenue,
                        actual_revenue = EXCLUDED.actual_revenue,
                        variance_units = EXCLUDED.variance_units,
                        variance_pct = EXCLUDED.variance_pct,
                        status = EXCLUDED.status,
                        source = EXCLUDED.source,
                        source_record_id = EXCLUDED.source_record_id,
                        raw_payload = EXCLUDED.raw_payload,
                        source_deleted_at = NULL
                      RETURNING summary_key
                    )
                    SELECT
                      (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
                      (SELECT count(*) FROM existing_keys) AS existing_rows
                    """,
                    (Jsonb(payload),),
                ).fetchone()
                counts["updated"] += int(result["existing_rows"])
                counts["inserted"] += int(result["chunk_rows"]) - int(result["existing_rows"])
        conn.commit()
    return counts


def upsert_inventory_snapshots_direct(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    rows = dedupe_inventory_rows(rows)

    chunk_size = 500
    with connect_db() as conn:
        with conn.cursor() as cur:
            for index in range(0, len(rows), chunk_size):
                chunk = rows[index:index + chunk_size]
                payload = []
                for row in chunk:
                    item = dict(row)
                    item["snapshot_date"] = item["snapshot_date"].isoformat()
                    payload.append(item)
                result = cur.execute(
                    """
                    WITH payload AS (
                      SELECT %s::jsonb AS data
                    ),
                    existing_keys AS (
                      SELECT snapshot_date, asin, marketplace
                      FROM inventory_snapshots
                      WHERE (snapshot_date, asin, marketplace) IN (
                        SELECT snapshot_date::date, asin, marketplace
                        FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                          snapshot_date text,
                          asin text,
                          marketplace text
                        )
                      )
                    ),
                    upserted AS (
                      INSERT INTO inventory_snapshots (
                        snapshot_date,
                        snapshot_timestamp,
                        asin,
                        sku,
                        marketplace,
                        listing_id,
                        fulfillable_quantity,
                        inbound_working_quantity,
                        inbound_shipped_quantity,
                        inbound_receiving_quantity,
                        reserved_quantity,
                        total_warehouse_quantity,
                        source,
                        source_record_id,
                        raw_payload,
                        source_deleted_at
                      )
                      SELECT
                        src.snapshot_date::date,
                        src.snapshot_timestamp::timestamptz,
                        src.asin,
                        src.sku,
                        src.marketplace,
                        src.listing_id,
                        src.fulfillable_quantity,
                        src.inbound_working_quantity,
                        src.inbound_shipped_quantity,
                        src.inbound_receiving_quantity,
                        src.reserved_quantity,
                        src.total_warehouse_quantity,
                        src.source,
                        src.source_record_id,
                        src.raw_payload,
                        NULL
                      FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
                        snapshot_date text,
                        snapshot_timestamp text,
                        asin text,
                        sku text,
                        marketplace text,
                        listing_id text,
                        fulfillable_quantity integer,
                        inbound_working_quantity integer,
                        inbound_shipped_quantity integer,
                        inbound_receiving_quantity integer,
                        reserved_quantity integer,
                        total_warehouse_quantity integer,
                        source text,
                        source_record_id text,
                        raw_payload jsonb
                      )
                      ON CONFLICT (snapshot_date, asin, marketplace) DO UPDATE SET
                        snapshot_timestamp = EXCLUDED.snapshot_timestamp,
                        sku = EXCLUDED.sku,
                        listing_id = EXCLUDED.listing_id,
                        fulfillable_quantity = EXCLUDED.fulfillable_quantity,
                        inbound_working_quantity = EXCLUDED.inbound_working_quantity,
                        inbound_shipped_quantity = EXCLUDED.inbound_shipped_quantity,
                        inbound_receiving_quantity = EXCLUDED.inbound_receiving_quantity,
                        reserved_quantity = EXCLUDED.reserved_quantity,
                        total_warehouse_quantity = EXCLUDED.total_warehouse_quantity,
                        source = EXCLUDED.source,
                        source_record_id = EXCLUDED.source_record_id,
                        raw_payload = EXCLUDED.raw_payload,
                        source_deleted_at = NULL
                      RETURNING snapshot_date
                    )
                    SELECT
                      (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
                      (SELECT count(*) FROM existing_keys) AS existing_rows
                    """,
                    (Jsonb(payload),),
                ).fetchone()
                counts["updated"] += int(result["existing_rows"])
                counts["inserted"] += int(result["chunk_rows"]) - int(result["existing_rows"])
        conn.commit()
    return counts


def upsert_sales_daily_actual_via_supabase(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts

    chunk_size = 100
    for index in range(0, len(rows), chunk_size):
        chunk = rows[index:index + chunk_size]
        payload = []
        for row in chunk:
            item = dict(row)
            item["date"] = item["date"].isoformat()
            payload.append(item)

        sql = f"""
        WITH payload AS (
          SELECT {sql_literal(payload)}::jsonb AS data
        ),
        existing_keys AS (
          SELECT sales_key
          FROM sales_daily_actual
          WHERE sales_key IN (
            SELECT sales_key
            FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
              sales_key text
            )
          )
        ),
        upserted AS (
          INSERT INTO sales_daily_actual (
            sales_key,
            date,
            asin,
            sku,
            marketplace,
            listing_id,
            product_id,
            key_product_market,
            units_sold,
            orders,
            gross_sales,
            net_sales,
            price,
            currency,
            source,
            source_record_id,
            raw_payload,
            source_deleted_at
          )
          SELECT
            src.sales_key,
            src.date::date,
            src.asin,
            src.sku,
            src.marketplace,
            src.listing_id,
            src.product_id,
            src.key_product_market,
            src.units_sold,
            src.orders,
            src.gross_sales,
            src.net_sales,
            src.price,
            src.currency,
            src.source,
            src.source_record_id,
            src.raw_payload,
            NULL
          FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
            sales_key text,
            date text,
            asin text,
            sku text,
            marketplace text,
            listing_id text,
            product_id text,
            key_product_market text,
            units_sold integer,
            orders integer,
            gross_sales numeric,
            net_sales numeric,
            price numeric,
            currency text,
            source text,
            source_record_id text,
            raw_payload jsonb
          )
          ON CONFLICT (sales_key) DO UPDATE SET
            date = EXCLUDED.date,
            asin = EXCLUDED.asin,
            sku = EXCLUDED.sku,
            marketplace = EXCLUDED.marketplace,
            listing_id = EXCLUDED.listing_id,
            product_id = EXCLUDED.product_id,
            key_product_market = EXCLUDED.key_product_market,
            units_sold = EXCLUDED.units_sold,
            orders = EXCLUDED.orders,
            gross_sales = EXCLUDED.gross_sales,
            net_sales = EXCLUDED.net_sales,
            price = EXCLUDED.price,
            currency = EXCLUDED.currency,
            source = EXCLUDED.source,
            source_record_id = EXCLUDED.source_record_id,
            raw_payload = EXCLUDED.raw_payload,
            source_deleted_at = NULL
          RETURNING sales_key
        )
        SELECT
          (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
          (SELECT count(*) FROM existing_keys) AS existing_rows;
        """
        result = run_select(sql)[0]
        chunk_rows = int(result["chunk_rows"])
        existing_rows = int(result["existing_rows"])
        counts["updated"] += existing_rows
        counts["inserted"] += chunk_rows - existing_rows

    return counts


def upsert_sales_plan_daily_via_supabase(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts

    chunk_size = 100
    for index in range(0, len(rows), chunk_size):
        chunk = rows[index:index + chunk_size]
        payload = []
        for row in chunk:
            item = dict(row)
            item["date"] = item["date"].isoformat()
            payload.append(item)

        sql = f"""
        WITH payload AS (
          SELECT {sql_literal(payload)}::jsonb AS data
        ),
        existing_keys AS (
          SELECT plan_key
          FROM sales_plan_daily
          WHERE plan_key IN (
            SELECT plan_key
            FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
              plan_key text
            )
          )
        ),
        upserted AS (
          INSERT INTO sales_plan_daily (
            plan_key,
            date,
            asin,
            sku,
            marketplace,
            listing_id,
            key_product_market,
            planned_units,
            planned_revenue,
            price,
            currency,
            source,
            source_record_id,
            raw_payload,
            source_deleted_at
          )
          SELECT
            src.plan_key,
            src.date::date,
            src.asin,
            src.sku,
            src.marketplace,
            src.listing_id,
            src.key_product_market,
            src.planned_units,
            src.planned_revenue,
            src.price,
            src.currency,
            src.source,
            src.source_record_id,
            src.raw_payload,
            NULL
          FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
            plan_key text,
            date text,
            asin text,
            sku text,
            marketplace text,
            listing_id text,
            key_product_market text,
            planned_units numeric,
            planned_revenue numeric,
            price numeric,
            currency text,
            source text,
            source_record_id text,
            raw_payload jsonb
          )
          ON CONFLICT (plan_key) DO UPDATE SET
            date = EXCLUDED.date,
            asin = EXCLUDED.asin,
            sku = EXCLUDED.sku,
            marketplace = EXCLUDED.marketplace,
            listing_id = EXCLUDED.listing_id,
            key_product_market = EXCLUDED.key_product_market,
            planned_units = EXCLUDED.planned_units,
            planned_revenue = EXCLUDED.planned_revenue,
            price = EXCLUDED.price,
            currency = EXCLUDED.currency,
            source = EXCLUDED.source,
            source_record_id = EXCLUDED.source_record_id,
            raw_payload = EXCLUDED.raw_payload,
            source_deleted_at = NULL
          RETURNING plan_key
        )
        SELECT
          (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
          (SELECT count(*) FROM existing_keys) AS existing_rows;
        """
        result = run_select(sql)[0]
        chunk_rows = int(result["chunk_rows"])
        existing_rows = int(result["existing_rows"])
        counts["updated"] += existing_rows
        counts["inserted"] += chunk_rows - existing_rows

    return counts


def upsert_plan_vs_actual_summary_via_supabase(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts

    chunk_size = 100
    for index in range(0, len(rows), chunk_size):
        chunk = rows[index:index + chunk_size]
        payload = []
        for row in chunk:
            item = dict(row)
            item["period_start"] = item["period_start"].isoformat()
            item["period_end"] = item["period_end"].isoformat() if item["period_end"] else None
            payload.append(item)

        sql = f"""
        WITH payload AS (
          SELECT {sql_literal(payload)}::jsonb AS data
        ),
        existing_keys AS (
          SELECT summary_key
          FROM plan_vs_actual_summary
          WHERE summary_key IN (
            SELECT summary_key
            FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
              summary_key text
            )
          )
        ),
        upserted AS (
          INSERT INTO plan_vs_actual_summary (
            period_type,
            period_start,
            period_end,
            asin,
            sku,
            marketplace,
            listing_id,
            product_id,
            summary_key,
            planned_units,
            actual_units,
            planned_revenue,
            actual_revenue,
            variance_units,
            variance_pct,
            status,
            source,
            source_record_id,
            raw_payload,
            source_deleted_at
          )
          SELECT
            src.period_type,
            src.period_start::date,
            src.period_end::date,
            src.asin,
            src.sku,
            src.marketplace,
            src.listing_id,
            src.product_id,
            src.summary_key,
            src.planned_units,
            src.actual_units,
            src.planned_revenue,
            src.actual_revenue,
            src.variance_units,
            src.variance_pct,
            src.status,
            src.source,
            src.source_record_id,
            src.raw_payload,
            NULL
          FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
            period_type text,
            period_start text,
            period_end text,
            asin text,
            sku text,
            marketplace text,
            listing_id text,
            product_id text,
            summary_key text,
            planned_units numeric,
            actual_units integer,
            planned_revenue numeric,
            actual_revenue numeric,
            variance_units numeric,
            variance_pct numeric,
            status text,
            source text,
            source_record_id text,
            raw_payload jsonb
          )
          ON CONFLICT (summary_key) DO UPDATE SET
            period_type = EXCLUDED.period_type,
            period_start = EXCLUDED.period_start,
            period_end = EXCLUDED.period_end,
            asin = EXCLUDED.asin,
            sku = EXCLUDED.sku,
            marketplace = EXCLUDED.marketplace,
            listing_id = EXCLUDED.listing_id,
            product_id = EXCLUDED.product_id,
            planned_units = EXCLUDED.planned_units,
            actual_units = EXCLUDED.actual_units,
            planned_revenue = EXCLUDED.planned_revenue,
            actual_revenue = EXCLUDED.actual_revenue,
            variance_units = EXCLUDED.variance_units,
            variance_pct = EXCLUDED.variance_pct,
            status = EXCLUDED.status,
            source = EXCLUDED.source,
            source_record_id = EXCLUDED.source_record_id,
            raw_payload = EXCLUDED.raw_payload,
            source_deleted_at = NULL
          RETURNING summary_key
        )
        SELECT
          (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
          (SELECT count(*) FROM existing_keys) AS existing_rows;
        """
        result = run_select(sql)[0]
        chunk_rows = int(result["chunk_rows"])
        existing_rows = int(result["existing_rows"])
        counts["updated"] += existing_rows
        counts["inserted"] += chunk_rows - existing_rows

    return counts


def upsert_inventory_snapshots_via_supabase(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    rows = dedupe_inventory_rows(rows)

    chunk_size = 100
    for index in range(0, len(rows), chunk_size):
        chunk = rows[index:index + chunk_size]
        payload = []
        for row in chunk:
            item = dict(row)
            item["snapshot_date"] = item["snapshot_date"].isoformat()
            payload.append(item)

        sql = f"""
        WITH payload AS (
          SELECT {sql_literal(payload)}::jsonb AS data
        ),
        existing_keys AS (
          SELECT snapshot_date, asin, marketplace
          FROM inventory_snapshots
          WHERE (snapshot_date, asin, marketplace) IN (
            SELECT snapshot_date::date, asin, marketplace
            FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
              snapshot_date text,
              asin text,
              marketplace text
            )
          )
        ),
        upserted AS (
          INSERT INTO inventory_snapshots (
            snapshot_date,
            snapshot_timestamp,
            asin,
            sku,
            marketplace,
            listing_id,
            fulfillable_quantity,
            inbound_working_quantity,
            inbound_shipped_quantity,
            inbound_receiving_quantity,
            reserved_quantity,
            total_warehouse_quantity,
            source,
            source_record_id,
            raw_payload,
            source_deleted_at
          )
          SELECT
            src.snapshot_date::date,
            src.snapshot_timestamp::timestamptz,
            src.asin,
            src.sku,
            src.marketplace,
            src.listing_id,
            src.fulfillable_quantity,
            src.inbound_working_quantity,
            src.inbound_shipped_quantity,
            src.inbound_receiving_quantity,
            src.reserved_quantity,
            src.total_warehouse_quantity,
            src.source,
            src.source_record_id,
            src.raw_payload,
            NULL
          FROM jsonb_to_recordset((SELECT data FROM payload)) AS src(
            snapshot_date text,
            snapshot_timestamp text,
            asin text,
            sku text,
            marketplace text,
            listing_id text,
            fulfillable_quantity integer,
            inbound_working_quantity integer,
            inbound_shipped_quantity integer,
            inbound_receiving_quantity integer,
            reserved_quantity integer,
            total_warehouse_quantity integer,
            source text,
            source_record_id text,
            raw_payload jsonb
          )
          ON CONFLICT (snapshot_date, asin, marketplace) DO UPDATE SET
            snapshot_timestamp = EXCLUDED.snapshot_timestamp,
            sku = EXCLUDED.sku,
            listing_id = EXCLUDED.listing_id,
            fulfillable_quantity = EXCLUDED.fulfillable_quantity,
            inbound_working_quantity = EXCLUDED.inbound_working_quantity,
            inbound_shipped_quantity = EXCLUDED.inbound_shipped_quantity,
            inbound_receiving_quantity = EXCLUDED.inbound_receiving_quantity,
            reserved_quantity = EXCLUDED.reserved_quantity,
            total_warehouse_quantity = EXCLUDED.total_warehouse_quantity,
            source = EXCLUDED.source,
            source_record_id = EXCLUDED.source_record_id,
            raw_payload = EXCLUDED.raw_payload,
            source_deleted_at = NULL
          RETURNING snapshot_date
        )
        SELECT
          (SELECT count(*) FROM payload, jsonb_array_elements(data)) AS chunk_rows,
          (SELECT count(*) FROM existing_keys) AS existing_rows;
        """
        result = run_select(sql)[0]
        chunk_rows = int(result["chunk_rows"])
        existing_rows = int(result["existing_rows"])
        counts["updated"] += existing_rows
        counts["inserted"] += chunk_rows - existing_rows

    return counts


def dedupe_inventory_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[date, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["snapshot_date"], str(row["asin"]), str(row["marketplace"]))
        current = deduped.get(key)
        if current is None:
            deduped[key] = row
            continue
        current_ts = str(current.get("snapshot_timestamp") or "")
        next_ts = str(row.get("snapshot_timestamp") or "")
        if next_ts >= current_ts:
            deduped[key] = row
    return list(deduped.values())


def mark_missing_sales_daily_actual(source_record_ids: set[str], from_date: date | None, to_date: date | None) -> int:
    if not source_record_ids:
        return 0
    if from_date is None or to_date is None:
        raise RuntimeError("Refusing to mark missing records without an explicit date range")

    if get_db_mode() == "direct":
        with connect_db() as conn:
            with conn.cursor() as cur:
                result = cur.execute(
                    """
                    UPDATE sales_daily_actual
                    SET source_deleted_at = now()
                    WHERE source = 'airtable'
                      AND date >= %(from_date)s
                      AND date <= %(to_date)s
                      AND source_deleted_at IS NULL
                      AND NOT (source_record_id = ANY(%(source_record_ids)s))
                    """,
                    {
                        "from_date": from_date,
                        "to_date": to_date,
                        "source_record_ids": list(source_record_ids),
                    },
                )
                return result.rowcount or 0

    ids = ", ".join(sql_literal(value) for value in sorted(source_record_ids))
    sql = f"""
    WITH updated AS (
      UPDATE sales_daily_actual
      SET source_deleted_at = now()
      WHERE source = 'airtable'
        AND date >= {sql_literal(from_date)}
        AND date <= {sql_literal(to_date)}
        AND source_deleted_at IS NULL
        AND source_record_id NOT IN ({ids})
      RETURNING 1
    )
    SELECT count(*) AS deleted_count FROM updated;
    """
    return int(run_select(sql)[0]["deleted_count"])


def count_postgres_sales_daily_actual(from_date: date | None, to_date: date | None, marketplace: str | None) -> int:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"date >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"date <= {sql_literal(to_date)}")
    if marketplace:
        clauses.append(f"marketplace = {sql_literal(marketplace)}")

    sql = f"SELECT count(*) AS count FROM sales_daily_actual WHERE {' AND '.join(clauses)}"
    return int(run_select(sql)[0]["count"])


def count_postgres_sales_plan_daily(from_date: date | None, to_date: date | None, marketplace: str | None) -> int:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"date >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"date <= {sql_literal(to_date)}")
    if marketplace:
        clauses.append(f"marketplace = {sql_literal(marketplace)}")

    sql = f"SELECT count(*) AS count FROM sales_plan_daily WHERE {' AND '.join(clauses)}"
    return int(run_select(sql)[0]["count"])


def count_postgres_plan_vs_actual_summary(from_date: date | None, to_date: date | None, marketplace: str | None) -> int:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"period_start >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"period_start <= {sql_literal(to_date)}")
    if marketplace:
        clauses.append(f"marketplace = {sql_literal(marketplace)}")

    sql = f"SELECT count(*) AS count FROM plan_vs_actual_summary WHERE {' AND '.join(clauses)}"
    return int(run_select(sql)[0]["count"])


def count_postgres_inventory_snapshots(from_date: date | None, to_date: date | None, marketplace: str | None) -> int:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"snapshot_date >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"snapshot_date <= {sql_literal(to_date)}")
    if marketplace:
        clauses.append(f"marketplace = {sql_literal(marketplace)}")

    sql = f"SELECT count(*) AS count FROM inventory_snapshots WHERE {' AND '.join(clauses)}"
    return int(run_select(sql)[0]["count"])


def group_postgres_sales_daily_actual(from_date: date | None, to_date: date | None) -> list[dict[str, Any]]:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"date >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"date <= {sql_literal(to_date)}")

    sql = f"""
    SELECT date, marketplace, count(*) AS rows, sum(units_sold) AS units_sold, sum(gross_sales) AS gross_sales
    FROM sales_daily_actual
    WHERE {' AND '.join(clauses)}
    GROUP BY date, marketplace
    ORDER BY date DESC, marketplace
    """
    return run_select(sql)


def group_postgres_sales_plan_daily(from_date: date | None, to_date: date | None) -> list[dict[str, Any]]:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"date >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"date <= {sql_literal(to_date)}")

    sql = f"""
    SELECT date, marketplace, count(*) AS rows, sum(planned_units) AS planned_units, sum(planned_revenue) AS planned_revenue
    FROM sales_plan_daily
    WHERE {' AND '.join(clauses)}
    GROUP BY date, marketplace
    ORDER BY date DESC, marketplace
    """
    return run_select(sql)


def group_postgres_plan_vs_actual_summary(from_date: date | None, to_date: date | None) -> list[dict[str, Any]]:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"period_start >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"period_start <= {sql_literal(to_date)}")

    sql = f"""
    SELECT period_start AS date, marketplace, period_type, count(*) AS rows
    FROM plan_vs_actual_summary
    WHERE {' AND '.join(clauses)}
    GROUP BY period_start, marketplace, period_type
    ORDER BY period_start DESC, marketplace, period_type
    """
    return run_select(sql)


def group_postgres_inventory_snapshots(from_date: date | None, to_date: date | None) -> list[dict[str, Any]]:
    clauses = ["source_deleted_at IS NULL"]
    if from_date:
        clauses.append(f"snapshot_date >= {sql_literal(from_date)}")
    if to_date:
        clauses.append(f"snapshot_date <= {sql_literal(to_date)}")

    sql = f"""
    SELECT snapshot_date AS date, marketplace, count(*) AS rows, sum(fulfillable_quantity) AS fulfillable_quantity
    FROM inventory_snapshots
    WHERE {' AND '.join(clauses)}
    GROUP BY snapshot_date, marketplace
    ORDER BY snapshot_date DESC, marketplace
    """
    return run_select(sql)


def build_table_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--table", required=True, choices=sorted(TABLES.keys()))


def print_json(payload: Any) -> None:
    print(json.dumps(payload, default=str, ensure_ascii=False, indent=2))


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)
