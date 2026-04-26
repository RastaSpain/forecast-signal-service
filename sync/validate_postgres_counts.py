from __future__ import annotations

import argparse
from collections import defaultdict

from airtable_postgres_common import (
    build_table_arg,
    count_postgres_inventory_snapshots,
    count_postgres_plan_vs_actual_summary,
    count_postgres_sales_plan_daily,
    count_postgres_sales_daily_actual,
    fetch_airtable_records,
    get_table_config,
    group_postgres_inventory_snapshots,
    group_postgres_plan_vs_actual_summary,
    group_postgres_sales_plan_daily,
    group_postgres_sales_daily_actual,
    load_dotenv,
    parse_optional_date,
    print_json,
)


def extract_inventory_marketplace(fields: dict) -> str | None:
    raw = fields.get("Marketplace (from Maketplace)")
    if isinstance(raw, list):
        for item in raw:
            if item not in (None, ""):
                return str(item)
        return None
    if raw not in (None, ""):
        return str(raw)
    fallback = fields.get("Marketplace")
    if fallback in (None, ""):
        return None
    return str(fallback)


def extract_sales_plan_marketplace(fields: dict) -> str | None:
    raw = fields.get("Marketplace (from Marketplace) (from Listing ID)")
    if isinstance(raw, list):
        for item in raw:
            if item not in (None, ""):
                return str(item)
    if raw not in (None, ""):
        return str(raw)
    fallback = fields.get("Marketplace")
    if fallback in (None, ""):
        return None
    return str(fallback)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Airtable record counts against PostgreSQL rows.")
    build_table_arg(parser)
    parser.add_argument("--from-date", help="Inclusive source date, YYYY-MM-DD.")
    parser.add_argument("--to-date", help="Inclusive source date, YYYY-MM-DD.")
    parser.add_argument("--marketplace", help="Optional marketplace filter, for example USA, CA, UK.")
    parser.add_argument("--group-by-date-marketplace", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    from_date = parse_optional_date(args.from_date)
    to_date = parse_optional_date(args.to_date)
    config = get_table_config(args.table)

    records = fetch_airtable_records(config, from_date=from_date, to_date=to_date)
    if args.marketplace:
        if args.table == "inventory_snapshots":
            records = [
                r
                for r in records
                if extract_inventory_marketplace(r.get("fields", {})) == args.marketplace
            ]
        elif args.table == "sales_plan_daily":
            records = [
                r
                for r in records
                if extract_sales_plan_marketplace(r.get("fields", {})) == args.marketplace
            ]
        else:
            records = [r for r in records if r.get("fields", {}).get("Marketplace") == args.marketplace]

    comparable_records = records
    if args.table == "inventory_snapshots":
        deduped: dict[tuple[str, str, str], tuple[str, dict]] = {}
        for record in records:
            fields = record.get("fields", {})
            date_value = str(fields.get("Created") or fields.get("lastUpdatedTime") or "")[:10]
            asin = str(fields.get("asin") or fields.get("ASIN") or "")
            marketplace = extract_inventory_marketplace(fields) or ""
            if not date_value or not asin or not marketplace:
                continue
            key = (date_value, asin, marketplace)
            timestamp = str(fields.get("lastUpdatedTime") or fields.get("Created") or "")
            current = deduped.get(key)
            if current is None or timestamp >= current[0]:
                deduped[key] = (timestamp, record)
        comparable_records = [entry[1] for entry in deduped.values()]

    if args.table == "sales_daily_actual":
        pg_count = count_postgres_sales_daily_actual(from_date, to_date, args.marketplace)
    elif args.table == "sales_plan_daily":
        pg_count = count_postgres_sales_plan_daily(from_date, to_date, args.marketplace)
    elif args.table == "plan_vs_actual_summary":
        pg_count = count_postgres_plan_vs_actual_summary(from_date, to_date, args.marketplace)
    elif args.table == "inventory_snapshots":
        pg_count = count_postgres_inventory_snapshots(from_date, to_date, args.marketplace)
    else:
        raise RuntimeError(f"Validation not implemented for {args.table}")
    payload = {
        "table": args.table,
        "from_date": from_date,
        "to_date": to_date,
        "marketplace": args.marketplace,
        "airtable_count": len(comparable_records),
        "postgres_count": pg_count,
        "delta": pg_count - len(comparable_records),
    }
    if args.table == "inventory_snapshots":
        payload["airtable_count_raw"] = len(records)

    if args.group_by_date_marketplace:
        airtable_groups: dict[tuple[str, str], int] = defaultdict(int)
        for record in comparable_records:
            fields = record.get("fields", {})
            if args.table == "sales_daily_actual":
                date_value = fields.get("Date")
                marketplace = fields.get("Marketplace")
            elif args.table == "sales_plan_daily":
                date_value = fields.get("Date")
                marketplace = extract_sales_plan_marketplace(fields)
            elif args.table == "plan_vs_actual_summary":
                date_value = fields.get("Period")
                marketplace = fields.get("Marketplace")
            else:
                date_value = str(fields.get("Created") or fields.get("lastUpdatedTime") or "")[:10] or None
                marketplace = extract_inventory_marketplace(fields)
            key = (date_value, marketplace)
            airtable_groups[key] += 1
        if args.table == "sales_daily_actual":
            pg_groups = group_postgres_sales_daily_actual(from_date, to_date)
        elif args.table == "sales_plan_daily":
            pg_groups = group_postgres_sales_plan_daily(from_date, to_date)
        elif args.table == "plan_vs_actual_summary":
            pg_groups = group_postgres_plan_vs_actual_summary(from_date, to_date)
        else:
            pg_groups = group_postgres_inventory_snapshots(from_date, to_date)
        payload["postgres_groups"] = pg_groups
        payload["airtable_groups"] = [
            {"date": key[0], "marketplace": key[1], "rows": value}
            for key, value in sorted(
                airtable_groups.items(),
                key=lambda item: ((item[0][0] or ""), (item[0][1] or "")),
                reverse=True,
            )
        ]

    print_json(payload)


if __name__ == "__main__":
    main()
