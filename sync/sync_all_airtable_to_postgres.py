from __future__ import annotations

import argparse

from airtable_postgres_common import (
    TABLES,
    default_recent_window,
    fetch_airtable_records,
    get_table_config,
    load_dotenv,
    map_record,
    parse_optional_date,
    print_json,
    upsert_rows,
)


DEFAULT_TABLES = [
    "sales_daily_actual",
    "sales_plan_daily",
    "plan_vs_actual_summary",
    "inventory_snapshots",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync multiple Airtable tables into PostgreSQL in one run.")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        choices=sorted(TABLES.keys()),
        help="Tables to sync. Defaults to all supported heavy tables.",
    )
    parser.add_argument("--from-date", help="Inclusive source date, YYYY-MM-DD. Defaults to UTC yesterday minus 6 days.")
    parser.add_argument("--to-date", help="Inclusive source date, YYYY-MM-DD. Defaults to UTC yesterday.")
    parser.add_argument("--dry-run", action="store_true", help="Read and map records without writing to PostgreSQL.")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue syncing remaining tables after an error.")
    args = parser.parse_args()

    load_dotenv()
    default_from, default_to = default_recent_window()
    from_date = parse_optional_date(args.from_date) or default_from
    to_date = parse_optional_date(args.to_date) or default_to

    summary = []
    failures = []
    for table_name in args.tables:
        config = get_table_config(table_name)
        try:
            records = fetch_airtable_records(config, from_date=from_date, to_date=to_date)
            mapped = []
            skipped = []
            for record in records:
                try:
                    mapped.append(map_record(table_name, record))
                except Exception as exc:  # noqa: BLE001 - diagnostics should keep batch moving
                    skipped.append({"record_id": record.get("id"), "error": str(exc)})
            counts = upsert_rows(table_name, mapped, dry_run=args.dry_run)
            counts["fetched"] = len(records)
            counts["mapped"] = len(mapped)
            counts["skipped"] += len(skipped)

            summary.append(
                {
                    "table": table_name,
                    "status": "ok",
                    "counts": counts,
                    "skipped_samples": skipped[:10],
                }
            )
        except Exception as exc:  # noqa: BLE001 - surface clear per-table errors
            failures.append({"table": table_name, "error": str(exc)})
            summary.append({"table": table_name, "status": "error", "error": str(exc)})
            if not args.continue_on_error:
                break

    print_json(
        {
            "mode": "sync_all",
            "dry_run": args.dry_run,
            "from_date": from_date,
            "to_date": to_date,
            "tables": args.tables,
            "summary": summary,
            "failures": failures,
        }
    )

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

