import os
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from fastapi import FastAPI, HTTPException, Header
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Forecast Signal Service")

API_KEY = os.environ.get("SERVICE_API_KEY", "")
SYNC_SCRIPT = Path(__file__).resolve().parent / "sync" / "sync_all_airtable_to_postgres.py"
VALIDATE_SCRIPT = Path(__file__).resolve().parent / "sync" / "validate_postgres_counts.py"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run_analysis(x_api_key: str = Header(default="")):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        from main import run
        run()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def run_python_json(args: list[str], timeout_seconds: int = 900) -> dict:
    result = subprocess.run(
        [sys.executable, *args],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        cwd=str(Path(__file__).resolve().parent),
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"Command failed: {' '.join(args)}")
    output = (result.stdout or "").strip()
    if not output:
        return {}
    return json.loads(output)


@app.post("/sync/airtable-postgres")
def run_airtable_postgres_sync(
    x_api_key: str = Header(default=""),
    x_database_url: str = Header(default=""),
    from_date: str | None = None,
    to_date: str | None = None,
):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not SYNC_SCRIPT.exists() or not VALIDATE_SCRIPT.exists():
        raise HTTPException(status_code=500, detail="Sync scripts are missing on server")

    # Fallback for environments where DATABASE_URL is not configured in Railway variables.
    if not os.environ.get("DATABASE_URL") and x_database_url:
        os.environ["DATABASE_URL"] = x_database_url.strip()

    sync_args = [str(SYNC_SCRIPT)]
    if from_date:
        sync_args.extend(["--from-date", from_date])
    if to_date:
        sync_args.extend(["--to-date", to_date])

    try:
        sync_result = run_python_json(sync_args, timeout_seconds=1200)

        # Validate yesterday by default because this is the daily SLA window.
        anchor = datetime.now(timezone.utc).date() - timedelta(days=1)
        from_str = from_date or anchor.isoformat()
        to_str = to_date or anchor.isoformat()

        checks = []
        for table in ("sales_daily_actual", "plan_vs_actual_summary", "inventory_snapshots"):
            payload = run_python_json(
                [
                    str(VALIDATE_SCRIPT),
                    "--table",
                    table,
                    "--from-date",
                    from_str,
                    "--to-date",
                    to_str,
                ],
                timeout_seconds=600,
            )
            checks.append(payload)

        bad = [item for item in checks if int(item.get("delta", 0)) != 0]
        ok = len(bad) == 0

        response = {
            "status": "ok" if ok else "error",
            "sync": sync_result,
            "validation_window": {"from_date": from_str, "to_date": to_str},
            "checks": checks,
        }

        if not ok:
            raise HTTPException(status_code=500, detail=response)

        return response
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
