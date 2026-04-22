import os
import time
import requests
from typing import Any

BASE_URL = "https://api.airtable.com/v0"
META_URL = "https://api.airtable.com/v0/meta"


class AirtableClient:
    def __init__(self):
        self.api_key = os.environ["AIRTABLE_API_KEY"]
        self.base_id = os.environ.get("AIRTABLE_BASE_ID", "appHbiHFRAWtx2ErO")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def fetch_all(self, table_id: str, fields: list[str] | None = None,
                  filter_formula: str | None = None) -> list[dict]:
        records = []
        params: dict[str, Any] = {}
        if fields:
            params["fields[]"] = fields
        if filter_formula:
            params["filterByFormula"] = filter_formula

        url = f"{BASE_URL}/{self.base_id}/{table_id}"
        while True:
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset
            time.sleep(0.2)
        return records

    def create_record(self, table_id: str, fields: dict) -> dict:
        url = f"{BASE_URL}/{self.base_id}/{table_id}"
        resp = self.session.post(url, json={"fields": fields})
        resp.raise_for_status()
        return resp.json()

    def find_existing(self, table_id: str, rec_key: str) -> str | None:
        formula = f"{{Rec Key}}='{rec_key}'"
        records = self.fetch_all(table_id, fields=["Rec Key", "Status"],
                                 filter_formula=formula)
        for r in records:
            if r["fields"].get("Status") == "Pending":
                return r["id"]
        return None
