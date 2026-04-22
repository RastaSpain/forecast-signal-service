import os
from fastapi import FastAPI, HTTPException, Header
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Forecast Signal Service")

API_KEY = os.environ.get("SERVICE_API_KEY", "")


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
