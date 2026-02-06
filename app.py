"""FastAPI app for stock gainers dashboard."""
import uvicorn
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse

from src.polygon_client import PolygonClient
from src.gainers_engine import GainersEngine

app = FastAPI(title="Stock Gainers")
TEMPLATE_PATH = Path(__file__).parent / "templates" / "dashboard.html"


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return TEMPLATE_PATH.read_text()


@app.get("/gainers")
async def get_gainers(
    api_key: str = Query(..., description="Polygon API key"),
    minutes: int = Query(10, ge=1, le=30),
    premarket: bool = Query(False),
):
    try:
        client = PolygonClient(api_key)
        engine = GainersEngine(client, top_n=20, lookback_minutes=minutes)
        reports = await engine.get_top_gainers(premarket=premarket)

        return {
            "time": datetime.now().isoformat(),
            "minutes": minutes,
            "premarket": premarket,
            "gainers": [
                {
                    "ticker": r.ticker,
                    "name": r.name,
                    "price": r.current_price,
                    "volume": r.volume,
                    "gain_window": r.gain_10min_percent,
                    "gain_day": r.gain_day_percent,
                }
                for r in reports
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)