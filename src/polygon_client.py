"""Polygon.io API client for fetching stock market data."""
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import httpx
from dataclasses import dataclass


@dataclass
class StockBar:
    """Represents a single 1-minute OHLCV bar."""

    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    timestamp: datetime
    vwap: Optional[float] = None


@dataclass
class StockSnapshot:
    """Current snapshot of a stock including daily data."""

    ticker: str
    name: str
    current_price: float
    volume: int
    day_open: float
    day_change_percent: float
    bars: list[StockBar]


class PolygonClient:
    """Client for interacting with Polygon.io REST API."""

    BASE_URL = "https://api.polygon.io"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._ticker_names: dict[str, str] = {}
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create a reusable HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                limits=httpx.Limits(max_connections=50, max_keepalive_connections=50),
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, endpoint: str, params: dict = None) -> dict:
        """Make an authenticated request to Polygon API."""
        params = params or {}
        params["apiKey"] = self.api_key

        client = await self._get_client()
        response = await client.get(f"{self.BASE_URL}{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    async def get_gainers(self) -> list[dict]:
        """Get current top gainers from the market snapshot."""
        data = await self._request("/v2/snapshot/locale/us/markets/stocks/gainers")
        return data.get("tickers", [])

    async def get_all_tickers_snapshot(self) -> list[dict]:
        """Get snapshot of all US stock tickers."""
        data = await self._request(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params={"include_otc": "true"},
        )
        return data.get("tickers", [])

    async def get_ticker_details(self, ticker: str) -> dict:
        """Get details for a specific ticker including company name."""
        if ticker in self._ticker_names:
            return {"name": self._ticker_names[ticker]}

        try:
            data = await self._request(f"/v3/reference/tickers/{ticker}")
            result = data.get("results", {})
            name = result.get("name", ticker)
            self._ticker_names[ticker] = name
            return {"name": name}
        except Exception:
            return {"name": ticker}

    async def get_ticker_details_batch(self, tickers: list[str]) -> dict[str, str]:
        """Get company names for multiple tickers in a single API call."""
        # Return cached results, find uncached
        result = {}
        uncached = []
        for t in tickers:
            if t in self._ticker_names:
                result[t] = self._ticker_names[t]
            else:
                uncached.append(t)

        if not uncached:
            return result

        # Polygon /v3/reference/tickers supports comma-separated ticker filter
        # Process in chunks of 50 to stay within URL length limits
        for i in range(0, len(uncached), 50):
            chunk = uncached[i : i + 50]
            try:
                data = await self._request(
                    "/v3/reference/tickers",
                    params={"ticker.in": ",".join(chunk), "limit": len(chunk)},
                )
                for item in data.get("results", []):
                    name = item.get("name", item.get("ticker", ""))
                    ticker = item.get("ticker", "")
                    if ticker:
                        self._ticker_names[ticker] = name
                        result[ticker] = name
            except Exception:
                pass

        # Fill in any tickers that weren't found
        for t in uncached:
            if t not in result:
                result[t] = t

        return result

    async def get_aggregate_bars(
        self,
        ticker: str,
        from_date: datetime,
        to_date: datetime,
        timespan: str = "minute",
        multiplier: int = 1,
        premarket: bool = False,
    ) -> list[StockBar]:
        """Get aggregate bars for a ticker."""
        from_ts = from_date.strftime("%Y-%m-%d")
        to_ts = to_date.strftime("%Y-%m-%d")

        # Use timestamps for premarket to include extended hours
        params = {"adjusted": "true", "sort": "asc", "limit": 50000}
        if premarket:
            from_ts = int(from_date.timestamp() * 1000)
            to_ts = int(to_date.timestamp() * 1000)

        data = await self._request(
            f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_ts}/{to_ts}",
            params=params,
        )

        bars = []
        for result in data.get("results", []):
            bars.append(
                StockBar(
                    ticker=ticker,
                    open=result["o"],
                    high=result["h"],
                    low=result["l"],
                    close=result["c"],
                    volume=result["v"],
                    timestamp=datetime.fromtimestamp(result["t"] / 1000),
                    vwap=result.get("vw"),
                )
            )
        return bars

    async def get_previous_close(self, ticker: str) -> Optional[dict]:
        """Get previous day's close for a ticker."""
        try:
            data = await self._request(f"/v2/aggs/ticker/{ticker}/prev")
            results = data.get("results", [])
            if results:
                return results[0]
        except Exception:
            pass
        return None

    async def get_grouped_daily_bars(self, date: str) -> list[dict]:
        """Get all stocks' daily bars for a specific date."""
        data = await self._request(
            f"/v2/aggs/grouped/locale/us/market/stocks/{date}",
            params={"adjusted": "true", "include_otc": "true"},
        )
        return data.get("results", [])

    async def fetch_recent_bars_batch(
        self, tickers: list[str], minutes: int = 10, premarket: bool = False
    ) -> dict[str, list[StockBar]]:
        """Fetch recent 1-minute bars for multiple tickers."""
        end_time = (datetime.now() - timedelta(minutes=1)).replace(second=0, microsecond=0)
        start_time = end_time - timedelta(minutes=minutes - 1)
        from_date = start_time - timedelta(minutes=5)

        async def fetch_single(ticker: str) -> tuple[str, list[StockBar]]:
            try:
                bars = await self.get_aggregate_bars(
                    ticker, from_date, end_time, premarket=premarket
                )
                window = [b for b in bars if start_time <= b.timestamp <= end_time]
                return ticker, window[-minutes:] if len(window) > minutes else window
            except Exception:
                return ticker, []

        tasks = [fetch_single(t) for t in tickers]
        results = await asyncio.gather(*tasks)
        return dict(results)
