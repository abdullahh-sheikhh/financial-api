"""Engine for calculating top percentage gainers."""
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .polygon_client import PolygonClient


@dataclass
class GainerReport:
    """Report data for a single gainer stock."""

    ticker: str
    name: str
    current_price: float
    volume: int
    gain_10min_percent: float
    gain_day_percent: float
    timestamp: datetime


class GainersEngine:
    """Engine for computing top percentage gainers from market data."""

    def __init__(self, client: PolygonClient, top_n: int = 20, lookback_minutes: int = 10):
        self.client = client
        self.top_n = top_n
        self.lookback_minutes = lookback_minutes
        self._ticker_cache: dict[str, str] = {}

    async def get_top_gainers(self, premarket: bool = False) -> list[GainerReport]:
        """
        Get top N percentage gainers based on 10-minute window.

        This method:
        1. Fetches current market snapshot for all stocks
        2. Gets the top gainers by daily performance
        3. Fetches 10-minute bars to calculate short-term gain
        4. Ranks and returns top N by 10-minute gain
        """
        # Get current market snapshot with gainers
        snapshots = await self.client.get_gainers()

        if not snapshots:
            # Fallback: get all tickers snapshot
            snapshots = await self.client.get_all_tickers_snapshot()

        # Process snapshots into candidates
        candidates = []
        for snap in snapshots[:100]:  # Process top 100 candidates
            ticker = snap.get("ticker", "")
            if not ticker:
                continue

            day_data = snap.get("day", {})
            prev_day = snap.get("prevDay", {})
            min_data = snap.get("min", {})

            current_price = min_data.get("c") or day_data.get("c", 0)
            if current_price <= 0:
                continue

            prev_close = prev_day.get("c", 0)
            day_open = day_data.get("o", prev_close)

            # Calculate daily gain
            if prev_close > 0:
                day_gain = ((current_price - prev_close) / prev_close) * 100
            else:
                day_gain = 0

            volume = day_data.get("v", 0)

            candidates.append(
                {
                    "ticker": ticker,
                    "current_price": current_price,
                    "volume": volume,
                    "day_gain": day_gain,
                    "day_open": day_open,
                }
            )

        # Only fetch bars for top candidates (sorted by daily gain) — we need top_n results
        candidates.sort(key=lambda c: c["day_gain"], reverse=True)
        candidates = candidates[: self.top_n + 10]  # small buffer above top_n

        tickers = [c["ticker"] for c in candidates]

        # Fetch bars and ticker names in parallel (independent of each other)
        bars_future = self.client.fetch_recent_bars_batch(
            tickers, minutes=self.lookback_minutes, premarket=premarket
        )
        names_future = self.client.get_ticker_details_batch(tickers)
        bars_map, name_map = await asyncio.gather(bars_future, names_future)

        # Calculate 10-minute gains
        reports = []
        for candidate in candidates:
            ticker = candidate["ticker"]
            bars = bars_map.get(ticker, [])

            # Calculate 10-minute gain
            gain_10min = 0.0
            if len(bars) >= 2:
                first_price = bars[0].open
                last_price = bars[-1].close
                if first_price > 0:
                    gain_10min = ((last_price - first_price) / first_price) * 100

            reports.append(
                GainerReport(
                    ticker=ticker,
                    name=str(name_map.get(ticker, ticker)),
                    current_price=candidate["current_price"],
                    volume=candidate["volume"],
                    gain_10min_percent=round(gain_10min, 2),
                    gain_day_percent=round(candidate["day_gain"], 2),
                    timestamp=datetime.now(),
                )
            )

        # Sort by 10-minute gain and return top N
        reports.sort(key=lambda r: r.gain_10min_percent, reverse=True)
        return reports[: self.top_n]

    async def get_top_gainers_simple(self) -> list[GainerReport]:
        """
        Simplified method using Polygon's built-in gainers endpoint.

        This is faster but may not have precise 10-minute calculations.
        """
        snapshots = await self.client.get_gainers()

        reports = []
        for snap in snapshots[: self.top_n]:
            ticker = snap.get("ticker", "")
            if not ticker:
                continue

            day_data = snap.get("day", {})
            prev_day = snap.get("prevDay", {})
            min_data = snap.get("min", {})

            current_price = min_data.get("c") or day_data.get("c", 0)
            prev_close = prev_day.get("c", 0)

            if current_price <= 0:
                continue

            # Daily gain
            day_gain = 0
            if prev_close > 0:
                day_gain = ((current_price - prev_close) / prev_close) * 100

            # Use todaysChangePerc if available
            todays_change = snap.get("todaysChangePerc", day_gain)

            name = await self._get_ticker_name(ticker)

            reports.append(
                GainerReport(
                    ticker=ticker,
                    name=name,
                    current_price=current_price,
                    volume=day_data.get("v", 0),
                    gain_10min_percent=round(todays_change, 2),  # Approximation
                    gain_day_percent=round(day_gain, 2),
                    timestamp=datetime.now(),
                )
            )

        return reports

    async def _get_ticker_name(self, ticker: str) -> str:
        """Get company name for a ticker with caching."""
        if ticker in self._ticker_cache:
            return self._ticker_cache[ticker]

        details = await self.client.get_ticker_details(ticker)
        name = details.get("name", ticker)
        self._ticker_cache[ticker] = name
        return name


def format_report(reports: list[GainerReport]) -> str:
    """Format a list of gainer reports as a readable string."""
    if not reports:
        return "No gainers found."

    lines = [
        "=" * 80,
        f"TOP {len(reports)} GAINERS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        f"{'Ticker':<8} {'Name':<25} {'Price':>10} {'Volume':>12} {'10min%':>8} {'Day%':>8}",
        "-" * 80,
    ]

    for r in reports:
        name = r.name[:24] if len(r.name) > 24 else r.name
        lines.append(
            f"{r.ticker:<8} {name:<25} ${r.current_price:>9.2f} {r.volume:>12,} "
            f"{r.gain_10min_percent:>+7.2f}% {r.gain_day_percent:>+7.2f}%"
        )

    lines.append("=" * 80)
    return "\n".join(lines)
