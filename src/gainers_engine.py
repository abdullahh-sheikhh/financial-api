"""Engine for calculating top percentage gainers."""
import asyncio
from dataclasses import dataclass
from datetime import datetime
from .polygon_client import PolygonClient


@dataclass
class GainerReport:
    """Report data for a single gainer stock."""

    ticker: str
    name: str
    market_price: float
    avg_price: float
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

    async def get_top_gainers(self) -> list[GainerReport]:
        """
        Get top N percentage gainers based on 10-minute window.

        This method:
        1. Fetches current market snapshot for all stocks
        2. Gets the top gainers by daily performance
        3. Fetches 10-minute bars to calculate short-term gain
        4. Ranks and returns top N by 10-minute gain
        """
        snapshots = await self.client.get_gainers()

        if not snapshots:
            snapshots = await self.client.get_all_tickers_snapshot()

        candidates = []
        for snap in snapshots[:100]:
            ticker = snap.get("ticker", "")
            if not ticker:
                continue

            last_trade = snap.get("lastTrade", {})
            day_data = snap.get("day", {})
            prev_day = snap.get("prevDay", {})

            market_price = last_trade.get("p", 0)
            if market_price <= 0:
                continue

            prev_close = prev_day.get("c", 0)

            if prev_close > 0:
                day_gain = ((market_price - prev_close) / prev_close) * 100
            else:
                day_gain = 0

            volume = day_data.get("v", 0)

            candidates.append(
                {
                    "ticker": ticker,
                    "market_price": market_price,
                    "volume": volume,
                    "day_gain": day_gain,
                }
            )

        candidates.sort(key=lambda c: c["day_gain"], reverse=True)
        candidates = candidates[: self.top_n + 10]

        tickers = [c["ticker"] for c in candidates]

        bars_future = self.client.fetch_recent_bars_batch(
            tickers, minutes=self.lookback_minutes
        )
        names_future = self.client.get_ticker_details_batch(tickers)
        bars_map, name_map = await asyncio.gather(bars_future, names_future)

        reports = []
        for candidate in candidates:
            ticker = candidate["ticker"]
            current_price = candidate["market_price"]

            bars = bars_map.get(ticker, [])
            avg_price = current_price
            if len(bars) >= 2:
                avg_price = sum(b.close for b in bars) / len(bars)

            gain_10min = 0.0
            if len(bars) >= 1:
                base_price = bars[0].close
                if base_price > 0:
                    gain_10min = ((current_price - base_price) / base_price) * 100

            reports.append(
                GainerReport(
                    ticker=ticker,
                    name=str(name_map.get(ticker, ticker)),
                    market_price=current_price,
                    avg_price=round(avg_price, 4),
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

            last_trade = snap.get("lastTrade", {})
            day_data = snap.get("day", {})
            prev_day = snap.get("prevDay", {})

            market_price = last_trade.get("p", 0)
            prev_close = prev_day.get("c", 0)

            if market_price <= 0:
                continue

            day_gain = 0
            if prev_close > 0:
                day_gain = ((market_price - prev_close) / prev_close) * 100

            todays_change = snap.get("todaysChangePerc", day_gain)

            name = await self._get_ticker_name(ticker)

            reports.append(
                GainerReport(
                    ticker=ticker,
                    name=name,
                    market_price=market_price,
                    avg_price=market_price,
                    volume=day_data.get("v", 0),
                    gain_10min_percent=round(todays_change, 2),
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
        "=" * 100,
        f"TOP {len(reports)} GAINERS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 100,
        f"{'Ticker':<8} {'Name':<25} {'Current$':>10} {'AvgPrice':>10} {'Volume':>12} {'10min%':>8} {'Day%':>8}",
        "-" * 100,
    ]

    for r in reports:
        name = r.name[:24] if len(r.name) > 24 else r.name
        lines.append(
            f"{r.ticker:<8} {name:<25} ${r.market_price:>9.4f} ${r.avg_price:>9.4f} {r.volume:>12,} "
            f"{r.gain_10min_percent:>+7.2f}% {r.gain_day_percent:>+7.2f}%"
        )

    lines.append("=" * 100)
    return "\n".join(lines)
