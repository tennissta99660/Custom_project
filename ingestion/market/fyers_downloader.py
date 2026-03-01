"""
Fyers Downloader — Indian Market Data via Fyers API

Downloads real-time and historical OHLCV data for Indian equities (NSE/BSE)
using the Fyers API v3.

Data flow:
  Fyers API → fetch quotes/history → ClickHouse (market_ticks + OHLCV)
                                    → PostgreSQL (company metadata)

Requires: FYERS_APP_ID, FYERS_SECRET, FYERS_ACCESS_TOKEN in .env
The access token must be generated via the Fyers OAuth2 flow.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── Default Indian Market Symbols ──────────────────────────────────
DEFAULT_SYMBOLS = [
    "NSE:RELIANCE-EQ",
    "NSE:TCS-EQ",
    "NSE:HDFCBANK-EQ",
    "NSE:INFY-EQ",
    "NSE:ICICIBANK-EQ",
    "NSE:HINDUNILVR-EQ",
    "NSE:ITC-EQ",
    "NSE:SBIN-EQ",
    "NSE:BHARTIARTL-EQ",
    "NSE:LT-EQ",
    "NSE:NIFTY50-INDEX",
    "NSE:NIFTYBANK-INDEX",
]

# Resolution codes for Fyers historical data
RESOLUTIONS = {
    "1min": "1",
    "5min": "5",
    "15min": "15",
    "30min": "30",
    "1hour": "60",
    "1day": "D",
    "1week": "W",
    "1month": "M",
}


class FyersDownloader:
    """Downloads market data from Fyers API for Indian equities."""

    BASE_URL = "https://api-t1.fyers.in/api/v3"
    DATA_URL = "https://api-t1.fyers.in/data"

    def __init__(
        self,
        app_id: Optional[str] = None,
        access_token: Optional[str] = None,
    ):
        self.app_id = app_id or os.getenv("FYERS_APP_ID", "")
        self.access_token = access_token or os.getenv("FYERS_ACCESS_TOKEN", "")

        if not self.app_id or not self.access_token:
            logger.warning("FYERS_APP_ID or FYERS_ACCESS_TOKEN not set — downloads will fail")

        self.auth_header = f"{self.app_id}:{self.access_token}"
        self.client = httpx.Client(
            timeout=30.0,
            headers={"Authorization": self.auth_header},
        )
        logger.info("FyersDownloader initialized")

    def fetch_quotes(self, symbols: list[str]) -> list[dict]:
        """
        Fetch real-time quotes for a list of symbols.

        Args:
            symbols: List of Fyers symbol strings (e.g., ["NSE:RELIANCE-EQ"])
        """
        try:
            resp = self.client.get(
                f"{self.DATA_URL}/quotes",
                params={"symbols": ",".join(symbols)},
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("s") != "ok":
                logger.error(f"Fyers quotes error: {data.get('message', 'Unknown')}")
                return []

            quotes = data.get("d", [])
            logger.info(f"Fetched {len(quotes)} quotes")
            return quotes

        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch quotes: {e}")
            return []

    def fetch_historical(
        self,
        symbol: str,
        resolution: str = "1day",
        days_back: int = 30,
    ) -> list[dict]:
        """
        Fetch historical OHLCV candles for a symbol.

        Args:
            symbol: Fyers symbol string
            resolution: Candle resolution (1min, 5min, 1day, etc.)
            days_back: Number of days of history to fetch
        """
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)

        res_code = RESOLUTIONS.get(resolution, "D")

        try:
            resp = self.client.get(
                f"{self.DATA_URL}/history",
                params={
                    "symbol": symbol,
                    "resolution": res_code,
                    "date_format": "1",
                    "range_from": str(int(start.timestamp())),
                    "range_to": str(int(end.timestamp())),
                    "cont_flag": "1",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("s") != "ok":
                logger.error(f"Fyers history error for {symbol}: {data.get('message', 'Unknown')}")
                return []

            candles = data.get("candles", [])
            # Candles format: [timestamp, open, high, low, close, volume]
            records = []
            for candle in candles:
                if len(candle) >= 6:
                    records.append({
                        "symbol": symbol,
                        "timestamp": datetime.fromtimestamp(candle[0], tz=timezone.utc),
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5]),
                    })

            logger.info(f"Fetched {len(records)} candles for {symbol} ({resolution})")
            return records

        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch history for {symbol}: {e}")
            return []

    def insert_ticks_to_clickhouse(self, records: list[dict]):
        """Insert tick/candle data into ClickHouse market_ticks table."""
        if not records:
            return

        rows = []
        for r in records:
            rows.append([
                r["symbol"],
                r["close"],  # Use close price as the tick price
                r["volume"],
                r["timestamp"],
            ])

        try:
            ch = Database.clickhouse()
            ch.insert(
                "market_ticks",
                rows,
                column_names=["symbol", "price", "volume", "timestamp"],
            )
            ch.close()
            logger.info(f"ClickHouse: inserted {len(rows)} market ticks")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

    def run(self, symbols: list[str] = None, resolution: str = "1day", days_back: int = 30):
        """
        Full pipeline: fetch historical data for symbols → insert to ClickHouse.

        Args:
            symbols: List of Fyers symbols. Defaults to DEFAULT_SYMBOLS.
            resolution: Candle resolution.
            days_back: Days of history.
        """
        logger.info("━" * 50)
        logger.info("Fyers Downloader — Starting")
        logger.info("━" * 50)

        if not self.app_id or not self.access_token:
            logger.error("No credentials — aborting. Set FYERS_APP_ID and FYERS_ACCESS_TOKEN in .env")
            return

        symbols = symbols or DEFAULT_SYMBOLS

        all_records = []
        for symbol in symbols:
            records = self.fetch_historical(symbol, resolution=resolution, days_back=days_back)
            all_records.extend(records)

        logger.info(f"Total candles fetched: {len(all_records)}")
        self.insert_ticks_to_clickhouse(all_records)

        logger.info("Fyers Downloader — Complete ✓")

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = FyersDownloader()
    try:
        # Pass symbols as CLI args, or use defaults
        symbols = sys.argv[1:] if len(sys.argv) > 1 else None
        downloader.run(symbols=symbols)
    finally:
        downloader.close()
