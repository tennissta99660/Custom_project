"""
Angel One SmartAPI Downloader — Real-time Indian Market Data (NSE/BSE)

Downloads real-time quotes, historical OHLCV candles, and market feed data
from NSE/BSE using Angel One's SmartAPI. Completely free with a demat account.

Data flow:
  Angel One SmartAPI → fetch quotes/history → ClickHouse (market_ticks)
                                             → PostgreSQL (companies)
                                             → Neo4j (:Company) nodes

Requires in .env:
  ANGEL_API_KEY      — from smartapi.angelbroking.com
  ANGEL_CLIENT_ID    — your demat client ID (e.g., xxxxx)
  ANGEL_PASSWORD     — your trading password / MPIN
  ANGEL_TOTP_SECRET  — from the TOTP setup in Angel One app

Install: pip install smartapi-python pyotp
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── Default NSE/BSE Symbols (Angel token format) ──────────────────
# Angel One uses exchange:symbol:token format
# Token list: https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json
DEFAULT_SYMBOLS = [
    {"symbol": "RELIANCE",   "token": "2885",  "exchange": "NSE"},
    {"symbol": "TCS",        "token": "11536", "exchange": "NSE"},
    {"symbol": "HDFCBANK",   "token": "1333",  "exchange": "NSE"},
    {"symbol": "INFY",       "token": "1594",  "exchange": "NSE"},
    {"symbol": "ICICIBANK",  "token": "4963",  "exchange": "NSE"},
    {"symbol": "HINDUNILVR", "token": "1394",  "exchange": "NSE"},
    {"symbol": "ITC",        "token": "1660",  "exchange": "NSE"},
    {"symbol": "SBIN",       "token": "3045",  "exchange": "NSE"},
    {"symbol": "BHARTIARTL", "token": "10604", "exchange": "NSE"},
    {"symbol": "LT",         "token": "11483", "exchange": "NSE"},
    {"symbol": "NIFTY",      "token": "99926000", "exchange": "NSE"},
    {"symbol": "BANKNIFTY",  "token": "99926009", "exchange": "NSE"},
]

# Candle interval mapping for SmartAPI
INTERVALS = {
    "1min":  "ONE_MINUTE",
    "3min":  "THREE_MINUTE",
    "5min":  "FIVE_MINUTE",
    "10min": "TEN_MINUTE",
    "15min": "FIFTEEN_MINUTE",
    "30min": "THIRTY_MINUTE",
    "1hour": "ONE_HOUR",
    "1day":  "ONE_DAY",
}


class AngelOneDownloader:
    """Downloads market data from Angel One SmartAPI for Indian equities."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        client_id: Optional[str] = None,
        password: Optional[str] = None,
        totp_secret: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("ANGEL_API_KEY", "")
        self.client_id = client_id or os.getenv("ANGEL_CLIENT_ID", "")
        self.password = password or os.getenv("ANGEL_PASSWORD", "")
        self.totp_secret = totp_secret or os.getenv("ANGEL_TOTP_SECRET", "")
        self.smart_api = None
        self._authenticated = False

        if not all([self.api_key, self.client_id, self.password]):
            logger.warning("Angel One credentials not fully set — downloads will fail")

        logger.info("AngelOneDownloader initialized")

    def authenticate(self) -> bool:
        """Authenticate with Angel One SmartAPI using TOTP."""
        try:
            from SmartApi import SmartConnect
            import pyotp
        except ImportError:
            logger.error("Required packages not installed. Run: pip install smartapi-python pyotp")
            return False

        try:
            self.smart_api = SmartConnect(api_key=self.api_key)

            # Generate TOTP
            totp = ""
            if self.totp_secret:
                totp = pyotp.TOTP(self.totp_secret).now()

            data = self.smart_api.generateSession(
                clientCode=self.client_id,
                password=self.password,
                totp=totp,
            )

            if data.get("status"):
                self._authenticated = True
                auth_token = data["data"]["jwtToken"]
                self.smart_api.getProfile(auth_token)
                logger.info(f"Authenticated as {self.client_id}")
                return True
            else:
                logger.error(f"Authentication failed: {data.get('message', 'Unknown error')}")
                return False

        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False

    def fetch_quote(self, symbols: list[dict] = None) -> list[dict]:
        """
        Fetch real-time LTP (Last Traded Price) for a list of symbols.

        Args:
            symbols: List of dicts with {symbol, token, exchange}
        """
        if not self._authenticated:
            logger.error("Not authenticated — call authenticate() first")
            return []

        symbols = symbols or DEFAULT_SYMBOLS

        try:
            # Build exchange token list
            exchange_tokens = {}
            for s in symbols:
                exchange = s["exchange"]
                if exchange not in exchange_tokens:
                    exchange_tokens[exchange] = []
                exchange_tokens[exchange].append(s["token"])

            data = self.smart_api.getMarketData(
                mode="FULL",
                exchangeTokens=exchange_tokens,
            )

            if data.get("status"):
                fetched = data.get("data", {}).get("fetched", [])
                quotes = []
                for item in fetched:
                    # Find the symbol name for this token
                    token = str(item.get("symbolToken", ""))
                    sym_name = token  # fallback
                    for s in symbols:
                        if s["token"] == token:
                            sym_name = s["symbol"]
                            break

                    quotes.append({
                        "symbol": f"NSE:{sym_name}",
                        "ltp": float(item.get("ltp", 0)),
                        "open": float(item.get("open", 0)),
                        "high": float(item.get("high", 0)),
                        "low": float(item.get("low", 0)),
                        "close": float(item.get("close", 0)),
                        "volume": int(item.get("tradeVolume", 0)),
                        "timestamp": datetime.now(timezone.utc),
                    })

                logger.info(f"Fetched {len(quotes)} real-time quotes")
                return quotes
            else:
                logger.error(f"Quote fetch failed: {data.get('message', '')}")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch quotes: {e}")
            return []

    def fetch_historical(
        self,
        symbol_info: dict,
        interval: str = "1day",
        days_back: int = 3653,
    ) -> list[dict]:
        """
        Fetch historical OHLCV candles for a symbol.

        Args:
            symbol_info: Dict with {symbol, token, exchange}
            interval: Candle interval (1min, 5min, 15min, 1hour, 1day)
            days_back: Number of days of history
        """
        if not self._authenticated:
            logger.error("Not authenticated — call authenticate() first")
            return []

        end = datetime.now()
        start = end - timedelta(days=days_back)

        interval_key = INTERVALS.get(interval, "ONE_DAY")

        try:
            params = {
                "exchange": symbol_info["exchange"],
                "symboltoken": symbol_info["token"],
                "interval": interval_key,
                "fromdate": start.strftime("%Y-%m-%d %H:%M"),
                "todate": end.strftime("%Y-%m-%d %H:%M"),
            }

            data = self.smart_api.getCandleData(params)

            if data.get("status") and data.get("data"):
                candles = data["data"]
                records = []
                for candle in candles:
                    # Format: [timestamp, open, high, low, close, volume]
                    if len(candle) >= 6:
                        try:
                            ts = datetime.strptime(candle[0], "%Y-%m-%dT%H:%M:%S%z")
                        except (ValueError, TypeError):
                            ts = datetime.now(timezone.utc)

                        records.append({
                            "symbol": f"NSE:{symbol_info['symbol']}",
                            "timestamp": ts,
                            "open": float(candle[1]),
                            "high": float(candle[2]),
                            "low": float(candle[3]),
                            "close": float(candle[4]),
                            "volume": float(candle[5]),
                        })

                logger.info(f"Fetched {len(records)} candles for {symbol_info['symbol']} ({interval})")
                return records
            else:
                logger.warning(f"No data for {symbol_info['symbol']}: {data.get('message', '')}")
                return []

        except Exception as e:
            logger.error(f"Failed to fetch history for {symbol_info['symbol']}: {e}")
            return []

    def insert_ticks_to_clickhouse(self, records: list[dict]):
        """Insert tick/candle data into ClickHouse market_ticks table."""
        if not records:
            return

        rows = []
        for r in records:
            rows.append([
                r["symbol"],
                r.get("close", r.get("ltp", 0)),
                r.get("volume", 0),
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

    def insert_companies_to_postgres(self, symbols: list[dict]):
        """Insert/update Indian company tickers in PostgreSQL."""
        if not symbols:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for s in symbols:
                # Skip indices
                if s["symbol"] in ("NIFTY", "BANKNIFTY"):
                    continue
                try:
                    conn.execute(
                        text("""
                            INSERT INTO companies (ticker, name)
                            VALUES (:ticker, :name)
                            ON CONFLICT (ticker) DO NOTHING
                        """),
                        {
                            "ticker": s["symbol"][:10],
                            "name": s["symbol"],
                        },
                    )
                    inserted += 1
                except Exception as e:
                    logger.debug(f"Skipped company: {e}")
            conn.commit()

        logger.info(f"PostgreSQL: upserted {inserted} companies")

    def insert_companies_to_neo4j(self, symbols: list[dict]):
        """Insert Indian company nodes into Neo4j."""
        if not symbols:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for s in symbols:
                    if s["symbol"] in ("NIFTY", "BANKNIFTY"):
                        continue
                    try:
                        session.run(
                            """
                            MERGE (c:Company {ticker: $ticker})
                            SET c.name = $name,
                                c.exchange = $exchange,
                                c.country = 'IN'
                            """,
                            ticker=s["symbol"],
                            name=s["symbol"],
                            exchange=s["exchange"],
                        )
                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j skip: {e}")

            logger.info(f"Neo4j: created/updated {created} company nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def run(
        self,
        symbols: list[dict] = None,
        interval: str = "1day",
        days_back: int = 3653,
        fetch_realtime: bool = True,
    ):
        """
        Full pipeline: authenticate → fetch data → store in ClickHouse + PG + Neo4j.

        Args:
            symbols: List of symbol dicts. Defaults to DEFAULT_SYMBOLS.
            interval: Candle interval for historical data.
            days_back: Days of history to fetch.
            fetch_realtime: Also fetch current real-time quotes.
        """
        logger.info("━" * 50)
        logger.info("Angel One SmartAPI Downloader — Starting")
        logger.info("━" * 50)

        if not self.api_key or not self.client_id:
            logger.error("No credentials — aborting. Set ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PASSWORD in .env")
            return

        # Authenticate
        if not self.authenticate():
            logger.error("Authentication failed — aborting")
            return

        symbols = symbols or DEFAULT_SYMBOLS

        # 1. Fetch historical data
        all_records = []
        for sym in symbols:
            records = self.fetch_historical(sym, interval=interval, days_back=days_back)
            all_records.extend(records)

        logger.info(f"Total historical candles: {len(all_records)}")
        self.insert_ticks_to_clickhouse(all_records)

        # 2. Fetch real-time quotes
        if fetch_realtime:
            quotes = self.fetch_quote(symbols)
            self.insert_ticks_to_clickhouse(quotes)

        # 3. Store company metadata
        self.insert_companies_to_postgres(symbols)
        self.insert_companies_to_neo4j(symbols)

        # Logout
        try:
            self.smart_api.terminateSession(self.client_id)
            logger.info("Session terminated")
        except Exception:
            pass

        logger.info("Angel One SmartAPI Downloader — Complete ✓")


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = AngelOneDownloader()
    downloader.run()
