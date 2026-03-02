"""
Live Market Data — Continuous real-time market data ingestion

Polls yfinance every 60s for latest quotes and Angel One WebSocket
for real-time NSE/BSE ticks. Runs continuously as a background process.

Usage:
    python ingestion/market/live_market.py
"""

import os
import sys
import time
from datetime import datetime, timezone

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database

# ── Tickers to monitor ───────────────────────────────────────────
YFINANCE_TICKERS = [
    # US Mega-caps
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    # Indices
    "^GSPC", "^DJI", "^IXIC", "^VIX",
    # Commodities
    "GC=F", "CL=F", "SI=F", "NG=F",
    # Crypto
    "BTC-USD", "ETH-USD",
    # Forex
    "USDINR=X", "EURUSD=X",
]

ANGEL_ONE_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
    "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "LT",
    "NIFTY", "BANKNIFTY",
]

POLL_INTERVAL = 60  # seconds


def poll_yfinance():
    """Fetch latest quotes from yfinance and insert into ClickHouse."""
    try:
        import yfinance as yf
        import clickhouse_connect

        data = yf.download(YFINANCE_TICKERS, period="1d", interval="1m", progress=False)
        if data.empty:
            logger.debug("yfinance: no new data")
            return 0

        ch = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )

        rows = []
        for ticker in YFINANCE_TICKERS:
            try:
                if ticker in data["Close"].columns:
                    latest = data["Close"][ticker].dropna()
                    if not latest.empty:
                        price = float(latest.iloc[-1])
                        vol = float(data["Volume"][ticker].dropna().iloc[-1]) if ticker in data["Volume"].columns else 0
                        rows.append([
                            ticker, "equity", datetime.now(timezone.utc),
                            price, price, price, price, vol, "yfinance_live",
                        ])
            except Exception:
                continue

        if rows:
            ch.insert("atlas.market_ticks", rows, column_names=[
                "symbol", "asset_class", "timestamp",
                "open", "high", "low", "close", "volume", "source",
            ])
            logger.info(f"yfinance: inserted {len(rows)} live quotes")

        return len(rows)

    except Exception as e:
        logger.error(f"yfinance poll failed: {e}")
        return 0


def poll_angel_one():
    """Fetch latest quotes from Angel One and insert into ClickHouse."""
    try:
        from SmartApi import SmartConnect
        import pyotp

        api_key = os.getenv("ANGEL_API_KEY", "")
        client_id = os.getenv("ANGEL_CLIENT_ID", "")
        password = os.getenv("ANGEL_PASSWORD", "")
        totp_secret = os.getenv("ANGEL_TOTP_SECRET", "")

        if not all([api_key, client_id, password, totp_secret]):
            return 0

        obj = SmartConnect(api_key=api_key)
        totp = pyotp.TOTP(totp_secret).now()
        session = obj.generateSession(client_id, password, totp)

        if not session or not session.get("data"):
            return 0

        token_map = {
            "RELIANCE": "2885", "TCS": "11536", "HDFCBANK": "1333",
            "INFY": "1594", "ICICIBANK": "4963", "HINDUNILVR": "1394",
            "ITC": "1660", "SBIN": "3045", "BHARTIARTL": "10604",
            "LT": "11483", "NIFTY": "99926000", "BANKNIFTY": "99926009",
        }

        exchange_data = []
        for symbol, token in token_map.items():
            exchange = "NSE" if token not in ["99926000", "99926009"] else "NSE"
            exchange_data.append({"exchange": exchange, "tradingsymbol": symbol, "symboltoken": token})

        quote = obj.getMarketData("FULL", exchange_data)
        if not quote or not quote.get("data", {}).get("fetched"):
            obj.terminateSession(client_id)
            return 0

        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )

        rows = []
        for item in quote["data"]["fetched"]:
            rows.append([
                item.get("tradingSymbol", ""), "equity_in",
                datetime.now(timezone.utc),
                float(item.get("open", 0)), float(item.get("high", 0)),
                float(item.get("low", 0)), float(item.get("ltp", 0)),
                float(item.get("tradeVolume", 0)), "angel_one_live",
            ])

        if rows:
            ch.insert("atlas.market_ticks", rows, column_names=[
                "symbol", "asset_class", "timestamp",
                "open", "high", "low", "close", "volume", "source",
            ])

        obj.terminateSession(client_id)
        logger.info(f"Angel One: inserted {len(rows)} live quotes")
        return len(rows)

    except Exception as e:
        logger.error(f"Angel One poll failed: {e}")
        return 0


def main():
    logger.info("━" * 50)
    logger.info("Live Market Data — Starting")
    logger.info(f"Polling every {POLL_INTERVAL}s")
    logger.info(f"Tickers: {len(YFINANCE_TICKERS)} yfinance + {len(ANGEL_ONE_SYMBOLS)} Angel One")
    logger.info("━" * 50)

    cycle = 0
    while True:
        try:
            cycle += 1
            logger.info(f"\n── Cycle {cycle} @ {datetime.now().strftime('%H:%M:%S')} ──")

            yf_count = poll_yfinance()
            angel_count = poll_angel_one()

            logger.info(f"Inserted: {yf_count} yfinance + {angel_count} Angel One")
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Live Market Data — Stopped by user")
            break
        except Exception as e:
            logger.error(f"Cycle {cycle} error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
