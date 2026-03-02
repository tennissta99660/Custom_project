"""
Yahoo Finance Downloader — Global Market Data via yfinance

Downloads OHLCV data, company info, and financial metrics for any publicly
traded stock worldwide using the yfinance library (no API key required).

Data flow:
  yfinance → fetch history + info → ClickHouse (market_ticks)
                                   → PostgreSQL (companies)
                                   → Neo4j (:Company) nodes

This is the primary free data source for US/global equities, ETFs, indices,
futures, and crypto.
"""

import os
import sys
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── Default Tickers ────────────────────────────────────────────────
DEFAULT_TICKERS = {
    "us_mega": ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "BRK-B"],
    "us_finance": ["JPM", "BAC", "GS", "MS", "V", "MA"],
    "us_energy": ["XOM", "CVX", "COP", "SLB", "OXY"],
    "us_indices": ["^GSPC", "^DJI", "^IXIC", "^VIX"],
    "commodities": ["GC=F", "SI=F", "CL=F", "NG=F", "BZ=F"],  # Gold, Silver, WTI, NatGas, Brent
    "crypto": ["BTC-USD", "ETH-USD"],
    "india": ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS"],
    "etfs": ["SPY", "QQQ", "IWM", "EEM", "GLD", "USO"],
}


class YFinanceDownloader:
    """Downloads market data using the yfinance library."""

    def __init__(self):
        try:
            import yfinance as yf
            self.yf = yf
            logger.info("YFinanceDownloader initialized")
        except ImportError:
            logger.error("yfinance not installed. Run: pip install yfinance")
            self.yf = None

    def fetch_history(
        self,
        ticker: str,
        period: str = "10y",
        interval: str = "1d",
    ) -> list[dict]:
        """
        Fetch historical OHLCV data for a ticker.

        Args:
            ticker: Stock symbol (e.g., "AAPL", "RELIANCE.NS")
            period: Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Candle interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        """
        if not self.yf:
            return []

        try:
            stock = self.yf.Ticker(ticker)
            df = stock.history(period=period, interval=interval)

            if df.empty:
                logger.warning(f"No data returned for {ticker}")
                return []

            records = []
            for idx, row in df.iterrows():
                ts = idx.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                records.append({
                    "symbol": ticker,
                    "timestamp": ts,
                    "open": float(row.get("Open", 0)),
                    "high": float(row.get("High", 0)),
                    "low": float(row.get("Low", 0)),
                    "close": float(row.get("Close", 0)),
                    "volume": float(row.get("Volume", 0)),
                })

            logger.info(f"Fetched {len(records)} candles for {ticker} ({period}/{interval})")
            return records

        except Exception as e:
            logger.error(f"Failed to fetch history for {ticker}: {e}")
            return []

    def fetch_company_info(self, ticker: str) -> Optional[dict]:
        """Fetch company profile information."""
        if not self.yf:
            return None

        try:
            stock = self.yf.Ticker(ticker)
            info = stock.info or {}

            if not info.get("shortName"):
                return None

            return {
                "ticker": ticker,
                "name": info.get("shortName", ""),
                "long_name": info.get("longName", ""),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "country": info.get("country", ""),
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "dividend_yield": info.get("dividendYield"),
                "52w_high": info.get("fiftyTwoWeekHigh"),
                "52w_low": info.get("fiftyTwoWeekLow"),
                "avg_volume": info.get("averageVolume"),
                "website": info.get("website", ""),
                "description": info.get("longBusinessSummary", ""),
            }

        except Exception as e:
            logger.debug(f"Failed to fetch info for {ticker}: {e}")
            return None

    def insert_ticks_to_clickhouse(self, records: list[dict]):
        """Insert OHLCV data into ClickHouse market_ticks table."""
        if not records:
            return

        rows = []
        for r in records:
            rows.append([
                r["symbol"],
                r["close"],
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

    def insert_companies_to_postgres(self, infos: list[dict]):
        """Insert/update company metadata in PostgreSQL."""
        if not infos:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for info in infos:
                if not info:
                    continue
                try:
                    conn.execute(
                        text("""
                            INSERT INTO companies (ticker, name)
                            VALUES (:ticker, :name)
                            ON CONFLICT (ticker) DO UPDATE SET name = :name
                        """),
                        {
                            "ticker": info["ticker"][:10],
                            "name": info.get("name", "")[:255],
                        },
                    )
                    inserted += 1
                except Exception as e:
                    logger.debug(f"Skipped company: {e}")
            conn.commit()

        logger.info(f"PostgreSQL: upserted {inserted} companies")

    def insert_companies_to_neo4j(self, infos: list[dict]):
        """Insert company nodes into Neo4j with industry/sector relationships."""
        if not infos:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for info in infos:
                    if not info:
                        continue
                    try:
                        session.run(
                            """
                            MERGE (c:Company {ticker: $ticker})
                            SET c.name = $name,
                                c.sector = $sector,
                                c.industry = $industry,
                                c.country = $country,
                                c.market_cap = $market_cap
                            """,
                            ticker=info["ticker"],
                            name=info.get("name", ""),
                            sector=info.get("sector", ""),
                            industry=info.get("industry", ""),
                            country=info.get("country", ""),
                            market_cap=info.get("market_cap"),
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
        tickers: list[str] = None,
        categories: list[str] = None,
        period: str = "10y",
        interval: str = "1d",
        fetch_info: bool = True,
    ):
        """
        Full pipeline: fetch market data → ClickHouse + PG + Neo4j.

        Args:
            tickers: Specific tickers to fetch. Overrides categories.
            categories: Categories from DEFAULT_TICKERS to use.
            period: yfinance period string.
            interval: yfinance interval string.
            fetch_info: Whether to also fetch and store company info.
        """
        logger.info("━" * 50)
        logger.info("Yahoo Finance Downloader — Starting")
        logger.info("━" * 50)

        if not self.yf:
            logger.error("yfinance not available — aborting")
            return

        # Build ticker list
        if tickers:
            all_tickers = tickers
        else:
            categories = categories or list(DEFAULT_TICKERS.keys())
            all_tickers = []
            for cat in categories:
                all_tickers.extend(DEFAULT_TICKERS.get(cat, []))

        logger.info(f"Fetching data for {len(all_tickers)} tickers")

        # 1. Fetch historical data
        all_records = []
        for ticker in all_tickers:
            records = self.fetch_history(ticker, period=period, interval=interval)
            all_records.extend(records)

        logger.info(f"Total candles: {len(all_records)}")
        self.insert_ticks_to_clickhouse(all_records)

        # 2. Fetch and store company info
        if fetch_info:
            infos = []
            for ticker in all_tickers:
                # Skip indices, futures, crypto for company info
                if any(c in ticker for c in ["^", "=F", "-USD"]):
                    continue
                info = self.fetch_company_info(ticker)
                if info:
                    infos.append(info)

            self.insert_companies_to_postgres(infos)
            self.insert_companies_to_neo4j(infos)

        logger.info("Yahoo Finance Downloader — Complete ✓")


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = YFinanceDownloader()
    # Pass tickers as CLI args, or use defaults
    tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    downloader.run(tickers=tickers)
