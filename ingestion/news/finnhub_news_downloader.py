"""
Finnhub News Downloader — Financial-specific news with sentiment scores.

Fetches company-specific and market-wide news from Finnhub.io.
Free tier: 60 API calls/minute. Requires API key (free at finnhub.io).

Data flow:
  Finnhub API → parse → PostgreSQL (news_articles + news_ticker_mentions)
                      → ClickHouse (news_sentiment_ts)
                      → Neo4j (:Article)-[:MENTIONS]->(:Company)
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


class FinnhubNewsDownloader:
    """Downloads financial news and sentiment from Finnhub API."""

    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FINNHUB_API_KEY", "")
        if not self.api_key:
            logger.warning("FINNHUB_API_KEY not set — downloads will fail")

        self.client = httpx.Client(
            timeout=30.0,
            headers={"X-Finnhub-Token": self.api_key},
        )
        logger.info("FinnhubNewsDownloader initialized")

    def fetch_company_news(self, ticker: str, days_back: int = 365) -> list[dict]:
        """Fetch news for a specific company ticker."""
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        try:
            resp = self.client.get(
                f"{self.BASE_URL}/company-news",
                params={"symbol": ticker, "from": start, "to": end},
            )
            resp.raise_for_status()
            articles = resp.json()
            logger.info(f"Fetched {len(articles)} articles for {ticker}")
            return articles
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch news for {ticker}: {e}")
            return []

    def fetch_general_news(self, category: str = "general") -> list[dict]:
        """Fetch general market news. Categories: general, forex, crypto, merger."""
        try:
            resp = self.client.get(
                f"{self.BASE_URL}/news",
                params={"category": category},
            )
            resp.raise_for_status()
            articles = resp.json()
            logger.info(f"Fetched {len(articles)} general news articles (category: {category})")
            return articles
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch general news: {e}")
            return []

    def fetch_market_sentiment(self, ticker: str) -> Optional[dict]:
        """Fetch social sentiment for a ticker (Reddit + Twitter)."""
        try:
            resp = self.client.get(
                f"{self.BASE_URL}/stock/social-sentiment",
                params={"symbol": ticker},
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.debug(f"Sentiment fetch failed for {ticker}: {e}")
            return None

    def _normalize_article(self, raw: dict, ticker: str = "") -> dict:
        """Normalize Finnhub article format to our standard schema."""
        # Finnhub timestamps are Unix seconds
        ts = raw.get("datetime", 0)
        published = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None

        return {
            "source_name": "finnhub",
            "source_id": str(raw.get("id", "")),
            "title": raw.get("headline", "")[:1024],
            "summary": raw.get("summary", "")[:4096],
            "content": None,  # Finnhub doesn't provide full content
            "url": raw.get("url", ""),
            "author": raw.get("source", ""),
            "category": raw.get("category", "general"),
            "language": "en",
            "country": "US",
            "image_url": raw.get("image", ""),
            "published_at": published,
            "ticker": ticker or raw.get("related", ""),
            "sentiment": raw.get("sentiment", 0.0),
        }

    def insert_to_postgres(self, articles: list[dict]):
        """Insert articles into news_articles + news_ticker_mentions."""
        if not articles:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for article in articles:
                normalized = self._normalize_article(article) if "headline" in article else article

                try:
                    # Insert article
                    result = conn.execute(
                        text("""
                            INSERT INTO news_articles (
                                source_name, source_id, title, summary, content,
                                url, author, category, language, country,
                                image_url, published_at
                            ) VALUES (
                                :source_name, :source_id, :title, :summary, :content,
                                :url, :author, :category, :language, :country,
                                :image_url, :published_at
                            )
                            ON CONFLICT (url) DO NOTHING
                            RETURNING id
                        """),
                        {
                            "source_name": normalized["source_name"],
                            "source_id": normalized["source_id"],
                            "title": normalized["title"],
                            "summary": normalized["summary"],
                            "content": normalized.get("content"),
                            "url": normalized["url"],
                            "author": normalized["author"],
                            "category": normalized["category"],
                            "language": normalized["language"],
                            "country": normalized["country"],
                            "image_url": normalized.get("image_url"),
                            "published_at": normalized.get("published_at"),
                        },
                    )

                    row = result.fetchone()
                    if row and normalized.get("ticker"):
                        article_id = row[0]
                        # Insert ticker mention
                        conn.execute(
                            text("""
                                INSERT INTO news_ticker_mentions (article_id, ticker, sentiment_score, relevance_score)
                                VALUES (:aid, :ticker, :sentiment, 1.0)
                                ON CONFLICT DO NOTHING
                            """),
                            {
                                "aid": article_id,
                                "ticker": normalized["ticker"][:10],
                                "sentiment": normalized.get("sentiment", 0.0),
                            },
                        )
                        inserted += 1

                except Exception as e:
                    logger.debug(f"Skipped article: {e}")

            conn.commit()

        logger.info(f"PostgreSQL: inserted {inserted} articles")

    def insert_sentiment_to_clickhouse(self, articles: list[dict]):
        """Insert sentiment time-series into ClickHouse."""
        if not articles:
            return

        rows = []
        for article in articles:
            normalized = self._normalize_article(article) if "headline" in article else article
            ticker = normalized.get("ticker", "")
            sentiment = normalized.get("sentiment", 0.0)
            published = normalized.get("published_at")

            if ticker and published:
                rows.append([
                    ticker,
                    "finnhub",
                    float(sentiment) if sentiment else 0.0,
                    0.0,  # tone (not available from Finnhub directly)
                    published,
                ])

        if not rows:
            return

        try:
            ch = Database.clickhouse()
            ch.insert(
                "news_sentiment_ts",
                rows,
                column_names=["ticker", "source", "sentiment", "tone", "timestamp"],
            )
            ch.close()
            logger.info(f"ClickHouse: inserted {len(rows)} sentiment rows")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

    def run(self, tickers: list[str] = None, categories: list[str] = None):
        """Full pipeline: fetch news for tickers + categories → insert everywhere."""
        logger.info("━" * 50)
        logger.info("Finnhub News Downloader — Starting")
        logger.info("━" * 50)

        if not self.api_key:
            logger.error("No API key — aborting. Get one free at https://finnhub.io")
            return

        tickers = tickers or ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
        categories = categories or ["general", "forex", "crypto", "merger"]

        all_articles = []

        # Fetch company-specific news
        for ticker in tickers:
            articles = self.fetch_company_news(ticker)
            for a in articles:
                a["_ticker"] = ticker  # preserve association
            all_articles.extend(articles)

        # Fetch general market news
        for cat in categories:
            articles = self.fetch_general_news(cat)
            all_articles.extend(articles)

        logger.info(f"Total articles fetched: {len(all_articles)}")

        self.insert_to_postgres(all_articles)
        self.insert_sentiment_to_clickhouse(all_articles)
        self.insert_to_neo4j(all_articles)

        logger.info("Finnhub News Downloader — Complete ✓")

    def insert_to_neo4j(self, articles: list[dict]):
        """Insert articles into Neo4j as (:Article)-[:MENTIONS]->(:Company) graph."""
        if not articles:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for article in articles:
                    normalized = self._normalize_article(article) if "headline" in article else article
                    url = normalized.get("url", "")
                    if not url:
                        continue

                    ticker = normalized.get("ticker", "")
                    source_name = normalized.get("author", "finnhub")
                    published = normalized.get("published_at")
                    pub_str = published.isoformat() if published else None

                    try:
                        # Merge Article + Source nodes
                        session.run(
                            """
                            MERGE (a:Article {url: $url})
                            SET a.title = $title,
                                a.summary = $summary,
                                a.source_name = 'finnhub',
                                a.category = $category,
                                a.published_at = $published_at
                            MERGE (src:Source {name: $source_name})
                            MERGE (a)-[:FROM]->(src)
                            """,
                            url=url,
                            title=normalized.get("title", ""),
                            summary=normalized.get("summary", ""),
                            category=normalized.get("category", "general"),
                            published_at=pub_str,
                            source_name=source_name,
                        )

                        # Link to company ticker if available
                        if ticker:
                            session.run(
                                """
                                MERGE (a:Article {url: $url})
                                MERGE (c:Company {ticker: $ticker})
                                MERGE (a)-[:MENTIONS {sentiment: $sentiment}]->(c)
                                """,
                                url=url,
                                ticker=ticker[:10],
                                sentiment=float(normalized.get("sentiment", 0.0)),
                            )

                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j skip: {e}")

            logger.info(f"Neo4j: created {created} article nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = FinnhubNewsDownloader()
    try:
        # Custom tickers can be passed as CLI args
        tickers = sys.argv[1:] if len(sys.argv) > 1 else None
        downloader.run(tickers=tickers)
    finally:
        downloader.close()
