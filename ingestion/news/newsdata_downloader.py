"""
NewsData.io Downloader — Breaking news from 88,000+ sources in 80+ languages.

Fetches global breaking news with country/category/language filtering.
Free tier: 200 credits/day. Requires API key (free at newsdata.io).

Data flow:
  NewsData.io API → parse → PostgreSQL (news_articles)
                          → ClickHouse (news_sentiment_ts)

Categories: business, crime, domestic, education, entertainment,
            environment, food, health, lifestyle, other, politics,
            science, sports, technology, top, tourism, world
"""

import os
import sys
from datetime import datetime, timezone
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


class NewsDataDownloader:
    """Downloads breaking news from NewsData.io API."""

    BASE_URL = "https://newsdata.io/api/1"

    # Countries relevant to financial/commodity analysis
    DEFAULT_COUNTRIES = "us,gb,in,cn,de,jp,sg,au,br,sa"

    # Categories most relevant to the Atlas platform
    DEFAULT_CATEGORIES = "business,politics,world,environment,technology,science"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("NEWSDATA_API_KEY", "")
        if not self.api_key:
            logger.warning("NEWSDATA_API_KEY not set — downloads will fail")

        self.client = httpx.Client(timeout=30.0)
        logger.info("NewsDataDownloader initialized")

    def fetch_latest(
        self,
        country: str = None,
        category: str = None,
        language: str = "en",
        query: str = None,
        size: int = 50,
    ) -> list[dict]:
        """
        Fetch latest news from NewsData.io.

        Args:
            country:  Comma-separated country codes (e.g., "us,gb,in")
            category: Comma-separated categories (e.g., "business,politics")
            language: Language code (e.g., "en")
            query:    Keyword search query
            size:     Number of results (max 50 per request on free tier)
        """
        params = {
            "apikey": self.api_key,
            "language": language,
            "size": min(size, 50),
        }
        if country:
            params["country"] = country
        if category:
            params["category"] = category
        if query:
            params["q"] = query

        try:
            resp = self.client.get(f"{self.BASE_URL}/latest", params=params)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                logger.error(f"NewsData.io error: {data.get('results', {}).get('message', 'Unknown')}")
                return []

            articles = data.get("results", [])
            logger.info(f"Fetched {len(articles)} articles (country={country}, category={category})")
            return articles

        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch from NewsData.io: {e}")
            return []

    def fetch_by_topic(self, topic: str) -> list[dict]:
        """Fetch news for a specific topic/keyword across all countries."""
        return self.fetch_latest(
            country=self.DEFAULT_COUNTRIES,
            query=topic,
        )

    def _normalize_article(self, raw: dict) -> dict:
        """Normalize NewsData.io response to our standard schema."""
        # Parse date
        pub_date = raw.get("pubDate")
        published = None
        if pub_date:
            try:
                published = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                try:
                    from email.utils import parsedate_to_datetime
                    published = parsedate_to_datetime(pub_date).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    pass

        # Extract first country if multiple
        countries = raw.get("country", [])
        country = countries[0] if isinstance(countries, list) and countries else ""

        # Extract first category
        categories = raw.get("category", [])
        category = categories[0] if isinstance(categories, list) and categories else "general"

        # Sentiment (if available in the response)
        sentiment = raw.get("sentiment")
        sentiment_score = 0.0
        if sentiment:
            if sentiment == "positive":
                sentiment_score = 0.7
            elif sentiment == "negative":
                sentiment_score = -0.7
            elif sentiment == "neutral":
                sentiment_score = 0.0

        return {
            "source_name": "newsdata",
            "source_id": raw.get("article_id", ""),
            "title": (raw.get("title") or "")[:1024],
            "summary": (raw.get("description") or "")[:4096],
            "content": raw.get("content"),
            "url": raw.get("link", ""),
            "author": ", ".join(raw.get("creator", []) or []),
            "category": category,
            "language": raw.get("language", "en"),
            "country": country.upper() if country else "",
            "image_url": raw.get("image_url", ""),
            "published_at": published,
            "keywords": raw.get("keywords", []),
            "sentiment": sentiment_score,
        }

    def insert_to_postgres(self, articles: list[dict]):
        """Insert normalized articles into news_articles table."""
        if not articles:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for raw_article in articles:
                article = self._normalize_article(raw_article)

                if not article["url"]:
                    continue

                try:
                    conn.execute(
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
                        """),
                        {
                            "source_name": article["source_name"],
                            "source_id": article["source_id"],
                            "title": article["title"],
                            "summary": article["summary"],
                            "content": article["content"],
                            "url": article["url"],
                            "author": article["author"],
                            "category": article["category"],
                            "language": article["language"],
                            "country": article["country"],
                            "image_url": article.get("image_url"),
                            "published_at": article.get("published_at"),
                        },
                    )
                    inserted += 1
                except Exception as e:
                    logger.debug(f"Skipped article: {e}")

            conn.commit()

        logger.info(f"PostgreSQL: inserted {inserted} articles from NewsData.io")

    def run(
        self,
        countries: str = None,
        categories: str = None,
        topics: list[str] = None,
    ):
        """
        Full pipeline: fetch latest + topic-specific news → insert to PostgreSQL.

        Args:
            countries:  Override default countries (comma-separated)
            categories: Override default categories (comma-separated)
            topics:     Additional keyword topics to search for
        """
        logger.info("━" * 50)
        logger.info("NewsData.io Downloader — Starting")
        logger.info("━" * 50)

        if not self.api_key:
            logger.error("No API key — aborting. Get one free at https://newsdata.io")
            return

        all_articles = []

        # 1. Fetch by country + category (main pull)
        main_articles = self.fetch_latest(
            country=countries or self.DEFAULT_COUNTRIES,
            category=categories or self.DEFAULT_CATEGORIES,
        )
        all_articles.extend(main_articles)

        # 2. Fetch by specific topics (if provided)
        topics = topics or [
            "oil prices",
            "interest rate",
            "supply chain",
            "trade war",
            "commodity",
        ]
        for topic in topics:
            topic_articles = self.fetch_by_topic(topic)
            all_articles.extend(topic_articles)

        # Deduplicate by URL
        seen_urls = set()
        unique = []
        for a in all_articles:
            url = a.get("link", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique.append(a)

        logger.info(f"Total unique articles: {len(unique)}")
        self.insert_to_postgres(unique)
        self.insert_sentiment_to_clickhouse(unique)
        self.insert_to_neo4j(unique)

        logger.info("NewsData.io Downloader — Complete ✓")

    def insert_sentiment_to_clickhouse(self, articles: list[dict]):
        """Insert sentiment time-series into ClickHouse news_sentiment_ts."""
        if not articles:
            return

        rows = []
        for raw_article in articles:
            article = self._normalize_article(raw_article)
            published = article.get("published_at")
            sentiment = article.get("sentiment", 0.0)

            if published:
                # Use category as ticker-proxy for non-ticker-specific news
                topic_key = article.get("category", "general").upper()
                rows.append([
                    topic_key,
                    "newsdata",
                    float(sentiment),
                    0.0,  # tone not available from NewsData.io
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

    def insert_to_neo4j(self, articles: list[dict]):
        """Insert articles into Neo4j as graph nodes with relationships."""
        if not articles:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for raw_article in articles:
                    article = self._normalize_article(raw_article)
                    url = article.get("url", "")
                    if not url:
                        continue

                    category = article.get("category", "general")
                    source_name = article.get("author", "newsdata")
                    published = article.get("published_at")
                    pub_str = published.isoformat() if published else None

                    try:
                        session.run(
                            """
                            MERGE (a:Article {url: $url})
                            SET a.title = $title,
                                a.summary = $summary,
                                a.source_name = 'newsdata',
                                a.category = $category,
                                a.country = $country,
                                a.language = $language,
                                a.published_at = $published_at,
                                a.sentiment = $sentiment
                            MERGE (src:Source {name: $source_name})
                            MERGE (a)-[:FROM]->(src)
                            MERGE (cat:NewsCategory {name: $category})
                            MERGE (a)-[:CATEGORIZED_AS]->(cat)
                            """,
                            url=url,
                            title=article.get("title", ""),
                            summary=article.get("summary", ""),
                            category=category,
                            country=article.get("country", ""),
                            language=article.get("language", "en"),
                            published_at=pub_str,
                            sentiment=float(article.get("sentiment", 0.0)),
                            source_name=source_name or "unknown",
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
    downloader = NewsDataDownloader()
    try:
        # Optional: pass topics as CLI args
        topics = sys.argv[1:] if len(sys.argv) > 1 else None
        downloader.run(topics=topics)
    finally:
        downloader.close()
