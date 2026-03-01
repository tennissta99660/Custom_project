"""
RSS/Atom Feed Aggregator — Curated global news from 50+ free sources.

No API key needed. Uses feedparser to consume RSS/Atom feeds from:
  - Reuters, BBC, AP, Al Jazeera (world news)
  - Bloomberg, FT, WSJ, CNBC (financial)
  - OilPrice, Platts (commodities)
  - Lloyd's List, TradeWinds (shipping)
  - Federal Reserve, ECB, RBI (central banks)
  - AgriPulse, World-Grain (agriculture)
  - UN News, Federal Register (government)

Data flow:
  RSS feeds → feedparser → deduplicate by URL → PostgreSQL (news_articles)
"""

import os
import sys
import hashlib
from datetime import datetime, timezone
from typing import Optional
from email.utils import parsedate_to_datetime

import feedparser
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── Feed Configuration ──────────────────────────────────────────────
# Organized by category for easy filtering and expansion
FEED_SOURCES = {
    # ── World / Geopolitics ──
    "world": [
        {"name": "Reuters World",         "url": "https://feeds.reuters.com/Reuters/worldNews",           "lang": "en", "country": "US"},
        {"name": "BBC World",             "url": "http://feeds.bbci.co.uk/news/world/rss.xml",            "lang": "en", "country": "GB"},
        {"name": "AP Top News",           "url": "https://rsshub.app/apnews/topics/apf-topnews",         "lang": "en", "country": "US"},
        {"name": "Al Jazeera",            "url": "https://www.aljazeera.com/xml/rss/all.xml",             "lang": "en", "country": "QA"},
        {"name": "France24",              "url": "https://www.france24.com/en/rss",                       "lang": "en", "country": "FR"},
        {"name": "DW News",               "url": "https://rss.dw.com/rdf/rss-en-all",                    "lang": "en", "country": "DE"},
        {"name": "NHK World",             "url": "https://www3.nhk.or.jp/rss/news/cat0.xml",             "lang": "en", "country": "JP"},
    ],

    # ── Financial / Markets ──
    "financial": [
        {"name": "Reuters Business",      "url": "https://feeds.reuters.com/reuters/businessNews",        "lang": "en", "country": "US"},
        {"name": "CNBC Top News",         "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "lang": "en", "country": "US"},
        {"name": "MarketWatch",           "url": "http://feeds.marketwatch.com/marketwatch/topstories",   "lang": "en", "country": "US"},
        {"name": "Yahoo Finance",         "url": "https://finance.yahoo.com/news/rssindex",               "lang": "en", "country": "US"},
        {"name": "Investing.com",         "url": "https://www.investing.com/rss/news.rss",                "lang": "en", "country": "US"},
        {"name": "Economic Times India",  "url": "https://economictimes.indiatimes.com/rssfeedsdefault.cms", "lang": "en", "country": "IN"},
        {"name": "Moneycontrol",          "url": "https://www.moneycontrol.com/rss/latestnews.xml",       "lang": "en", "country": "IN"},
    ],

    # ── Commodities / Energy ──
    "commodities": [
        {"name": "OilPrice.com",          "url": "https://oilprice.com/rss/main",                        "lang": "en", "country": "US"},
        {"name": "Reuters Commodities",   "url": "https://feeds.reuters.com/news/commodities",            "lang": "en", "country": "US"},
        {"name": "Mining.com",            "url": "https://www.mining.com/feed/",                          "lang": "en", "country": "CA"},
        {"name": "Rigzone",              "url": "https://www.rigzone.com/news/rss/rigzone_latest.aspx",  "lang": "en", "country": "US"},
    ],

    # ── Shipping / Maritime ──
    "shipping": [
        {"name": "Maritime Executive",    "url": "https://www.maritime-executive.com/rss",                "lang": "en", "country": "US"},
        {"name": "Splash247",            "url": "https://splash247.com/feed/",                           "lang": "en", "country": "SG"},
        {"name": "gCaptain",             "url": "https://gcaptain.com/feed/",                            "lang": "en", "country": "US"},
        {"name": "Hellenic Shipping",    "url": "https://www.hellenicshippingnews.com/feed/",            "lang": "en", "country": "GR"},
    ],

    # ── Agriculture ──
    "agriculture": [
        {"name": "World-Grain",          "url": "https://www.world-grain.com/ext/rss",                   "lang": "en", "country": "US"},
        {"name": "AgriPulse",            "url": "https://www.agri-pulse.com/rss",                        "lang": "en", "country": "US"},
        {"name": "FarmProgress",         "url": "https://www.farmprogress.com/rss.xml",                  "lang": "en", "country": "US"},
    ],

    # ── Central Banks / Policy ──
    "central_banks": [
        {"name": "Federal Reserve",       "url": "https://www.federalreserve.gov/feeds/press_all.xml",   "lang": "en", "country": "US"},
        {"name": "ECB Press",            "url": "https://www.ecb.europa.eu/rss/press.html",              "lang": "en", "country": "EU"},
        {"name": "RBI Press",            "url": "https://rbi.org.in/scripts/BS_PressReleaseDisplay.aspx?format=rss", "lang": "en", "country": "IN"},
    ],

    # ── Tech / Cyber Security ──
    "technology": [
        {"name": "Ars Technica",         "url": "https://feeds.arstechnica.com/arstechnica/index",       "lang": "en", "country": "US"},
        {"name": "The Verge",            "url": "https://www.theverge.com/rss/index.xml",                "lang": "en", "country": "US"},
        {"name": "TechCrunch",           "url": "https://techcrunch.com/feed/",                          "lang": "en", "country": "US"},
        {"name": "Krebs on Security",    "url": "https://krebsonsecurity.com/feed/",                     "lang": "en", "country": "US"},
        {"name": "Hacker News",          "url": "https://hnrss.org/frontpage",                           "lang": "en", "country": "US"},
    ],

    # ── Science / Environment ──
    "science": [
        {"name": "Nature News",          "url": "https://www.nature.com/nature.rss",                     "lang": "en", "country": "GB"},
        {"name": "NASA News",            "url": "https://www.nasa.gov/rss/dyn/breaking_news.rss",        "lang": "en", "country": "US"},
        {"name": "UN News",              "url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml","lang": "en", "country": "UN"},
    ],

    # ── India Specific ──
    "india": [
        {"name": "NDTV",                 "url": "https://feeds.feedburner.com/ndtvnews-india-news",      "lang": "en", "country": "IN"},
        {"name": "LiveMint",             "url": "https://www.livemint.com/rss/news",                     "lang": "en", "country": "IN"},
        {"name": "Business Standard",    "url": "https://www.business-standard.com/rss/home_page_top_stories.rss", "lang": "en", "country": "IN"},
    ],
}


class RSSAggregator:
    """Aggregates news from curated RSS/Atom feeds worldwide."""

    def __init__(self, categories: list[str] = None):
        """
        Args:
            categories: List of feed categories to fetch. None = all categories.
                        Options: world, financial, commodities, shipping, agriculture,
                                 central_banks, technology, science, india
        """
        self.categories = categories or list(FEED_SOURCES.keys())
        self._seen_urls = set()
        logger.info(f"RSSAggregator initialized — categories: {self.categories}")

    def _url_hash(self, url: str) -> str:
        """Generate a dedup hash from URL."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]

    def _parse_date(self, entry: dict) -> Optional[datetime]:
        """Extract published date from feed entry."""
        date_fields = ["published", "updated", "created"]
        for field in date_fields:
            val = entry.get(f"{field}_parsed")
            if val:
                try:
                    from time import mktime
                    return datetime.fromtimestamp(mktime(val), tz=timezone.utc)
                except (TypeError, ValueError, OverflowError):
                    pass

            val_str = entry.get(field)
            if val_str:
                try:
                    return parsedate_to_datetime(val_str).replace(tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    pass

        return None

    def _extract_image(self, entry: dict) -> str:
        """Try to extract an image URL from the feed entry."""
        # Check media_content
        media = entry.get("media_content", [])
        if media and isinstance(media, list):
            for m in media:
                if m.get("medium") == "image" or "image" in m.get("type", ""):
                    return m.get("url", "")

        # Check media_thumbnail
        thumb = entry.get("media_thumbnail", [])
        if thumb and isinstance(thumb, list):
            return thumb[0].get("url", "")

        # Check enclosures
        links = entry.get("links", [])
        for link in links:
            if "image" in link.get("type", ""):
                return link.get("href", "")

        return ""

    def fetch_feed(self, source: dict) -> list[dict]:
        """Parse a single RSS/Atom feed and return normalized articles."""
        name = source["name"]
        url = source["url"]
        articles = []

        try:
            feed = feedparser.parse(url)

            if feed.bozo and not feed.entries:
                logger.warning(f"Feed error for {name}: {feed.bozo_exception}")
                return []

            for entry in feed.entries:
                article_url = entry.get("link", "")
                if not article_url:
                    continue

                # Dedup
                url_hash = self._url_hash(article_url)
                if url_hash in self._seen_urls:
                    continue
                self._seen_urls.add(url_hash)

                # Extract content — prefer summary, fallback to description
                summary = ""
                if hasattr(entry, "summary"):
                    summary = entry.summary
                elif hasattr(entry, "description"):
                    summary = entry.description

                # Strip HTML tags (simple approach)
                import re
                summary = re.sub(r"<[^>]+>", "", summary).strip()

                articles.append({
                    "source_name": "rss",
                    "source_id": url_hash,
                    "title": entry.get("title", "")[:1024],
                    "summary": summary[:4096],
                    "content": None,
                    "url": article_url,
                    "author": entry.get("author", source["name"]),
                    "category": source.get("_category", "general"),
                    "language": source.get("lang", "en"),
                    "country": source.get("country", ""),
                    "image_url": self._extract_image(entry),
                    "published_at": self._parse_date(entry),
                    "feed_name": name,
                })

            logger.info(f"  {name}: {len(articles)} articles")
        except Exception as e:
            logger.error(f"  {name}: FAILED — {e}")

        return articles

    def fetch_all(self) -> list[dict]:
        """Fetch articles from all configured feeds."""
        all_articles = []
        self._seen_urls.clear()

        for category in self.categories:
            feeds = FEED_SOURCES.get(category, [])
            logger.info(f"Category [{category}]: {len(feeds)} feeds")

            for source in feeds:
                source["_category"] = category
                articles = self.fetch_feed(source)
                all_articles.extend(articles)

        return all_articles

    def insert_to_postgres(self, articles: list[dict]):
        """Insert articles into news_articles table."""
        if not articles:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for article in articles:
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
                            "content": article.get("content"),
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

        logger.info(f"PostgreSQL: inserted {inserted} articles from RSS feeds")

    def run(self):
        """Full pipeline: fetch all RSS feeds → insert to PostgreSQL."""
        logger.info("━" * 50)
        logger.info("RSS Aggregator — Starting")
        logger.info(f"Categories: {', '.join(self.categories)}")
        logger.info("━" * 50)

        articles = self.fetch_all()
        if not articles:
            logger.warning("No articles fetched from any feed")
            return

        logger.info(f"Total unique articles: {len(articles)}")
        self.insert_to_postgres(articles)
        self.insert_sentiment_to_clickhouse(articles)
        self.insert_to_neo4j(articles)

        # Print category breakdown
        from collections import Counter
        breakdown = Counter(a["category"] for a in articles)
        for cat, count in breakdown.most_common():
            logger.info(f"  {cat}: {count} articles")

        logger.info("RSS Aggregator — Complete ✓")


    def insert_sentiment_to_clickhouse(self, articles: list[dict]):
        """Insert per-category sentiment time-series into ClickHouse."""
        if not articles:
            return

        rows = []
        for article in articles:
            published = article.get("published_at")
            if published:
                category_key = article.get("category", "general").upper()
                rows.append([
                    category_key,
                    "rss",
                    0.0,  # sentiment — RSS feeds don't provide this
                    0.0,  # tone
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
            logger.info(f"ClickHouse: inserted {len(rows)} RSS sentiment rows")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

    def insert_to_neo4j(self, articles: list[dict]):
        """Insert articles into Neo4j as graph nodes with source and category relationships."""
        if not articles:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for article in articles:
                    url = article.get("url", "")
                    if not url:
                        continue

                    category = article.get("category", "general")
                    feed_name = article.get("feed_name", article.get("author", "rss"))
                    published = article.get("published_at")
                    pub_str = published.isoformat() if published else None

                    try:
                        session.run(
                            """
                            MERGE (a:Article {url: $url})
                            SET a.title = $title,
                                a.source_name = 'rss',
                                a.category = $category,
                                a.country = $country,
                                a.language = $language,
                                a.published_at = $published_at
                            MERGE (src:Source {name: $feed_name})
                            MERGE (a)-[:FROM]->(src)
                            MERGE (cat:NewsCategory {name: $category})
                            MERGE (a)-[:CATEGORIZED_AS]->(cat)
                            """,
                            url=url,
                            title=article.get("title", ""),
                            category=category,
                            country=article.get("country", ""),
                            language=article.get("language", "en"),
                            published_at=pub_str,
                            feed_name=feed_name,
                        )
                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j skip: {e}")

            logger.info(f"Neo4j: created {created} article nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()


# ── CLI Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Pass category names as CLI args, or fetch all
    categories = sys.argv[1:] if len(sys.argv) > 1 else None
    aggregator = RSSAggregator(categories=categories)
    aggregator.run()
