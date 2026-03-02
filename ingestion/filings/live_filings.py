"""
Live SEC Filings — Monitor EDGAR RSS for new filings

Polls the SEC EDGAR RSS feed every 10 minutes for new
10-K, 10-Q, 8-K filings from tracked companies.

Usage:
    python ingestion/filings/live_filings.py
"""

import os
import sys
import time
from datetime import datetime, timezone

import httpx
import feedparser
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database

# SEC EDGAR RSS feeds for latest filings
EDGAR_RSS_FEEDS = {
    "all_filings": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&dateb=&owner=include&count=40&search_text=&action=getcurrent&output=atom",
    "10-K": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=10-K&dateb=&owner=include&count=40&search_text=&action=getcurrent&output=atom",
    "10-Q": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=10-Q&dateb=&owner=include&count=40&search_text=&action=getcurrent&output=atom",
    "8-K": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&dateb=&owner=include&count=40&search_text=&action=getcurrent&output=atom",
}

# Track what we've already seen
seen_filings = set()

POLL_INTERVAL = 600  # 10 minutes
USER_AGENT = os.getenv("EDGAR_USER_AGENT", "Atlas atlas@example.com")


def poll_edgar():
    """Poll EDGAR RSS feeds for new filings."""
    global seen_filings

    client = httpx.Client(
        timeout=30.0,
        headers={"User-Agent": USER_AGENT},
    )

    engine = Database.pg()
    from sqlalchemy import text

    new_count = 0

    for feed_name, url in EDGAR_RSS_FEEDS.items():
        try:
            resp = client.get(url)
            resp.raise_for_status()
            feed = feedparser.parse(resp.text)

            for entry in feed.entries:
                filing_id = entry.get("id", entry.get("link", ""))
                if filing_id in seen_filings:
                    continue

                seen_filings.add(filing_id)

                title = entry.get("title", "")
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                published = entry.get("published", "")
                updated = entry.get("updated", "")

                # Extract company name and form type from title
                # Format: "10-K - Company Name (0001234567) (Filer)"
                form_type = ""
                company_name = ""
                if " - " in title:
                    parts = title.split(" - ", 1)
                    form_type = parts[0].strip()
                    company_name = parts[1].split("(")[0].strip() if "(" in parts[1] else parts[1].strip()

                try:
                    with engine.connect() as conn:
                        conn.execute(
                            text("""
                                INSERT INTO filings (company_cik, form_type, filed_date, url, created_at)
                                VALUES (:cik, :form, NOW(), :url, NOW())
                                ON CONFLICT DO NOTHING
                            """),
                            {
                                "cik": "",
                                "form": form_type,
                                "url": link,
                            },
                        )
                        conn.commit()
                    new_count += 1
                except Exception as e:
                    logger.debug(f"Skip filing insert: {e}")

            time.sleep(0.5)  # Rate limit between feeds

        except Exception as e:
            logger.error(f"[EDGAR] {feed_name} poll failed: {e}")

    client.close()

    if new_count:
        logger.info(f"[EDGAR] {new_count} new filings detected")
    else:
        logger.debug("[EDGAR] No new filings")

    return new_count


def main():
    logger.info("━" * 50)
    logger.info("Live SEC Filings Monitor — Starting")
    logger.info(f"Polling EDGAR RSS every {POLL_INTERVAL}s")
    logger.info(f"Feeds: {', '.join(EDGAR_RSS_FEEDS.keys())}")
    logger.info("━" * 50)

    cycle = 0
    while True:
        try:
            cycle += 1
            logger.info(f"\n── Cycle {cycle} @ {datetime.now().strftime('%H:%M:%S')} ──")
            poll_edgar()
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Live SEC Filings — Stopped by user")
            break
        except Exception as e:
            logger.error(f"Cycle {cycle} error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
