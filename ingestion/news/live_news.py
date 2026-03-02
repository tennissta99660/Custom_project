"""
Live News Ingestion — Continuous news monitoring

Runs 3 polling loops:
  1. RSS feeds — every 5 minutes
  2. GDELT events — every 15 minutes (matches GDELT update cycle)
  3. Finnhub company news — every 10 minutes

Usage:
    python ingestion/news/live_news.py
"""

import os
import sys
import time
import threading
from datetime import datetime, timezone

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


def poll_rss(interval: int = 300):
    """Poll RSS feeds every 5 minutes."""
    from ingestion.news.rss_aggregator import RSSAggregator

    logger.info(f"[RSS] Starting — polling every {interval}s")
    agg = RSSAggregator()

    while True:
        try:
            logger.info(f"[RSS] Fetching @ {datetime.now().strftime('%H:%M:%S')}")
            agg.run()
            logger.info("[RSS] Cycle complete")
        except Exception as e:
            logger.error(f"[RSS] Error: {e}")
        time.sleep(interval)


def poll_gdelt(interval: int = 900):
    """Poll GDELT every 15 minutes (matches their update cycle)."""
    from ingestion.news.gdelt_downloader import GDELTDownloader

    logger.info(f"[GDELT] Starting — polling every {interval}s")
    dl = GDELTDownloader()

    while True:
        try:
            logger.info(f"[GDELT] Fetching @ {datetime.now().strftime('%H:%M:%S')}")
            dl.run()
            logger.info("[GDELT] Cycle complete")
        except Exception as e:
            logger.error(f"[GDELT] Error: {e}")
        time.sleep(interval)


def poll_finnhub(interval: int = 600):
    """Poll Finnhub company news every 10 minutes."""
    from ingestion.news.finnhub_news_downloader import FinnhubNewsDownloader

    api_key = os.getenv("FINNHUB_API_KEY", "")
    if not api_key:
        logger.warning("[Finnhub] No API key — skipping")
        return

    logger.info(f"[Finnhub] Starting — polling every {interval}s")
    dl = FinnhubNewsDownloader()

    while True:
        try:
            logger.info(f"[Finnhub] Fetching @ {datetime.now().strftime('%H:%M:%S')}")
            dl.run()
            logger.info("[Finnhub] Cycle complete")
        except Exception as e:
            logger.error(f"[Finnhub] Error: {e}")
        time.sleep(interval)


def main():
    logger.info("━" * 50)
    logger.info("Live News Ingestion — Starting")
    logger.info("  RSS:     every 5 min")
    logger.info("  GDELT:   every 15 min")
    logger.info("  Finnhub: every 10 min")
    logger.info("━" * 50)

    threads = [
        threading.Thread(target=poll_rss, daemon=True, name="rss"),
        threading.Thread(target=poll_gdelt, daemon=True, name="gdelt"),
        threading.Thread(target=poll_finnhub, daemon=True, name="finnhub"),
    ]

    for t in threads:
        t.start()
        time.sleep(2)  # Stagger start

    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            logger.debug(f"Active threads: {', '.join(alive)}")
    except KeyboardInterrupt:
        logger.info("Live News Ingestion — Stopped by user")


if __name__ == "__main__":
    main()
