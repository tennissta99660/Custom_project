# News ingestion pipeline
# Sources: GDELT, Finnhub, RSS feeds

from ingestion.news.gdelt_downloader import GDELTDownloader
from ingestion.news.finnhub_news_downloader import FinnhubNewsDownloader
from ingestion.news.rss_aggregator import RSSAggregator

__all__ = [
    "GDELTDownloader",
    "FinnhubNewsDownloader",
    "RSSAggregator",
]
