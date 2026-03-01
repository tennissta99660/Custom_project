# News ingestion pipeline
# Sources: GDELT, Finnhub, RSS feeds, NewsData.io

from ingestion.news.gdelt_downloader import GDELTDownloader
from ingestion.news.finnhub_news_downloader import FinnhubNewsDownloader
from ingestion.news.rss_aggregator import RSSAggregator
from ingestion.news.newsdata_downloader import NewsDataDownloader

__all__ = [
    "GDELTDownloader",
    "FinnhubNewsDownloader",
    "RSSAggregator",
    "NewsDataDownloader",
]
