"""
EDGAR Downloader — SEC Filing Fetcher for Atlas Search Engine

Downloads company filings (10-K, 10-Q, 8-K, etc.) from the SEC EDGAR API.
Free access — requires only a User-Agent string identifying you (name + email).

Data flow:
  SEC EDGAR API → fetch filings index → download full documents
                → PostgreSQL (companies + filings + filing_chunks)
                → Neo4j (:Company)-[:FILED]->(:Filing)

Endpoints:
  - https://efts.sec.gov/LATEST/search-index?q=...  (full-text search)
  - https://data.sec.gov/submissions/CIK{cik}.json  (company submissions)
  - https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&...
"""

import os
import sys
import time
import re
from datetime import datetime, timezone
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# Supported filing types relevant to financial analysis
SUPPORTED_FORM_TYPES = [
    "10-K",    # Annual report
    "10-Q",    # Quarterly report
    "8-K",     # Current/material events
    "20-F",    # Annual report (foreign private issuer)
    "6-K",     # Semi-annual report (foreign)
    "S-1",     # IPO registration
    "DEF 14A", # Proxy statement
    "13F-HR",  # Institutional holdings
]


class EDGARDownloader:
    """Downloads SEC filings from EDGAR and stores them in the Atlas databases."""

    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    FULL_TEXT_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
    FILING_BASE_URL = "https://www.sec.gov/Archives/edgar/data"

    def __init__(self, user_agent: Optional[str] = None):
        self.user_agent = user_agent or os.getenv("EDGAR_USER_AGENT", "Atlas atlas@example.com")
        self.client = httpx.Client(
            timeout=30.0,
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            follow_redirects=True,
        )
        # SEC rate limit: max 10 requests/second
        self._last_request_time = 0.0
        logger.info(f"EDGARDownloader initialized (User-Agent: {self.user_agent})")

    def _rate_limit(self):
        """Enforce SEC's 10 req/sec rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.12:  # ~8 req/sec to stay safe
            time.sleep(0.12 - elapsed)
        self._last_request_time = time.time()

    def _normalize_cik(self, cik: str) -> str:
        """Pad CIK to 10 digits as required by the EDGAR API."""
        return cik.strip().zfill(10)

    def fetch_company_submissions(self, cik: str) -> Optional[dict]:
        """Fetch the complete submissions history for a company by CIK."""
        cik = self._normalize_cik(cik)
        url = self.SUBMISSIONS_URL.format(cik=cik)

        self._rate_limit()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            logger.info(f"Fetched submissions for CIK {cik}: {data.get('name', 'Unknown')}")
            return data
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch submissions for CIK {cik}: {e}")
            return None

    def get_recent_filings(self, cik: str, form_types: list[str] = None, limit: int = 20) -> list[dict]:
        """
        Get recent filings for a company, filtered by form type.

        Args:
            cik: Central Index Key (e.g., "0000320193" for Apple)
            form_types: List of form types to filter (e.g., ["10-K", "10-Q"])
            limit: Max number of filings to return
        """
        form_types = form_types or SUPPORTED_FORM_TYPES
        data = self.fetch_company_submissions(cik)
        if not data:
            return []

        filings_data = data.get("filings", {}).get("recent", {})
        forms = filings_data.get("form", [])
        dates = filings_data.get("filingDate", [])
        accessions = filings_data.get("accessionNumber", [])
        primary_docs = filings_data.get("primaryDocument", [])
        descriptions = filings_data.get("primaryDocDescription", [])

        company_name = data.get("name", "")
        company_ticker = ""
        tickers = data.get("tickers", [])
        if tickers:
            company_ticker = tickers[0]

        filings = []
        for i in range(min(len(forms), limit)):
            form = forms[i]
            if form not in form_types:
                continue

            accession = accessions[i].replace("-", "")
            primary_doc = primary_docs[i] if i < len(primary_docs) else ""
            doc_url = f"{self.FILING_BASE_URL}/{cik.lstrip('0')}/{accession}/{primary_doc}"

            filings.append({
                "cik": cik,
                "company_name": company_name,
                "ticker": company_ticker,
                "form_type": form,
                "filed_at": dates[i] if i < len(dates) else None,
                "accession_number": accessions[i],
                "url": doc_url,
                "description": descriptions[i] if i < len(descriptions) else "",
            })

        logger.info(f"Found {len(filings)} filings for CIK {cik} (filtered: {form_types})")
        return filings

    def fetch_filing_document(self, url: str) -> Optional[str]:
        """Download the full text of a filing document."""
        self._rate_limit()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Strip HTML tags for plain text storage
            content = re.sub(r"<[^>]+>", " ", content)
            content = re.sub(r"\s+", " ", content).strip()

            logger.debug(f"Downloaded filing: {len(content)} chars from {url}")
            return content
        except httpx.HTTPError as e:
            logger.error(f"Failed to download filing from {url}: {e}")
            return None

    def _chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> list[str]:
        """Split text into overlapping chunks for embedding storage."""
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunk = text[start:end]
            if chunk.strip():
                chunks.append(chunk.strip())
            start += chunk_size - overlap
        return chunks

    def insert_to_postgres(self, filings: list[dict]):
        """Insert filings into PostgreSQL companies + filings + filing_chunks tables."""
        if not filings:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for filing in filings:
                try:
                    # Upsert company
                    ticker = filing.get("ticker", "")
                    if ticker:
                        conn.execute(
                            text("""
                                INSERT INTO companies (ticker, name, cik)
                                VALUES (:ticker, :name, :cik)
                                ON CONFLICT (ticker) DO UPDATE SET name = :name, cik = :cik
                            """),
                            {
                                "ticker": ticker[:10],
                                "name": filing.get("company_name", "")[:255],
                                "cik": filing.get("cik", "")[:20],
                            },
                        )

                        # Get company_id
                        result = conn.execute(
                            text("SELECT id FROM companies WHERE ticker = :ticker"),
                            {"ticker": ticker[:10]},
                        )
                        row = result.fetchone()
                        company_id = row[0] if row else None
                    else:
                        company_id = None

                    # Parse date
                    filed_at = None
                    date_str = filing.get("filed_at")
                    if date_str:
                        try:
                            filed_at = datetime.strptime(date_str, "%Y-%m-%d").date()
                        except ValueError:
                            pass

                    # Insert filing
                    result = conn.execute(
                        text("""
                            INSERT INTO filings (company_id, form_type, filed_at, url)
                            VALUES (:company_id, :form_type, :filed_at, :url)
                            ON CONFLICT DO NOTHING
                            RETURNING id
                        """),
                        {
                            "company_id": company_id,
                            "form_type": filing.get("form_type", "")[:20],
                            "filed_at": filed_at,
                            "url": filing.get("url", ""),
                        },
                    )

                    filing_row = result.fetchone()
                    filing_id = filing_row[0] if filing_row else None

                    # Download and chunk the filing content
                    if filing_id and filing.get("url"):
                        content = self.fetch_filing_document(filing["url"])
                        if content:
                            chunks = self._chunk_text(content)
                            for chunk in chunks[:50]:  # Cap chunks per filing
                                conn.execute(
                                    text("""
                                        INSERT INTO filing_chunks (filing_id, content)
                                        VALUES (:filing_id, :content)
                                    """),
                                    {"filing_id": filing_id, "content": chunk},
                                )

                    inserted += 1
                except Exception as e:
                    logger.debug(f"Skipped filing: {e}")

            conn.commit()

        logger.info(f"PostgreSQL: inserted {inserted} filings")

    def insert_to_neo4j(self, filings: list[dict]):
        """Insert filings into Neo4j as (:Company)-[:FILED]->(:Filing) graph."""
        if not filings:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for filing in filings:
                    ticker = filing.get("ticker", "")
                    if not ticker:
                        continue

                    try:
                        session.run(
                            """
                            MERGE (c:Company {ticker: $ticker})
                            SET c.name = $name, c.cik = $cik
                            MERGE (f:Filing {id: $accession})
                            SET f.form_type = $form_type,
                                f.filed_at = $filed_at,
                                f.url = $url
                            MERGE (c)-[:FILED]->(f)
                            """,
                            ticker=ticker[:10],
                            name=filing.get("company_name", ""),
                            cik=filing.get("cik", ""),
                            accession=filing.get("accession_number", ""),
                            form_type=filing.get("form_type", ""),
                            filed_at=filing.get("filed_at", ""),
                            url=filing.get("url", ""),
                        )
                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j skip: {e}")

            logger.info(f"Neo4j: created {created} filing nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def run(self, ciks: list[str] = None, form_types: list[str] = None):
        """
        Full pipeline: fetch filings for given CIKs → download → store in PG + Neo4j.

        Args:
            ciks: List of CIK numbers. Defaults to major companies.
            form_types: Filing types to fetch. Defaults to SUPPORTED_FORM_TYPES.
        """
        logger.info("━" * 50)
        logger.info("EDGAR Downloader — Starting")
        logger.info("━" * 50)

        # Default: major US companies
        ciks = ciks or [
            "0000320193",  # Apple
            "0000789019",  # Microsoft
            "0001652044",  # Alphabet/Google
            "0001018724",  # Amazon
            "0001318605",  # Tesla
        ]

        all_filings = []
        for cik in ciks:
            filings = self.get_recent_filings(cik, form_types=form_types, limit=5)
            all_filings.extend(filings)

        logger.info(f"Total filings to process: {len(all_filings)}")

        self.insert_to_postgres(all_filings)
        self.insert_to_neo4j(all_filings)

        logger.info("EDGAR Downloader — Complete ✓")

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = EDGARDownloader()
    try:
        # Pass CIK numbers as CLI args, or use defaults
        ciks = sys.argv[1:] if len(sys.argv) > 1 else None
        downloader.run(ciks=ciks)
    finally:
        downloader.close()
