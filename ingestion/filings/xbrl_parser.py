"""
XBRL Parser — Financial Statement Data Extractor

Parses XBRL (eXtensible Business Reporting Language) data from SEC filings
to extract structured financial facts (revenue, net income, EPS, assets, etc.).

Data flow:
  SEC EDGAR XBRL JSON API → parse facts → PostgreSQL (structured financials)
                                         → ClickHouse (time-series metrics)
                                         → Neo4j (:Company)-[:REPORTED]->(:Metric)

Endpoints:
  - https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json  (all facts for a company)
  - https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept}.json  (specific concept)
"""

import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# Key financial concepts to extract from XBRL filings (US-GAAP taxonomy)
KEY_CONCEPTS = {
    # Income Statement
    "Revenues": "Total Revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "Net Revenue",
    "CostOfGoodsAndServicesSold": "Cost of Goods Sold",
    "GrossProfit": "Gross Profit",
    "OperatingIncomeLoss": "Operating Income",
    "NetIncomeLoss": "Net Income",
    "EarningsPerShareBasic": "EPS (Basic)",
    "EarningsPerShareDiluted": "EPS (Diluted)",

    # Balance Sheet
    "Assets": "Total Assets",
    "Liabilities": "Total Liabilities",
    "StockholdersEquity": "Stockholders Equity",
    "CashAndCashEquivalentsAtCarryingValue": "Cash & Equivalents",
    "LongTermDebt": "Long-term Debt",
    "ShortTermBorrowings": "Short-term Debt",

    # Cash Flow
    "NetCashProvidedByUsedInOperatingActivities": "Operating Cash Flow",
    "NetCashProvidedByUsedInInvestingActivities": "Investing Cash Flow",
    "NetCashProvidedByUsedInFinancingActivities": "Financing Cash Flow",
    "PaymentsOfDividends": "Dividends Paid",

    # Shares
    "CommonStockSharesOutstanding": "Shares Outstanding",
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted": "Weighted Avg Shares",
}


class XBRLParser:
    """Parses XBRL financial data from SEC EDGAR."""

    COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    COMPANY_CONCEPT_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{concept}.json"

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
        self._last_request_time = 0.0
        logger.info(f"XBRLParser initialized (User-Agent: {self.user_agent})")

    def _rate_limit(self):
        """Enforce SEC's 10 req/sec rate limit."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.12:
            time.sleep(0.12 - elapsed)
        self._last_request_time = time.time()

    def _normalize_cik(self, cik: str) -> str:
        return cik.strip().zfill(10)

    def fetch_company_facts(self, cik: str) -> Optional[dict]:
        """
        Fetch all XBRL facts for a company.
        Returns the full companyfacts JSON including all taxonomies and concepts.
        """
        cik = self._normalize_cik(cik)
        url = self.COMPANY_FACTS_URL.format(cik=cik)

        self._rate_limit()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
            name = data.get("entityName", "Unknown")
            logger.info(f"Fetched XBRL facts for {name} (CIK: {cik})")
            return data
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch XBRL facts for CIK {cik}: {e}")
            return None

    def extract_key_metrics(self, cik: str) -> list[dict]:
        """
        Extract key financial metrics from a company's XBRL data.
        Returns a flat list of metric records with values over time.
        """
        data = self.fetch_company_facts(cik)
        if not data:
            return []

        entity_name = data.get("entityName", "")
        facts = data.get("facts", {})
        us_gaap = facts.get("us-gaap", {})

        metrics = []
        for concept, label in KEY_CONCEPTS.items():
            concept_data = us_gaap.get(concept)
            if not concept_data:
                continue

            units = concept_data.get("units", {})

            # Try USD first, then shares, then pure (for ratios/EPS)
            for unit_key in ["USD", "shares", "pure", "USD/shares"]:
                entries = units.get(unit_key, [])
                for entry in entries:
                    # Only take annual (10-K) and quarterly (10-Q) filings
                    form = entry.get("form", "")
                    if form not in ("10-K", "10-Q"):
                        continue

                    # Parse date
                    end_date = entry.get("end")
                    filed_date = entry.get("filed")

                    metrics.append({
                        "cik": cik,
                        "company_name": entity_name,
                        "concept": concept,
                        "label": label,
                        "value": entry.get("val"),
                        "unit": unit_key,
                        "form_type": form,
                        "period_end": end_date,
                        "filed_at": filed_date,
                        "fiscal_year": entry.get("fy"),
                        "fiscal_period": entry.get("fp"),
                        "accession": entry.get("accn", ""),
                    })

        logger.info(f"Extracted {len(metrics)} metric records for CIK {cik}")
        return metrics

    def insert_to_clickhouse(self, metrics: list[dict]):
        """Insert financial metrics as time-series into ClickHouse."""
        if not metrics:
            return

        rows = []
        for m in metrics:
            end_date = m.get("period_end")
            if not end_date or m.get("value") is None:
                continue

            try:
                ts = datetime.strptime(end_date, "%Y-%m-%d")
            except (ValueError, TypeError):
                continue

            # Use concept as the "symbol" for time-series storage
            ticker_key = f"{m.get('cik', '')}:{m.get('concept', '')}"

            rows.append([
                ticker_key,
                float(m["value"]),
                0.0,  # volume placeholder
                ts,
            ])

        if not rows:
            return

        try:
            ch = Database.clickhouse()
            ch.insert(
                "market_ticks",
                rows,
                column_names=["symbol", "price", "volume", "timestamp"],
            )
            ch.close()
            logger.info(f"ClickHouse: inserted {len(rows)} XBRL metric points")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

    def insert_to_neo4j(self, metrics: list[dict]):
        """Insert metrics into Neo4j as (:Company)-[:REPORTED]->(:Metric) graph."""
        if not metrics:
            return

        driver = Database.neo4j()
        created = 0

        # Deduplicate: one node per (company, concept, period_end)
        seen = set()

        try:
            with driver.session(database="atlas") as session:
                for m in metrics:
                    cik = m.get("cik", "")
                    concept = m.get("concept", "")
                    period_end = m.get("period_end", "")
                    key = (cik, concept, period_end)
                    if key in seen:
                        continue
                    seen.add(key)

                    try:
                        session.run(
                            """
                            MERGE (c:Company {cik: $cik})
                            SET c.name = $name
                            MERGE (m:Metric {id: $metric_id})
                            SET m.concept = $concept,
                                m.label = $label,
                                m.value = $value,
                                m.unit = $unit,
                                m.period_end = $period_end,
                                m.form_type = $form_type
                            MERGE (c)-[:REPORTED]->(m)
                            """,
                            cik=cik,
                            name=m.get("company_name", ""),
                            metric_id=f"{cik}_{concept}_{period_end}",
                            concept=concept,
                            label=m.get("label", ""),
                            value=float(m["value"]) if m.get("value") is not None else 0.0,
                            unit=m.get("unit", ""),
                            period_end=period_end,
                            form_type=m.get("form_type", ""),
                        )
                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j skip: {e}")

            logger.info(f"Neo4j: created {created} metric nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def run(self, ciks: list[str] = None):
        """
        Full pipeline: fetch XBRL facts → extract metrics → store in ClickHouse + Neo4j.

        Args:
            ciks: List of CIK numbers. Defaults to major companies.
        """
        logger.info("━" * 50)
        logger.info("XBRL Parser — Starting")
        logger.info("━" * 50)

        ciks = ciks or [
            "0000320193",  # Apple
            "0000789019",  # Microsoft
            "0001652044",  # Alphabet/Google
            "0001018724",  # Amazon
            "0001318605",  # Tesla
        ]

        all_metrics = []
        for cik in ciks:
            metrics = self.extract_key_metrics(cik)
            all_metrics.extend(metrics)

        logger.info(f"Total metrics extracted: {len(all_metrics)}")

        self.insert_to_clickhouse(all_metrics)
        self.insert_to_neo4j(all_metrics)

        logger.info("XBRL Parser — Complete ✓")

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    parser = XBRLParser()
    try:
        # Pass CIK numbers as CLI args, or use defaults
        ciks = sys.argv[1:] if len(sys.argv) > 1 else None
        parser.run(ciks=ciks)
    finally:
        parser.close()
