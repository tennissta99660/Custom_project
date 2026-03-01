"""
GDELT Downloader — Global Database of Events, Language, and Tone

Fetches global event data updated every 15 minutes from the GDELT Project.
Completely free, no API key required, unlimited access.

Data flow:
  GDELT CSV batch → parse → PostgreSQL (gdelt_events + PostGIS) + ClickHouse (aggregates)
  
Endpoints used:
  - http://data.gdeltproject.org/gdeltv2/lastupdate.txt  (latest 15-min batch URLs)
  - CSV columns: GlobalEventID, Day, Actor1/2, EventCode, GoldsteinScale, NumMentions, AvgTone, Geo, SourceURL
"""

import os
import sys
import io
import zipfile
import csv
from datetime import datetime, timezone
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── GDELT v2 Column Mapping ────────────────────────────────────────
# Full spec: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
GDELT_COLUMNS = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone",
    "Actor1Geo_Type", "Actor1Geo_FullName", "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
    "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
    "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID",
    "DATEADDED", "SOURCEURL",
]

# CAMEO event code descriptions (top-level)
CAMEO_ROOT_CODES = {
    "01": "Make public statement",
    "02": "Appeal",
    "03": "Express intent to cooperate",
    "04": "Consult",
    "05": "Engage in diplomatic cooperation",
    "06": "Engage in material cooperation",
    "07": "Provide aid",
    "08": "Yield",
    "09": "Investigate",
    "10": "Demand",
    "11": "Disapprove",
    "12": "Reject",
    "13": "Threaten",
    "14": "Protest",
    "15": "Exhibit military posture",
    "16": "Reduce relations",
    "17": "Coerce",
    "18": "Assault",
    "19": "Fight",
    "20": "Engage in unconventional mass violence",
}


class GDELTDownloader:
    """Downloads and parses GDELT v2 event data."""

    LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

    def __init__(self):
        self.client = httpx.Client(timeout=60.0, follow_redirects=True)
        logger.info("GDELTDownloader initialized")

    def _get_latest_export_url(self) -> Optional[str]:
        """Fetch the URL of the latest 15-min GDELT export CSV."""
        try:
            resp = self.client.get(self.LAST_UPDATE_URL)
            resp.raise_for_status()
            for line in resp.text.strip().split("\n"):
                parts = line.strip().split()
                if len(parts) >= 3 and parts[2].endswith(".export.CSV.zip"):
                    return parts[2]
            logger.warning("No export CSV found in lastupdate.txt")
            return None
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch GDELT last update: {e}")
            return None

    def _download_and_extract_csv(self, url: str) -> list[dict]:
        """Download a zipped GDELT CSV and parse it into dicts."""
        events = []
        try:
            resp = self.client.get(url)
            resp.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for filename in zf.namelist():
                    if filename.endswith(".CSV"):
                        with zf.open(filename) as f:
                            reader = csv.reader(
                                io.TextIOWrapper(f, encoding="utf-8", errors="replace"),
                                delimiter="\t",
                            )
                            for row in reader:
                                if len(row) >= len(GDELT_COLUMNS):
                                    event = dict(zip(GDELT_COLUMNS, row))
                                    events.append(event)

            logger.info(f"Parsed {len(events)} events from {url}")
        except Exception as e:
            logger.error(f"Failed to download/parse GDELT CSV: {e}")

        return events

    def _safe_float(self, val: str, default: float = 0.0) -> float:
        try:
            return float(val) if val else default
        except (ValueError, TypeError):
            return default

    def _safe_int(self, val: str, default: int = 0) -> int:
        try:
            return int(val) if val else default
        except (ValueError, TypeError):
            return default

    def _get_event_description(self, event_code: str) -> str:
        """Get human-readable description from CAMEO root code."""
        root = event_code[:2] if event_code else ""
        return CAMEO_ROOT_CODES.get(root, "Unknown event")

    def fetch_latest(self) -> list[dict]:
        """Fetch the latest 15-minute GDELT batch, returns parsed events."""
        url = self._get_latest_export_url()
        if not url:
            return []
        return self._download_and_extract_csv(url)

    def insert_to_postgres(self, events: list[dict]):
        """Insert parsed events into PostgreSQL gdelt_events table."""
        if not events:
            logger.warning("No events to insert into PostgreSQL")
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        skipped = 0

        with engine.connect() as conn:
            for event in events:
                lat = self._safe_float(event.get("ActionGeo_Lat"))
                lon = self._safe_float(event.get("ActionGeo_Long"))
                global_event_id = self._safe_int(event.get("GlobalEventID"))

                if not global_event_id:
                    skipped += 1
                    continue

                # Parse event date
                day_str = event.get("Day", "")
                try:
                    event_date = datetime.strptime(day_str, "%Y%m%d").date() if day_str else None
                except ValueError:
                    event_date = None

                try:
                    conn.execute(
                        text("""
                            INSERT INTO gdelt_events (
                                global_event_id, event_date,
                                actor1_name, actor1_country, actor1_type,
                                actor2_name, actor2_country, actor2_type,
                                event_code, event_description,
                                goldstein_scale, num_mentions, num_sources, num_articles,
                                avg_tone, geom, source_url
                            ) VALUES (
                                :geid, :edate,
                                :a1name, :a1country, :a1type,
                                :a2name, :a2country, :a2type,
                                :ecode, :edesc,
                                :goldstein, :mentions, :sources, :articles,
                                :tone, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), :url
                            )
                            ON CONFLICT (global_event_id) DO NOTHING
                        """),
                        {
                            "geid": global_event_id,
                            "edate": event_date,
                            "a1name": event.get("Actor1Name", "")[:255],
                            "a1country": event.get("Actor1CountryCode", "")[:10],
                            "a1type": event.get("Actor1Type1Code", "")[:100],
                            "a2name": event.get("Actor2Name", "")[:255],
                            "a2country": event.get("Actor2CountryCode", "")[:10],
                            "a2type": event.get("Actor2Type1Code", "")[:100],
                            "ecode": event.get("EventCode", "")[:10],
                            "edesc": self._get_event_description(event.get("EventCode", "")),
                            "goldstein": self._safe_float(event.get("GoldsteinScale")),
                            "mentions": self._safe_int(event.get("NumMentions")),
                            "sources": self._safe_int(event.get("NumSources")),
                            "articles": self._safe_int(event.get("NumArticles")),
                            "tone": self._safe_float(event.get("AvgTone")),
                            "lat": lat,
                            "lon": lon,
                            "url": event.get("SOURCEURL", "")[:2048],
                        },
                    )
                    inserted += 1
                except Exception as e:
                    skipped += 1
                    logger.debug(f"Skipped event {global_event_id}: {e}")

            conn.commit()

        logger.info(f"PostgreSQL: inserted {inserted}, skipped {skipped}")

    def insert_to_clickhouse(self, events: list[dict]):
        """Insert event volume aggregates into ClickHouse gdelt_event_volume."""
        if not events:
            return

        from collections import defaultdict

        # Aggregate by event_code + country + hour
        buckets = defaultdict(lambda: {"count": 0, "tone_sum": 0.0, "goldstein_sum": 0.0})

        for event in events:
            code = event.get("EventCode", "UNK")[:10]
            country = event.get("ActionGeo_CountryCode", "XX")[:10]
            day_str = event.get("DATEADDED", "")

            try:
                ts = datetime.strptime(day_str[:10], "%Y%m%d%H") if len(day_str) >= 10 else datetime.now(timezone.utc)
            except ValueError:
                ts = datetime.now(timezone.utc)

            # Round to hour
            hour_bucket = ts.replace(minute=0, second=0, microsecond=0)
            key = (code, country, hour_bucket)
            buckets[key]["count"] += 1
            buckets[key]["tone_sum"] += self._safe_float(event.get("AvgTone"))
            buckets[key]["goldstein_sum"] += self._safe_float(event.get("GoldsteinScale"))

        # Prepare rows for ClickHouse
        rows = []
        for (code, country, hour), agg in buckets.items():
            rows.append([
                code,
                country,
                agg["count"],
                agg["tone_sum"] / max(agg["count"], 1),
                agg["goldstein_sum"] / max(agg["count"], 1),
                hour,
            ])

        try:
            ch = Database.clickhouse()
            ch.insert(
                "gdelt_event_volume",
                rows,
                column_names=["event_code", "country", "event_count", "avg_tone", "avg_goldstein", "bucket_hour"],
            )
            ch.close()
            logger.info(f"ClickHouse: inserted {len(rows)} aggregated buckets")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

    def run(self):
        """Full pipeline: fetch latest GDELT → insert PG + ClickHouse."""
        logger.info("━" * 50)
        logger.info("GDELT Downloader — Starting")
        logger.info("━" * 50)

        events = self.fetch_latest()
        if not events:
            logger.warning("No events fetched, exiting")
            return

        logger.info(f"Fetched {len(events)} events")
        self.insert_to_postgres(events)
        self.insert_to_clickhouse(events)
        self.insert_to_neo4j(events)

        logger.info("GDELT Downloader — Complete ✓")

    def insert_to_neo4j(self, events: list[dict]):
        """Insert GDELT events into Neo4j as (:Event)-[:INVOLVES]->(:Country) graph."""
        if not events:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for event in events:
                    global_event_id = self._safe_int(event.get("GlobalEventID"))
                    if not global_event_id:
                        continue

                    event_code = event.get("EventCode", "UNK")
                    description = self._get_event_description(event_code)
                    actor1_country = event.get("Actor1CountryCode", "")
                    actor2_country = event.get("Actor2CountryCode", "")
                    source_url = event.get("SOURCEURL", "")

                    try:
                        # Create Event node
                        session.run(
                            """
                            MERGE (e:Event {global_event_id: $geid})
                            SET e.event_code = $code,
                                e.description = $desc,
                                e.goldstein_scale = $goldstein,
                                e.avg_tone = $tone,
                                e.num_mentions = $mentions,
                                e.source_url = $url
                            """,
                            geid=global_event_id,
                            code=event_code,
                            desc=description,
                            goldstein=self._safe_float(event.get("GoldsteinScale")),
                            tone=self._safe_float(event.get("AvgTone")),
                            mentions=self._safe_int(event.get("NumMentions")),
                            url=source_url,
                        )

                        # Link to Actor1 country
                        if actor1_country:
                            session.run(
                                """
                                MERGE (e:Event {global_event_id: $geid})
                                MERGE (c:Country {code: $country})
                                MERGE (e)-[:INVOLVES {role: 'actor1'}]->(c)
                                """,
                                geid=global_event_id,
                                country=actor1_country[:10],
                            )

                        # Link to Actor2 country
                        if actor2_country and actor2_country != actor1_country:
                            session.run(
                                """
                                MERGE (e:Event {global_event_id: $geid})
                                MERGE (c:Country {code: $country})
                                MERGE (e)-[:INVOLVES {role: 'actor2'}]->(c)
                                """,
                                geid=global_event_id,
                                country=actor2_country[:10],
                            )

                        # Link event to source article if URL exists
                        if source_url:
                            session.run(
                                """
                                MERGE (e:Event {global_event_id: $geid})
                                MERGE (a:Article {url: $url})
                                MERGE (e)-[:SOURCED_FROM]->(a)
                                """,
                                geid=global_event_id,
                                url=source_url,
                            )

                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j skip event {global_event_id}: {e}")

            logger.info(f"Neo4j: created {created} event nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = GDELTDownloader()
    try:
        downloader.run()
    finally:
        downloader.close()
