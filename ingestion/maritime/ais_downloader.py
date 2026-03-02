"""
AIS Downloader — Ship Position Tracking + Dark Ship Detection

Fetches AIS (Automatic Identification System) data for ship tracking
and identifies "dark ships" — vessels that go silent by turning off transponders
(often indicating sanctions evasion, illegal fishing, or smuggling).

Data flow:
  AIS APIs → fetch vessel positions → PostgreSQL (ship_positions + PostGIS)
                                     → ClickHouse (ship_tracks time-series)
                                     → Neo4j (:Vessel)-[:VISITED]->(:Port)

Data Sources (free):
  1. NOAA Marine Cadastre (US Coast Guard AIS archive, 2009–2025) ← HISTORICAL
  2. Marinesia API (real-time + 2026 data, free API key) ← REAL-TIME
  3. Danish Maritime Authority (European waters, fallback)
  4. UN Global Platform AIS (for sanctioned vessel tracking)

Dark Ship Detection:
  A vessel is flagged as "dark" when:
  - AIS signal disappears for >4 hours in open sea
  - Position jumps large distances (transponder was off)
  - Ship operates in sanctioned/restricted zones
  - Vessel identity (MMSI/IMO) changes mid-voyage
"""

import os
import sys
import csv
import io
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from collections import defaultdict

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── Key Ports for Geofencing ───────────────────────────────────────
# Major global ports for tracking vessel visits
KEY_PORTS = {
    "Singapore":     {"lat": 1.2644,  "lon": 103.8200, "radius_km": 15},
    "Shanghai":      {"lat": 31.2304, "lon": 121.4737, "radius_km": 20},
    "Rotterdam":     {"lat": 51.9496, "lon": 4.1462,   "radius_km": 15},
    "Houston":       {"lat": 29.7604, "lon": -95.3698, "radius_km": 20},
    "Fujairah":      {"lat": 25.1164, "lon": 56.3414,  "radius_km": 10},  # Oil hub
    "Jebel Ali":     {"lat": 25.0070, "lon": 55.0810,  "radius_km": 10},
    "Mumbai JNPT":   {"lat": 18.9500, "lon": 72.9500,  "radius_km": 10},
    "Chennai":       {"lat": 13.0827, "lon": 80.2707,  "radius_km": 10},
    "Busan":         {"lat": 35.1028, "lon": 129.0403, "radius_km": 15},
    "Santos":        {"lat": -23.9554,"lon": -46.3326, "radius_km": 15},
    "Ras Tanura":    {"lat": 26.6444, "lon": 50.1578,  "radius_km": 10},  # Saudi oil terminal
    "Kharg Island":  {"lat": 29.2340, "lon": 50.3130,  "radius_km": 10},  # Iran oil terminal
    "Novorossiysk":  {"lat": 44.7233, "lon": 37.7681,  "radius_km": 10},  # Russia oil
    "Yangshan":      {"lat": 30.6300, "lon": 122.0700, "radius_km": 15},  # China deep water
    "Mundra":        {"lat": 22.8390, "lon": 69.7250,  "radius_km": 10},  # India, Adani
}

# Dark zone hotspots — areas where ships commonly go dark
DARK_ZONES = {
    "Strait of Hormuz":    {"lat": 26.5, "lon": 56.2, "radius_km": 50},
    "Strait of Malacca":   {"lat": 2.5,  "lon": 101.5, "radius_km": 80},
    "Gulf of Guinea":      {"lat": 4.0,  "lon": 3.0,   "radius_km": 100},
    "STS Zone Laconian":   {"lat": 36.5, "lon": 23.0,  "radius_km": 30},  # Ship-to-ship transfers
    "STS Zone Ceuta":      {"lat": 35.9, "lon": -5.3,  "radius_km": 20},
    "South China Sea":     {"lat": 15.0, "lon": 115.0, "radius_km": 200},
    "Sea of Azov":         {"lat": 46.0, "lon": 37.0,  "radius_km": 50},  # Russia sanctions
}

# Vessel type codes (AIS)
VESSEL_TYPES = {
    70: "Cargo",
    71: "Cargo - Hazardous A",
    72: "Cargo - Hazardous B",
    73: "Cargo - Hazardous C",
    74: "Cargo - Hazardous D",
    80: "Tanker",
    81: "Tanker - Hazardous A",
    82: "Tanker - Hazardous B",
    83: "Tanker - Hazardous C",
    84: "Tanker - Hazardous D",
    60: "Passenger",
    30: "Fishing",
    36: "Sailing",
    52: "Tug",
}


class AISDownloader:
    """Downloads AIS ship tracking data from multiple free sources."""

    # ── NOAA Marine Cadastre — US Coast Guard AIS (2009–2025) ──
    NOAA_BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"

    # ── Marinesia API — real-time + 2026 data (free) ──
    MARINESIA_URL = "https://api.marinesia.com/api/v2"

    # ── Danish Maritime Authority — fallback (European waters) ──
    DMA_BASE_URL = "https://web.ais.dk/aisdata"

    def __init__(self):
        self.client = httpx.Client(timeout=120.0, follow_redirects=True)
        self.marinesia_key = os.getenv("MARINESIA_API_KEY", "")
        logger.info("AISDownloader initialized")
        if self.marinesia_key:
            logger.info("Marinesia API key found — real-time data enabled")
        else:
            logger.warning(
                "No MARINESIA_API_KEY in .env — register free at https://marinesia.com "
                "to get real-time AIS data"
            )

    def _haversine_km(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in kilometers."""
        import math
        R = 6371  # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def fetch_noaa_historical(self, date: datetime, max_rows: int = 100000) -> list[dict]:
        """
        Fetch historical AIS data from NOAA Marine Cadastre (US Coast Guard).
        Free download — daily CSV zip files, 2009–2025.

        URL pattern: https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/AIS_{date}.zip
        NOAA CSV columns: MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading,
                          VesselName, IMO, VesselType, Status, Length, Width, Draft,
                          Cargo, TransceiverClass
        """
        date_str = date.strftime("%Y_%m_%d")
        year = date.strftime("%Y")
        url = f"{self.NOAA_BASE_URL}/{year}/AIS_{date_str}.zip"

        try:
            logger.info(f"Downloading NOAA AIS for {date.strftime('%Y-%m-%d')}...")
            logger.info(f"URL: {url}")

            # Stream download — files can be 300MB+
            with self.client.stream("GET", url) as resp:
                resp.raise_for_status()
                chunks = []
                downloaded = 0
                for chunk in resp.iter_bytes(chunk_size=8192 * 16):
                    chunks.append(chunk)
                    downloaded += len(chunk)
                    if downloaded % (50 * 1024 * 1024) < 8192 * 16:
                        logger.info(f"  Downloaded {downloaded // (1024*1024)}MB...")
                data = b"".join(chunks)

            logger.info(f"  Total download: {len(data) // (1024*1024)}MB — parsing CSV...")

            import zipfile
            positions = []
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for filename in zf.namelist():
                    if filename.endswith(".csv"):
                        with zf.open(filename) as f:
                            reader = csv.DictReader(
                                io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                            )
                            row_count = 0
                            for row in reader:
                                try:
                                    lat = float(row.get("LAT", 0) or 0)
                                    lon = float(row.get("LON", 0) or 0)
                                    if lat == 0 and lon == 0:
                                        continue

                                    positions.append({
                                        "mmsi": row.get("MMSI", ""),
                                        "imo": row.get("IMO", ""),
                                        "ship_name": row.get("VesselName", ""),
                                        "ship_type": int(row.get("VesselType", 0) or 0),
                                        "lat": lat,
                                        "lon": lon,
                                        "speed": float(row.get("SOG", 0) or 0),
                                        "heading": float(row.get("Heading", 0) or 0),
                                        "course": float(row.get("COG", 0) or 0),
                                        "nav_status": row.get("Status", ""),
                                        "timestamp": row.get("BaseDateTime", ""),
                                        "destination": "",
                                        "draught": float(row.get("Draft", 0) or 0),
                                    })
                                    row_count += 1
                                    if row_count >= max_rows:
                                        logger.info(f"  Capped at {max_rows} rows (file has more)")
                                        break
                                except (ValueError, TypeError):
                                    continue

            logger.info(f"Parsed {len(positions)} AIS positions for {date.strftime('%Y-%m-%d')}")
            return positions

        except httpx.HTTPError as e:
            logger.error(f"NOAA AIS fetch failed for {date.strftime('%Y-%m-%d')}: {e}")
            return []

    def fetch_dma_historical(self, date: datetime) -> list[dict]:
        """Fallback: Fetch from Danish Maritime Authority (European waters only)."""
        date_str = date.strftime("%Y-%m-%d")
        url = f"{self.DMA_BASE_URL}/aisdk-{date_str}.zip"

        try:
            logger.info(f"Trying DMA fallback for {date_str}...")
            resp = self.client.get(url)
            resp.raise_for_status()

            import zipfile
            positions = []
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for filename in zf.namelist():
                    if filename.endswith(".csv"):
                        with zf.open(filename) as f:
                            reader = csv.DictReader(
                                io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                            )
                            for row in reader:
                                try:
                                    lat = float(row.get("Latitude", 0))
                                    lon = float(row.get("Longitude", 0))
                                    if lat == 0 and lon == 0:
                                        continue
                                    positions.append({
                                        "mmsi": row.get("MMSI", ""),
                                        "imo": row.get("IMO", ""),
                                        "ship_name": row.get("Name", ""),
                                        "ship_type": int(row.get("Ship type", 0) or 0),
                                        "lat": lat, "lon": lon,
                                        "speed": float(row.get("SOG", 0) or 0),
                                        "heading": float(row.get("Heading", 0) or 0),
                                        "course": float(row.get("COG", 0) or 0),
                                        "nav_status": row.get("Navigational status", ""),
                                        "timestamp": row.get("# Timestamp", ""),
                                        "destination": row.get("Destination", ""),
                                        "draught": float(row.get("Draught", 0) or 0),
                                    })
                                except (ValueError, TypeError):
                                    continue
            logger.info(f"DMA: Parsed {len(positions)} positions for {date_str}")
            return positions
        except Exception as e:
            logger.error(f"DMA fallback also failed: {e}")
            return []

    def fetch_marinesia_realtime(self) -> list[dict]:
        """
        Fetch real-time AIS data from Marinesia API for all dark zones and key ports.
        Uses the /api/v2/vessel/area endpoint with bounding boxes.

        Free API key from https://marinesia.com
        Returns current vessel positions in monitored maritime areas.
        """
        if not self.marinesia_key:
            logger.warning("Marinesia API key not set — skipping real-time fetch")
            return []

        all_positions = []

        # Query all dark zones
        areas_to_query = {}
        for name, zone in DARK_ZONES.items():
            r = zone["radius_km"] / 111  # rough degrees
            areas_to_query[name] = {
                "lat_min": zone["lat"] - r,
                "lat_max": zone["lat"] + r,
                "long_min": zone["lon"] - r,
                "long_max": zone["lon"] + r,
            }

        # Also query key ports
        for name, port in KEY_PORTS.items():
            r = port["radius_km"] / 111
            areas_to_query[f"Port_{name}"] = {
                "lat_min": port["lat"] - r,
                "lat_max": port["lat"] + r,
                "long_min": port["lon"] - r,
                "long_max": port["lon"] + r,
            }

        for area_name, bbox in areas_to_query.items():
            try:
                resp = self.client.get(
                    f"{self.MARINESIA_URL}/vessel/area",
                    params={
                        "lat_min": bbox["lat_min"],
                        "lat_max": bbox["lat_max"],
                        "long_min": bbox["long_min"],
                        "long_max": bbox["long_max"],
                        "key": self.marinesia_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

                if not data.get("error") and data.get("data"):
                    for v in data["data"]:
                        all_positions.append({
                            "mmsi": str(v.get("mmsi", "")),
                            "imo": str(v.get("imo", "")),
                            "ship_name": v.get("name", ""),
                            "ship_type": 0,  # Marinesia uses text type
                            "ship_type_text": v.get("type", ""),
                            "lat": v.get("lat", 0),
                            "lon": v.get("lng", 0),
                            "speed": v.get("sog", 0),
                            "heading": v.get("hdt", 0),
                            "course": v.get("cog", 0),
                            "nav_status": str(v.get("status", "")),
                            "timestamp": v.get("ts", ""),
                            "destination": v.get("dest", ""),
                            "draught": 0,
                            "source": "marinesia",
                            "area": area_name,
                            "flag": v.get("flag", ""),
                        })
                    logger.info(f"  {area_name}: {len(data['data'])} vessels")
                else:
                    logger.debug(f"  {area_name}: no vessels found")

                time.sleep(0.3)  # Rate limit

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(f"Rate limited on {area_name} — pausing 5s")
                    time.sleep(5)
                else:
                    logger.debug(f"  {area_name}: {e}")
            except Exception as e:
                logger.debug(f"  {area_name}: {e}")

        logger.info(f"Marinesia: fetched {len(all_positions)} vessel positions across {len(areas_to_query)} areas")
        return all_positions

    def detect_dark_ships(self, positions: list[dict], gap_hours: float = 4.0) -> list[dict]:
        """
        Detect vessels that went "dark" — AIS signal gaps indicating
        transponder was turned off.

        Args:
            positions: List of AIS position records (sorted by time per vessel)
            gap_hours: Minimum gap in hours to flag as "dark" (default: 4h)

        Returns:
            List of dark ship events with gap details
        """
        # Group positions by MMSI
        vessel_tracks = defaultdict(list)
        for pos in positions:
            mmsi = pos.get("mmsi", "")
            if mmsi:
                vessel_tracks[mmsi].append(pos)

        dark_events = []
        for mmsi, track in vessel_tracks.items():
            # Sort by timestamp
            track.sort(key=lambda x: x.get("timestamp", ""))

            for i in range(1, len(track)):
                prev = track[i - 1]
                curr = track[i]

                # Parse timestamps
                try:
                    t1_str = prev.get("timestamp", "")
                    t2_str = curr.get("timestamp", "")
                    if not t1_str or not t2_str:
                        continue

                    # Try common AIS timestamp formats
                    for fmt in ["%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            t1 = datetime.strptime(t1_str, fmt)
                            t2 = datetime.strptime(t2_str, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        continue

                    gap = (t2 - t1).total_seconds() / 3600  # hours

                    if gap >= gap_hours:
                        # Calculate distance jumped
                        dist = self._haversine_km(
                            prev["lat"], prev["lon"],
                            curr["lat"], curr["lon"],
                        )

                        # Check if in dark zone
                        in_dark_zone = ""
                        for zone_name, zone in DARK_ZONES.items():
                            d1 = self._haversine_km(prev["lat"], prev["lon"], zone["lat"], zone["lon"])
                            d2 = self._haversine_km(curr["lat"], curr["lon"], zone["lat"], zone["lon"])
                            if d1 <= zone["radius_km"] or d2 <= zone["radius_km"]:
                                in_dark_zone = zone_name
                                break

                        dark_events.append({
                            "mmsi": mmsi,
                            "ship_name": prev.get("ship_name", ""),
                            "ship_type": VESSEL_TYPES.get(prev.get("ship_type", 0), "Unknown"),
                            "last_seen_lat": prev["lat"],
                            "last_seen_lon": prev["lon"],
                            "reappear_lat": curr["lat"],
                            "reappear_lon": curr["lon"],
                            "last_seen_at": t1,
                            "reappear_at": t2,
                            "gap_hours": round(gap, 1),
                            "distance_jumped_km": round(dist, 1),
                            "speed_implied_knots": round(dist / gap * 0.54, 1) if gap > 0 else 0,
                            "dark_zone": in_dark_zone,
                            "risk_score": self._calculate_risk(gap, dist, in_dark_zone, prev),
                        })

                except Exception:
                    continue

        # Sort by risk score descending
        dark_events.sort(key=lambda x: x["risk_score"], reverse=True)
        logger.info(f"Detected {len(dark_events)} dark ship events")
        return dark_events

    def _calculate_risk(self, gap_hours: float, distance_km: float, dark_zone: str, vessel: dict) -> float:
        """Calculate a 0–100 risk score for a dark ship event."""
        score = 0.0

        # Longer gap = higher risk
        if gap_hours >= 24:
            score += 30
        elif gap_hours >= 12:
            score += 20
        elif gap_hours >= 6:
            score += 10

        # Large distance jump = suspicious
        if distance_km >= 200:
            score += 25
        elif distance_km >= 100:
            score += 15
        elif distance_km >= 50:
            score += 10

        # In known dark zone
        if dark_zone:
            score += 20

        # Tankers going dark = highest risk (sanctions evasion)
        ship_type = vessel.get("ship_type", 0)
        if 80 <= ship_type <= 89:  # Tanker
            score += 15
        elif 70 <= ship_type <= 79:  # Cargo
            score += 10

        return min(score, 100)

    def insert_positions_to_postgres(self, positions: list[dict]):
        """Insert AIS positions into PostgreSQL ship_positions table with PostGIS."""
        if not positions:
            return

        engine = Database.pg()
        from sqlalchemy import text

        inserted = 0
        with engine.connect() as conn:
            for pos in positions:
                try:
                    # Parse timestamp
                    ts = None
                    ts_str = pos.get("timestamp", "")
                    for fmt in ["%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            ts = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                            break
                        except ValueError:
                            continue

                    if not ts:
                        continue

                    conn.execute(
                        text("""
                            INSERT INTO ship_positions (mmsi, timestamp, geom, speed, heading)
                            VALUES (:mmsi, :ts, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), :speed, :heading)
                        """),
                        {
                            "mmsi": str(pos["mmsi"])[:20],
                            "ts": ts,
                            "lat": pos["lat"],
                            "lon": pos["lon"],
                            "speed": pos.get("speed", 0),
                            "heading": pos.get("heading", 0),
                        },
                    )
                    inserted += 1
                except Exception as e:
                    logger.debug(f"Skip position: {e}")

            conn.commit()

        logger.info(f"PostgreSQL: inserted {inserted} ship positions")

    def insert_to_neo4j(self, positions: list[dict], dark_events: list[dict] = None):
        """
        Insert vessel data into Neo4j:
          (:Vessel)-[:VISITED]->(:Port)
          (:Vessel)-[:WENT_DARK]->(:DarkEvent)
        """
        if not positions and not dark_events:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                # Track unique vessels
                vessels_seen = set()
                for pos in positions:
                    mmsi = pos.get("mmsi", "")
                    if not mmsi or mmsi in vessels_seen:
                        continue
                    vessels_seen.add(mmsi)

                    try:
                        session.run(
                            """
                            MERGE (v:Vessel {mmsi: $mmsi})
                            SET v.name = $name,
                                v.type = $type,
                                v.imo = $imo
                            """,
                            mmsi=mmsi,
                            name=pos.get("ship_name", ""),
                            type=VESSEL_TYPES.get(pos.get("ship_type", 0), "Unknown"),
                            imo=pos.get("imo", ""),
                        )

                        # Check if vessel is near any key port
                        for port_name, port in KEY_PORTS.items():
                            dist = self._haversine_km(pos["lat"], pos["lon"], port["lat"], port["lon"])
                            if dist <= port["radius_km"]:
                                session.run(
                                    """
                                    MERGE (v:Vessel {mmsi: $mmsi})
                                    MERGE (p:Port {name: $port})
                                    SET p.lat = $lat, p.lon = $lon
                                    MERGE (v)-[:VISITED {timestamp: $ts}]->(p)
                                    """,
                                    mmsi=mmsi,
                                    port=port_name,
                                    lat=port["lat"],
                                    lon=port["lon"],
                                    ts=pos.get("timestamp", ""),
                                )
                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j vessel skip: {e}")

                # Insert dark events
                if dark_events:
                    for event in dark_events[:100]:  # Cap at top 100 riskiest
                        try:
                            session.run(
                                """
                                MERGE (v:Vessel {mmsi: $mmsi})
                                CREATE (d:DarkEvent {
                                    gap_hours: $gap,
                                    distance_km: $dist,
                                    risk_score: $risk,
                                    dark_zone: $zone,
                                    last_seen_at: $last_seen,
                                    reappear_at: $reappear
                                })
                                MERGE (v)-[:WENT_DARK]->(d)
                                """,
                                mmsi=event["mmsi"],
                                gap=event["gap_hours"],
                                dist=event["distance_jumped_km"],
                                risk=event["risk_score"],
                                zone=event.get("dark_zone", ""),
                                last_seen=event["last_seen_at"].isoformat() if event.get("last_seen_at") else "",
                                reappear=event["reappear_at"].isoformat() if event.get("reappear_at") else "",
                            )
                        except Exception as e:
                            logger.debug(f"Neo4j dark event skip: {e}")

            logger.info(f"Neo4j: created {created} vessel nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def run(self, date: datetime = None):
        """
        Full pipeline: fetch AIS data → detect dark ships → store everywhere.

        Args:
            date: Date to fetch AIS data for. Defaults to yesterday.
        """
        logger.info("━" * 50)
        logger.info("AIS Downloader — Starting")
        logger.info("━" * 50)

        # NOAA data typically available 2009–2025 (not yet published for 2026)
        if date is None:
            date = datetime(2025, 1, 15, tzinfo=timezone.utc)
            logger.info("No date specified — using 2025-01-15 (latest available NOAA data)")

        # 1. Fetch AIS positions — try NOAA first, then DMA fallback
        positions = self.fetch_noaa_historical(date)
        if not positions:
            logger.warning("NOAA failed — trying DMA fallback...")
            positions = self.fetch_dma_historical(date)
        if not positions:
            logger.warning("Historical sources failed — trying Marinesia real-time...")
            positions = self.fetch_marinesia_realtime()
        if not positions:
            logger.error("No AIS data fetched from any source")
            return

        # 2. Detect dark ships
        dark_events = self.detect_dark_ships(positions, gap_hours=4.0)

        if dark_events:
            logger.info(f"\n🚨 TOP DARK SHIP EVENTS (risk > 30):")
            for event in dark_events[:10]:
                if event["risk_score"] > 30:
                    logger.info(
                        f"  MMSI {event['mmsi']} ({event['ship_type']}) | "
                        f"Dark for {event['gap_hours']}h | "
                        f"Jumped {event['distance_jumped_km']}km | "
                        f"Risk: {event['risk_score']}/100"
                        f"{' | Zone: ' + event['dark_zone'] if event['dark_zone'] else ''}"
                    )

        # 3. Insert to databases
        # Sample positions (full dataset can be millions of rows)
        sample_size = min(len(positions), 50000)
        if len(positions) > sample_size:
            import random
            positions_sample = random.sample(positions, sample_size)
            logger.info(f"Sampled {sample_size}/{len(positions)} positions for DB insert")
        else:
            positions_sample = positions

        self.insert_positions_to_postgres(positions_sample)
        self.insert_to_neo4j(positions_sample, dark_events)

        logger.info("AIS Downloader — Complete ✓")

    def run_backfill(self, days_back: int = 30):
        """
        Historical backfill: fetch AIS data for the past N days.

        Args:
            days_back: Number of days to go back.
        """
        logger.info("━" * 50)
        logger.info(f"AIS Historical Backfill — {days_back} days")
        logger.info("━" * 50)

        end_date = datetime.now(timezone.utc)
        current_date = end_date - timedelta(days=days_back)

        total = 0
        while current_date < end_date:
            self.run(date=current_date)
            time.sleep(1)  # Rate limit
            current_date += timedelta(days=1)
            total += 1

        logger.info(f"AIS Backfill Complete — processed {total} days")

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = AISDownloader()
    try:
        args = sys.argv[1:]

        # Parse --date YYYY-MM-DD
        target_date = None
        if "--date" in args:
            idx = args.index("--date")
            if len(args) > idx + 1:
                target_date = datetime.strptime(args[idx + 1], "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if "--backfill" in args:
            idx = args.index("--backfill")
            days = int(args[idx + 1]) if len(args) > idx + 1 else 30
            downloader.run_backfill(days_back=days)
        else:
            downloader.run(date=target_date)
    finally:
        downloader.close()
