"""
Sentinel-1 SAR Satellite Downloader — Ship Detection Imagery

Downloads Sentinel-1 Synthetic Aperture Radar (SAR) imagery from ESA's
Copernicus Data Space Ecosystem for maritime ship detection.

Why SAR for dark ships:
  - SAR works day/night, through clouds — critical for maritime surveillance
  - 10m resolution can detect vessels >25m length
  - Ships appear as bright spots against dark ocean background
  - Compare satellite-detected ships vs AIS positions → unmatched = dark ship

Data flow:
  Copernicus CDSE → search product catalog → download GRD images
    → PostgreSQL (satellite_scenes metadata)
    → Local filesystem (SAR .tiff/.zip files for ML pipeline)

Products used:
  - Sentinel-1 GRD (Ground Range Detected) — best for ship detection
  - Mode: IW (Interferometric Wide) — 250km swath, 10m resolution
  - Polarization: VV (best for ship detection on water)

Requires in .env:
  COPERNICUS_USER     — free account at dataspace.copernicus.eu
  COPERNICUS_PASSWORD — password for that account

Install: pip install rasterio sentinelsat (optional, for advanced processing)
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database


# ── Maritime Regions of Interest for Ship Detection ────────────────
# Bounding boxes [west, south, east, north] covering dark ship hotspots
MARITIME_AOIS = {
    "Strait_of_Hormuz": {
        "bbox": [54.0, 25.0, 58.0, 27.5],
        "desc": "Iran-UAE oil chokepoint — highest dark ship activity",
    },
    "Strait_of_Malacca": {
        "bbox": [99.0, 1.0, 104.5, 4.0],
        "desc": "Singapore-Malaysia — busiest shipping lane globally",
    },
    "Gulf_of_Guinea": {
        "bbox": [-2.0, 1.0, 8.0, 6.0],
        "desc": "Nigeria-Ghana — piracy and illegal oil bunkering",
    },
    "South_China_Sea": {
        "bbox": [110.0, 8.0, 120.0, 18.0],
        "desc": "Contested waters — military and fishing fleet tracking",
    },
    "Laconian_Gulf_STS": {
        "bbox": [22.0, 36.0, 24.0, 37.5],
        "desc": "Greece — ship-to-ship oil transfers for sanctions evasion",
    },
    "Sea_of_Azov": {
        "bbox": [35.0, 45.0, 40.0, 47.5],
        "desc": "Russia-Ukraine — sanctions monitoring",
    },
    "Arabian_Sea_West": {
        "bbox": [55.0, 20.0, 65.0, 25.0],
        "desc": "Oman-Pakistan — Iranian oil smuggling route",
    },
    "Mumbai_JNPT": {
        "bbox": [72.0, 18.0, 73.5, 19.5],
        "desc": "India — Mumbai/JNPT port area",
    },
    "Suez_Canal": {
        "bbox": [32.0, 29.5, 33.5, 31.5],
        "desc": "Egypt — critical global shipping chokepoint",
    },
    "English_Channel": {
        "bbox": [-2.0, 49.5, 2.0, 51.5],
        "desc": "UK-France — one of busiest shipping lanes in Europe",
    },
}


class SentinelDownloader:
    """Downloads Sentinel-1 SAR imagery from Copernicus Data Space for ship detection."""

    # Copernicus Data Space Ecosystem (CDSE) endpoints
    AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    DOWNLOAD_BASE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

    # Where to store downloaded SAR images
    DATA_DIR = Path(os.getenv("SAR_DATA_DIR", "data/sentinel"))

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.username = username or os.getenv("COPERNICUS_USER", "")
        self.password = password or os.getenv("COPERNICUS_PASSWORD", "")
        self.access_token = None
        self.token_expiry = None

        self.client = httpx.Client(timeout=120.0, follow_redirects=True)

        # Create data directories
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        (self.DATA_DIR / "raw").mkdir(exist_ok=True)       # Raw .zip SAR products
        (self.DATA_DIR / "metadata").mkdir(exist_ok=True)   # JSON metadata

        if not self.username or not self.password:
            logger.warning(
                "Copernicus credentials not set — register free at "
                "https://dataspace.copernicus.eu then set COPERNICUS_USER and "
                "COPERNICUS_PASSWORD in .env"
            )

        logger.info("SentinelDownloader initialized")
        logger.info(f"Data directory: {self.DATA_DIR.resolve()}")

    def authenticate(self) -> bool:
        """Get OAuth2 access token from Copernicus Data Space."""
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            return True  # Token still valid

        try:
            resp = self.client.post(
                self.AUTH_URL,
                data={
                    "client_id": "cdse-public",
                    "grant_type": "password",
                    "username": self.username,
                    "password": self.password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            resp.raise_for_status()
            data = resp.json()

            self.access_token = data["access_token"]
            # Token typically valid for 600 seconds (10 min)
            self.token_expiry = datetime.now() + timedelta(seconds=data.get("expires_in", 600) - 30)

            logger.info("Copernicus authentication successful")
            return True

        except Exception as e:
            logger.error(f"Copernicus authentication failed: {e}")
            return False

    def search_scenes(
        self,
        aoi_name: str = "Strait_of_Hormuz",
        days_back: int = 7,
        max_results: int = 20,
        custom_bbox: list[float] = None,
    ) -> list[dict]:
        """
        Search Copernicus catalog for Sentinel-1 GRD products over a maritime area.

        Args:
            aoi_name: Key from MARITIME_AOIS dict.
            days_back: How many days back to search.
            max_results: Max number of products to return.
            custom_bbox: Optional [west, south, east, north] to override aoi_name.

        Returns:
            List of product metadata dicts.
        """
        if custom_bbox:
            bbox = custom_bbox
        elif aoi_name in MARITIME_AOIS:
            bbox = MARITIME_AOIS[aoi_name]["bbox"]
        else:
            logger.error(f"Unknown AOI: {aoi_name}. Choose from: {list(MARITIME_AOIS.keys())}")
            return []

        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)

        # Build OData filter for Sentinel-1 GRD
        # WKT polygon from bbox
        w, s, e, n = bbox
        footprint = f"POLYGON(({w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))"

        odata_filter = (
            f"Collection/Name eq 'SENTINEL-1' "
            f"and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
            f"and att/OData.CSC.StringAttribute/Value eq 'GRD') "
            f"and OData.CSC.Intersects(area=geography'SRID=4326;{footprint}') "
            f"and ContentDate/Start gt {start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')} "
            f"and ContentDate/Start lt {end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        )

        try:
            resp = self.client.get(
                self.CATALOG_URL,
                params={
                    "$filter": odata_filter,
                    "$orderby": "ContentDate/Start desc",
                    "$top": max_results,
                    "$expand": "Attributes",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            products = []
            for item in data.get("value", []):
                # Extract key attributes
                attrs = {}
                for attr in item.get("Attributes", []):
                    attrs[attr.get("Name", "")] = attr.get("Value", "")

                product = {
                    "product_id": item.get("Id", ""),
                    "name": item.get("Name", ""),
                    "satellite": "Sentinel-1",
                    "product_type": attrs.get("productType", "GRD"),
                    "mode": attrs.get("operationalMode", "IW"),
                    "polarization": attrs.get("polarisationChannels", "VV+VH"),
                    "orbit_direction": attrs.get("orbitDirection", ""),
                    "start_time": item.get("ContentDate", {}).get("Start", ""),
                    "end_time": item.get("ContentDate", {}).get("End", ""),
                    "footprint": item.get("GeoFootprint", {}).get("coordinates", []),
                    "size_mb": round(item.get("ContentLength", 0) / 1e6, 1),
                    "aoi": aoi_name,
                    "bbox": bbox,
                    "online": item.get("Online", True),
                }
                products.append(product)

            logger.info(
                f"Found {len(products)} Sentinel-1 GRD scenes for {aoi_name} "
                f"({start_date.date()} → {end_date.date()})"
            )
            return products

        except Exception as e:
            logger.error(f"Catalog search failed: {e}")
            return []

    def search_all_aois(self, days_back: int = 7, max_per_aoi: int = 5) -> list[dict]:
        """Search all maritime AOIs for recent Sentinel-1 imagery."""
        all_products = []
        for aoi_name in MARITIME_AOIS:
            products = self.search_scenes(
                aoi_name=aoi_name,
                days_back=days_back,
                max_results=max_per_aoi,
            )
            all_products.extend(products)
            time.sleep(0.5)  # Rate limit

        logger.info(f"Total scenes across all AOIs: {len(all_products)}")
        return all_products

    def download_product(self, product: dict, skip_if_exists: bool = True) -> Optional[Path]:
        """
        Download a Sentinel-1 product (GRD .zip file).

        Args:
            product: Product dict from search_scenes().
            skip_if_exists: Skip download if file already exists locally.

        Returns:
            Path to downloaded file, or None if failed.
        """
        product_id = product["product_id"]
        filename = f"{product['name']}.zip"
        filepath = self.DATA_DIR / "raw" / filename

        if skip_if_exists and filepath.exists():
            logger.info(f"Already downloaded: {filename}")
            return filepath

        if not self.authenticate():
            return None

        download_url = f"{self.DOWNLOAD_BASE}({product_id})/$value"

        try:
            logger.info(f"Downloading {filename} ({product['size_mb']}MB)...")

            with self.client.stream(
                "GET",
                download_url,
                headers={"Authorization": f"Bearer {self.access_token}"},
            ) as resp:
                resp.raise_for_status()

                total = int(resp.headers.get("content-length", 0))
                downloaded = 0

                with open(filepath, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=8192 * 16):
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Progress logging every 50MB
                        if total and downloaded % (50 * 1024 * 1024) < 8192 * 16:
                            pct = round(downloaded / total * 100)
                            logger.info(f"  {pct}% ({downloaded // 1e6:.0f}MB / {total // 1e6:.0f}MB)")

            logger.info(f"Downloaded: {filepath}")

            # Save metadata JSON alongside
            meta_path = self.DATA_DIR / "metadata" / f"{product['name']}.json"
            with open(meta_path, "w") as f:
                json.dump(product, f, indent=2, default=str)

            return filepath

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.warning("Token expired, re-authenticating...")
                self.access_token = None
                return self.download_product(product, skip_if_exists)
            logger.error(f"Download failed ({e.response.status_code}): {e}")
            return None
        except Exception as e:
            logger.error(f"Download failed: {e}")
            # Clean up partial file
            if filepath.exists():
                filepath.unlink()
            return None

    def insert_scenes_to_postgres(self, products: list[dict]):
        """Insert scene metadata into PostgreSQL for tracking and cross-referencing."""
        if not products:
            return

        engine = Database.pg()
        from sqlalchemy import text

        # Create table if not exists
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS satellite_scenes (
                    id SERIAL PRIMARY KEY,
                    product_id VARCHAR(255) UNIQUE,
                    name VARCHAR(500),
                    satellite VARCHAR(50),
                    product_type VARCHAR(20),
                    mode VARCHAR(10),
                    polarization VARCHAR(20),
                    orbit_direction VARCHAR(20),
                    start_time TIMESTAMPTZ,
                    end_time TIMESTAMPTZ,
                    aoi VARCHAR(100),
                    bbox_west FLOAT,
                    bbox_south FLOAT,
                    bbox_east FLOAT,
                    bbox_north FLOAT,
                    size_mb FLOAT,
                    downloaded BOOLEAN DEFAULT FALSE,
                    local_path TEXT,
                    ships_detected INTEGER,
                    dark_ships_detected INTEGER,
                    processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """))
            conn.commit()

        inserted = 0
        with engine.connect() as conn:
            for p in products:
                try:
                    bbox = p.get("bbox", [0, 0, 0, 0])
                    start_time = None
                    if p.get("start_time"):
                        try:
                            start_time = datetime.fromisoformat(
                                p["start_time"].replace("Z", "+00:00")
                            )
                        except (ValueError, AttributeError):
                            pass

                    end_time = None
                    if p.get("end_time"):
                        try:
                            end_time = datetime.fromisoformat(
                                p["end_time"].replace("Z", "+00:00")
                            )
                        except (ValueError, AttributeError):
                            pass

                    conn.execute(
                        text("""
                            INSERT INTO satellite_scenes
                                (product_id, name, satellite, product_type, mode,
                                 polarization, orbit_direction, start_time, end_time,
                                 aoi, bbox_west, bbox_south, bbox_east, bbox_north, size_mb)
                            VALUES
                                (:pid, :name, :sat, :ptype, :mode,
                                 :pol, :orbit, :start, :end,
                                 :aoi, :w, :s, :e, :n, :size)
                            ON CONFLICT (product_id) DO NOTHING
                        """),
                        {
                            "pid": p["product_id"],
                            "name": p["name"][:500],
                            "sat": "Sentinel-1",
                            "ptype": p.get("product_type", "GRD"),
                            "mode": p.get("mode", "IW"),
                            "pol": p.get("polarization", ""),
                            "orbit": p.get("orbit_direction", ""),
                            "start": start_time,
                            "end": end_time,
                            "aoi": p.get("aoi", ""),
                            "w": bbox[0] if len(bbox) > 0 else 0,
                            "s": bbox[1] if len(bbox) > 1 else 0,
                            "e": bbox[2] if len(bbox) > 2 else 0,
                            "n": bbox[3] if len(bbox) > 3 else 0,
                            "size": p.get("size_mb", 0),
                        },
                    )
                    inserted += 1
                except Exception as e:
                    logger.debug(f"Skip scene insert: {e}")

            conn.commit()

        logger.info(f"PostgreSQL: inserted {inserted} satellite scene records")

    def insert_to_neo4j(self, products: list[dict]):
        """
        Insert satellite scene data into Neo4j graph:
          (:SatelliteScene)-[:COVERS]->(:MaritimeRegion)
        """
        if not products:
            return

        driver = Database.neo4j()
        created = 0

        try:
            with driver.session(database="atlas") as session:
                for p in products:
                    try:
                        aoi = p.get("aoi", "Unknown")
                        session.run(
                            """
                            MERGE (s:SatelliteScene {product_id: $pid})
                            SET s.name = $name,
                                s.satellite = 'Sentinel-1',
                                s.product_type = $ptype,
                                s.start_time = $start,
                                s.size_mb = $size

                            MERGE (r:MaritimeRegion {name: $region})
                            SET r.description = $desc

                            MERGE (s)-[:COVERS]->(r)
                            """,
                            pid=p["product_id"],
                            name=p["name"],
                            ptype=p.get("product_type", "GRD"),
                            start=p.get("start_time", ""),
                            size=p.get("size_mb", 0),
                            region=aoi,
                            desc=MARITIME_AOIS.get(aoi, {}).get("desc", ""),
                        )
                        created += 1
                    except Exception as e:
                        logger.debug(f"Neo4j scene skip: {e}")

            logger.info(f"Neo4j: created {created} satellite scene nodes")
        except Exception as e:
            logger.error(f"Neo4j insertion failed: {e}")
        finally:
            driver.close()

    def run(
        self,
        aois: list[str] = None,
        days_back: int = 7,
        max_per_aoi: int = 5,
        download: bool = False,
    ):
        """
        Full pipeline: search catalog → store metadata → optionally download imagery.

        Args:
            aois: List of AOI names to search. None = all AOIs.
            days_back: How many days back to search.
            max_per_aoi: Max products per AOI.
            download: Whether to actually download the SAR .zip files
                      (can be large, 500MB-1GB each).
        """
        logger.info("━" * 50)
        logger.info("Sentinel-1 SAR Downloader — Starting")
        logger.info("━" * 50)

        if not self.username:
            logger.error(
                "No credentials. Register free at https://dataspace.copernicus.eu "
                "then set COPERNICUS_USER and COPERNICUS_PASSWORD in .env"
            )
            return

        # Search for scenes
        all_products = []
        aoi_list = aois or list(MARITIME_AOIS.keys())

        for aoi_name in aoi_list:
            products = self.search_scenes(
                aoi_name=aoi_name,
                days_back=days_back,
                max_results=max_per_aoi,
            )
            all_products.extend(products)
            time.sleep(0.3)

        if not all_products:
            logger.warning("No Sentinel-1 scenes found")
            return

        # Log summary
        total_size = sum(p.get("size_mb", 0) for p in all_products)
        logger.info(f"\nFound {len(all_products)} scenes, total ~{total_size:.0f}MB")
        for aoi in aoi_list:
            aoi_count = sum(1 for p in all_products if p.get("aoi") == aoi)
            if aoi_count > 0:
                logger.info(f"  {aoi}: {aoi_count} scenes")

        # Store metadata
        self.insert_scenes_to_postgres(all_products)
        self.insert_to_neo4j(all_products)

        # Download actual imagery if requested
        if download:
            logger.info(f"\nDownloading {len(all_products)} SAR products...")
            downloaded = 0
            for product in all_products:
                path = self.download_product(product)
                if path:
                    downloaded += 1
                    # Update PostgreSQL record
                    try:
                        engine = Database.pg()
                        from sqlalchemy import text
                        with engine.connect() as conn:
                            conn.execute(
                                text("""
                                    UPDATE satellite_scenes
                                    SET downloaded = TRUE, local_path = :path
                                    WHERE product_id = :pid
                                """),
                                {"path": str(path), "pid": product["product_id"]},
                            )
                            conn.commit()
                    except Exception:
                        pass
                time.sleep(1)  # Rate limit downloads

            logger.info(f"Downloaded {downloaded}/{len(all_products)} products")
        else:
            logger.info(
                "\nMetadata saved. To download actual SAR images, run with --download flag."
                f"\n  ⚠️  Total download size: ~{total_size:.0f}MB"
            )

        logger.info("Sentinel-1 SAR Downloader — Complete ✓")

    def run_backfill(self, days_back: int = 365, max_per_aoi: int = 10, download: bool = False):
        """
        Historical backfill: search catalog for older scenes.

        Args:
            days_back: How many days back to search.
            max_per_aoi: Max products per AOI.
            download: Whether to download .zip files.
        """
        logger.info("━" * 50)
        logger.info(f"Sentinel-1 Historical Backfill — {days_back} days")
        logger.info("━" * 50)

        self.run(
            days_back=days_back,
            max_per_aoi=max_per_aoi,
            download=download,
        )

    def close(self):
        self.client.close()


# ── CLI Entry Point ─────────────────────────────────────────────────
if __name__ == "__main__":
    downloader = SentinelDownloader()
    try:
        args = sys.argv[1:]

        download = "--download" in args
        days = 7

        if "--backfill" in args:
            idx = args.index("--backfill")
            days = int(args[idx + 1]) if len(args) > idx + 1 else 365
            downloader.run_backfill(days_back=days, download=download)
        else:
            if "--days" in args:
                idx = args.index("--days")
                days = int(args[idx + 1]) if len(args) > idx + 1 else 7

            # Filter specific AOIs if provided
            aois = [a for a in args if a in MARITIME_AOIS]
            aois = aois or None

            downloader.run(aois=aois, days_back=days, download=download)
    finally:
        downloader.close()
