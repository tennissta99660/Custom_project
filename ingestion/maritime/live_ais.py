"""
Live AIS Tracking — Real-time ship position monitoring

Sources:
  1. AISStream.io — WebSocket stream for global real-time AIS
  2. Marinesia API — REST polling for dark zones and key ports

Runs continuously, inserting live vessel positions into the databases.

Usage:
    python ingestion/maritime/live_ais.py

Requires in .env:
    AISSTREAM_API_KEY  — free from https://aisstream.io
    MARINESIA_API_KEY  — free from https://marinesia.com
"""

import os
import sys
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict

import httpx
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db.connections import Database

# Import dark zones and ports from the main AIS module
from ingestion.maritime.ais_downloader import (
    KEY_PORTS, DARK_ZONES, VESSEL_TYPES, AISDownloader,
)


def run_aisstream(api_key: str):
    """
    Connect to AISStream.io WebSocket for real-time global AIS data.
    Filters for vessels in dark zones and key shipping lanes.
    """
    try:
        import websockets
        import asyncio
    except ImportError:
        logger.error("Install websockets: pip install websockets")
        return

    # Build bounding boxes for AISStream subscription
    bboxes = []
    for zone in DARK_ZONES.values():
        r = zone["radius_km"] / 111
        bboxes.append([
            [zone["lat"] - r, zone["lon"] - r],
            [zone["lat"] + r, zone["lon"] + r],
        ])
    for port in KEY_PORTS.values():
        r = port["radius_km"] / 111
        bboxes.append([
            [port["lat"] - r, port["lon"] - r],
            [port["lat"] + r, port["lon"] + r],
        ])

    subscribe_msg = {
        "APIKey": api_key,
        "BoundingBoxes": bboxes,
        "FilterMessageTypes": ["PositionReport"],
    }

    async def stream():
        batch = []
        batch_size = 100
        downloader = AISDownloader()

        while True:
            try:
                async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info("[AISStream] Connected — streaming live positions")

                    async for raw_msg in ws:
                        try:
                            msg = json.loads(raw_msg)
                            meta = msg.get("MetaData", {})
                            pos = msg.get("Message", {}).get("PositionReport", {})

                            if not pos:
                                continue

                            batch.append({
                                "mmsi": str(meta.get("MMSI", "")),
                                "imo": "",
                                "ship_name": meta.get("ShipName", ""),
                                "ship_type": int(meta.get("ShipType", 0) or 0),
                                "lat": pos.get("Latitude", 0),
                                "lon": pos.get("Longitude", 0),
                                "speed": pos.get("Sog", 0),
                                "heading": pos.get("TrueHeading", 0),
                                "course": pos.get("Cog", 0),
                                "nav_status": str(pos.get("NavigationalStatus", "")),
                                "timestamp": meta.get("time_utc", datetime.now(timezone.utc).isoformat()),
                                "destination": "",
                                "draught": 0,
                                "source": "aisstream",
                            })

                            if len(batch) >= batch_size:
                                logger.info(f"[AISStream] Batch of {len(batch)} — inserting to DB")
                                downloader.insert_positions_to_postgres(batch)
                                downloader.insert_to_neo4j(batch)
                                batch = []

                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.debug(f"[AISStream] Parse error: {e}")

            except Exception as e:
                logger.error(f"[AISStream] Connection lost: {e} — reconnecting in 10s")
                # Flush remaining batch
                if batch:
                    downloader.insert_positions_to_postgres(batch)
                    batch = []
                time.sleep(10)

    asyncio.run(stream())


def poll_marinesia(api_key: str, interval: int = 300):
    """
    Poll Marinesia API every 5 minutes for vessel positions
    in all dark zones and key ports.
    """
    logger.info(f"[Marinesia] Starting — polling every {interval}s")

    downloader = AISDownloader()

    while True:
        try:
            logger.info(f"[Marinesia] Fetching @ {datetime.now().strftime('%H:%M:%S')}")
            positions = downloader.fetch_marinesia_realtime()

            if positions:
                # Sample to avoid overwhelming DB with repeated positions
                sample = positions[:5000] if len(positions) > 5000 else positions
                downloader.insert_positions_to_postgres(sample)
                downloader.insert_to_neo4j(sample)
                logger.info(f"[Marinesia] Inserted {len(sample)} positions")
            else:
                logger.debug("[Marinesia] No positions returned")

        except Exception as e:
            logger.error(f"[Marinesia] Error: {e}")

        time.sleep(interval)


def main():
    aisstream_key = os.getenv("AISSTREAM_API_KEY", "")
    marinesia_key = os.getenv("MARINESIA_API_KEY", "")

    logger.info("━" * 50)
    logger.info("Live AIS Tracking — Starting")
    logger.info(f"  AISStream:  {'✓ enabled' if aisstream_key else '✗ no key (set AISSTREAM_API_KEY)'}")
    logger.info(f"  Marinesia:  {'✓ enabled' if marinesia_key else '✗ no key (set MARINESIA_API_KEY)'}")
    logger.info(f"  Dark zones: {len(DARK_ZONES)}")
    logger.info(f"  Key ports:  {len(KEY_PORTS)}")
    logger.info("━" * 50)

    if not aisstream_key and not marinesia_key:
        logger.error("No API keys set. Register free at aisstream.io and/or marinesia.com")
        return

    threads = []

    if marinesia_key:
        t = threading.Thread(target=poll_marinesia, args=(marinesia_key,), daemon=True, name="marinesia")
        threads.append(t)

    if aisstream_key:
        t = threading.Thread(target=run_aisstream, args=(aisstream_key,), daemon=True, name="aisstream")
        threads.append(t)

    for t in threads:
        t.start()
        time.sleep(1)

    try:
        while True:
            time.sleep(60)
            alive = [t.name for t in threads if t.is_alive()]
            logger.debug(f"Active: {', '.join(alive)}")
    except KeyboardInterrupt:
        logger.info("Live AIS Tracking — Stopped by user")


if __name__ == "__main__":
    main()
