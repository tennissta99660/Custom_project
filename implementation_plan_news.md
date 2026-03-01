# News Sources & Satellite Data Integration for Atlas Search Engine

The Atlas Search Engine is a financial intelligence platform with PostgreSQL (PostGIS), Neo4j, ClickHouse, and Redis. It already has scaffolded ingestion for EDGAR filings, XBRL, and market ticks, plus ship/port tracking via PostGIS. This plan adds **real-time global news ingestion** and documents **satellite data sources** that can power the platform's analytics.

---

## Proposed Changes

### News Ingestion Pipeline

We'll create a new `ingestion/news/` directory with four downloaders, ordered from most critical (free, unlimited) to supplementary:

---

#### [NEW] [gdelt_downloader.py](file:///e:/terminal_proj/ingestion/news/gdelt_downloader.py)

**GDELT (Global Database of Events, Language, and Tone)** — the backbone news source.

- **Why**: Completely free, unlimited access, updates every 15 minutes, covers 100+ languages, monitors broadcast/print/web news globally
- **What it captures**: Events (who did what to whom, where, when), mentions, tone/sentiment, source URLs, geocoded locations, Goldstein conflict scale, CAMEO event codes
- **Endpoints**:
  - `http://data.gdeltproject.org/gdeltv2/lastupdate.txt` — latest 15-min batch
  - `http://api.gdeltproject.org/api/v2/doc/doc` — full-text search, timeline, tone analysis
  - `http://api.gdeltproject.org/api/v2/geo/geo` — geographic heatmaps of events
- **Data stored**: Events table (actor1, actor2, event type, tone, goldstein scale, geo coords, source URLs)
- Downloads CSV batches, parses, and inserts into both PostgreSQL (structured events + PostGIS geom) and ClickHouse (time-series sentiment analytics)

---

#### [NEW] [finnhub_news_downloader.py](file:///e:/terminal_proj/ingestion/news/finnhub_news_downloader.py)

**Finnhub** — financial-specific news with sentiment scores.

- **Why**: Free tier (60 calls/min), provides sentiment scores, entity-tagged to tickers, covers market-moving news
- **What it captures**: Company news by ticker, market news by category, press releases, SEC filings buzz
- **Endpoints**:
  - `/api/v1/company-news?symbol=AAPL&from=2024-01-01&to=2024-12-31` — company-specific
  - `/api/v1/news?category=general` — general market news
- **Data stored**: Article title, summary, source, URL, ticker association, sentiment, published_at

---

#### [NEW] [rss_aggregator.py](file:///e:/terminal_proj/ingestion/news/rss_aggregator.py)

**RSS/Atom Feed Aggregator** — custom curated sources for deep coverage.

- **Why**: Free, no API keys needed, captures niche/specialized sources that APIs miss
- **Pre-configured feeds** (configurable list):
  - **Macro/Geopolitics**: Reuters World, BBC World, Al Jazeera, AP News
  - **Financial**: Bloomberg Markets, FT Markets, WSJ Markets, CNBC
  - **Commodities**: Reuters Commodities, Platts, OilPrice.com
  - **Shipping/Trade**: Lloyd's List, TradeWinds, The Maritime Executive, Splash247
  - **Agriculture**: AgriPulse, Farm Journal, World-Grain
  - **Central Banks**: Fed RSS, ECB Press, BoE, RBI
  - **Tech/Cyber**: Ars Technica, The Verge, Krebs on Security
  - **Government/Policy**: US Federal Register, EU Official Journal, UN News
- Uses `feedparser` library, deduplicates by URL hash, stores full article metadata

---

#### [NEW] [newsdata_downloader.py](file:///e:/terminal_proj/ingestion/news/newsdata_downloader.py)

**NewsData.io** — breaking news from 88,000+ sources in 80+ languages.

- **Why**: Free tier (200 credits/day), massive global coverage, supports category/country/language filters, sentiment analysis included
- **Endpoints**:
  - `https://newsdata.io/api/1/latest` — latest breaking news
  - `https://newsdata.io/api/1/news` — historical search with filters
- **Filters**: country, category (business, politics, world, environment, technology, science, health), language, keyword
- **Data stored**: Title, description, full content (when available), source, country, category, sentiment, keywords, image URL

---

### Database Schema Updates

---

#### [MODIFY] [init_schemas.py](file:///e:/terminal_proj/scripts/init_schemas.py)

Add the following new tables/constraints:

**PostgreSQL — new tables:**

```sql
-- Core news articles (deduplicated across all sources)
CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100),        -- 'gdelt', 'finnhub', 'rss', 'newsdata'
    source_id VARCHAR(255),          -- original ID from source
    title TEXT,
    summary TEXT,
    content TEXT,
    url TEXT UNIQUE,                  -- dedup key
    author VARCHAR(255),
    category VARCHAR(100),
    language VARCHAR(10),
    country VARCHAR(10),
    image_url TEXT,
    published_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    embedding vector(384)            -- for RAG semantic search
);

-- GDELT events (geopolitical event database)
CREATE TABLE IF NOT EXISTS gdelt_events (
    id SERIAL PRIMARY KEY,
    global_event_id BIGINT UNIQUE,
    event_date DATE,
    actor1_name VARCHAR(255),
    actor1_country VARCHAR(10),
    actor1_type VARCHAR(100),
    actor2_name VARCHAR(255),
    actor2_country VARCHAR(10),
    actor2_type VARCHAR(100),
    event_code VARCHAR(10),          -- CAMEO code
    event_description VARCHAR(255),
    goldstein_scale FLOAT,           -- -10 to +10 conflict scale
    num_mentions INT,
    num_sources INT,
    num_articles INT,
    avg_tone FLOAT,
    geom GEOMETRY(POINT, 4326),      -- PostGIS point
    source_url TEXT
);

-- News ↔ Company ticker associations
CREATE TABLE IF NOT EXISTS news_ticker_mentions (
    id SERIAL PRIMARY KEY,
    article_id INT REFERENCES news_articles(id),
    ticker VARCHAR(10),
    sentiment_score FLOAT,           -- -1.0 to 1.0
    relevance_score FLOAT            -- 0.0 to 1.0
);
```

**ClickHouse — new tables:**

```sql
-- News sentiment time-series (for charting sentiment over time per ticker/topic)
CREATE TABLE IF NOT EXISTS news_sentiment_ts (
    ticker      String,
    source      String,
    sentiment   Float64,
    tone        Float64,
    timestamp   DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (ticker, timestamp);

-- GDELT event volume time-series (for global instability tracking)
CREATE TABLE IF NOT EXISTS gdelt_event_volume (
    event_code      String,
    country         String,
    event_count     UInt32,
    avg_tone        Float64,
    avg_goldstein   Float64,
    bucket_hour     DateTime
) ENGINE = MergeTree()
ORDER BY (event_code, country, bucket_hour);
```

**Neo4j — new constraints:**

```cypher
CREATE CONSTRAINT IF NOT EXISTS FOR (a:Article) REQUIRE a.url IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (e:Event) REQUIRE e.global_event_id IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (src:Source) REQUIRE src.name IS UNIQUE
-- Relationships: (Article)-[:MENTIONS]->(Company), (Event)-[:INVOLVES]->(Country), (Article)-[:FROM]->(Source)
```

---

### Configuration Updates

---

#### [MODIFY] [settings.py](file:///e:/terminal_proj/config/settings.py)

Add new API key fields:

```python
# News APIs
FINNHUB_API_KEY: str = ""
NEWSDATA_API_KEY: str = ""
GDELT_UPDATE_INTERVAL: int = 900  # 15 minutes in seconds
```

---

#### [MODIFY] [.env](file:///e:/terminal_proj/.env)

Add corresponding environment variables:

```env
# ============================================
# NEWS APIs
# ============================================
FINNHUB_API_KEY=
NEWSDATA_API_KEY=
GDELT_UPDATE_INTERVAL=900
```

---

### New Directory Structure

```
ingestion/
├── filings/       (existing)
├── market/        (existing)
└── news/          (NEW)
    ├── __init__.py
    ├── gdelt_downloader.py
    ├── finnhub_news_downloader.py
    ├── rss_aggregator.py
    └── newsdata_downloader.py
```

---

## Satellite Data — What Can Be Retrieved & How It's Useful

This section catalogs satellite data sources relevant to the Atlas platform. These aren't being implemented now, but are documented for future integration planning.

### 1. AIS Vessel Tracking (Already Partially Scaffolded)
| Data | Source | Use Case |
|------|--------|----------|
| Real-time ship positions (lat/lon, speed, heading) | MarineTraffic API, VesselFinder, Spire Maritime | Track commodity shipments (oil tankers, bulk carriers, LNG) in real-time |
| Port call history | ExactEarth, Spire | Measure port congestion, predict supply delays |
| Dark vessel detection | HawkEye 360, Spire | Detect ships that disable AIS — sanctions evasion, illegal fishing |
| Vessel ownership chains | IHS Markit, Lloyd's Register | Map corporate ownership of shipping fleets — Neo4j graph |

### 2. Oil & Energy Infrastructure
| Data | Source | Use Case |
|------|--------|----------|
| Oil tank fill levels (via shadow analysis) | Orbital Insight, Kayrros, Ursa Space | Estimate crude oil inventories before EIA reports |
| Refinery activity (thermal emissions) | Planet Labs, Maxar | Track refinery throughput, planned/unplanned shutdowns |
| Pipeline construction monitoring | Satellite imagery (Sentinel-2) | Track new pipeline projects, geopolitical implications |
| Flaring activity | VIIRS Nightfire (NASA) | Monitor gas flaring — proxy for production levels |

### 3. Agriculture & Food Supply
| Data | Source | Use Case |
|------|--------|----------|
| NDVI (crop health index) | Copernicus Sentinel-2, EOSDA, OpenWeather Agro API | Monitor crop health → predict yield → forecast commodity prices (wheat, corn, soy) |
| Soil moisture | SMOS, SMAP (NASA) | Drought detection, irrigation demand |
| Growing degree days | ERA5 (ECMWF) | Crop maturity timing |
| Deforestation tracking | Global Forest Watch, GLAD Alerts | ESG compliance, supply chain due diligence (palm oil, soy, cattle) |
| Grain stockpile volume | Maxar, Planet Labs | Estimate grain storage at silos/ports — pre-USDA report signals |

### 4. Economic Activity Proxies
| Data | Source | Use Case |
|------|--------|----------|
| Nighttime lights intensity | VIIRS DNB (NASA) | GDP proxy, urbanization tracking, post-disaster recovery monitoring |
| Parking lot occupancy | RS Metrics, Orbital Insight | Retail sales prediction (Walmart, Target, malls) |
| Construction activity | Planet Labs, Maxar | Real estate development tracking, infrastructure investment signals |
| Factory thermal emissions | Sentinel-3 SLSTR | Manufacturing activity proxy, industrial output estimation |

### 5. Mining & Metals
| Data | Source | Use Case |
|------|--------|----------|
| Mine site activity | Planet Labs, Maxar | Track production at copper, lithium, iron ore mines |
| Tailings dam monitoring | Sentinel-1 SAR | Risk monitoring (dam failures like Brumadinho) |
| Stockpile volumes at ports | High-res satellite imagery | Metal inventory estimation before LME reports |

### 6. Weather & Natural Disasters
| Data | Source | Use Case |
|------|--------|----------|
| Hurricane/typhoon tracking | NOAA, EUMETSAT | Shipping route disruption, energy infrastructure risk |
| Flood extent mapping | Sentinel-1 SAR, Copernicus EMS | Insurance loss estimation, supply chain disruption |
| Wildfire detection | VIIRS, MODIS (NASA) | Agricultural loss, air quality impact, insurance |
| Snow cover / water reservoir levels | Sentinel-2, Landsat | Hydropower generation prediction, water scarcity |

### 7. Geopolitical & Military
| Data | Source | Use Case |
|------|--------|----------|
| Military base activity | Maxar, Planet Labs | Geopolitical tension proxy |
| Border activity | Satellite imagery | Migration patterns, trade disruption signals |
| Conflict zone monitoring | UNOSAT, Sentinel-1 SAR | Risk scoring for sovereign debt, insurance |

### 8. Environmental / ESG
| Data | Source | Use Case |
|------|--------|----------|
| CO2 / methane emissions | OCO-2 (NASA), Sentinel-5P (TROPOMI) | ESG scoring, carbon credit verification |
| Air quality (NO2, SO2, PM2.5) | Sentinel-5P, OpenAQ API | Health impact analysis, regulatory risk |
| Coral reef / ocean health | Copernicus Marine Service | Fishery economics, tourism impact |
| Glacier melt rate | Sentinel-2, ICESat-2 | Long-term climate risk for insurance, coastal real estate |

### Free/Open Satellite Data APIs Ready for Integration

| API | Data | Free Tier | URL |
|-----|------|-----------|-----|
| **Copernicus Open Access Hub** | Sentinel-1/2/3/5P imagery | Fully free | `https://dataspace.copernicus.eu` |
| **NASA Earthdata** | MODIS, VIIRS, Landsat, SMAP | Fully free | `https://earthdata.nasa.gov` |
| **OpenWeather Agro API** | NDVI, EVI, soil temp by polygon | Free tier (25K calls/day) | `https://agromonitoring.com/api` |
| **EOSDA Crop Monitoring API** | Vegetation indices, yield prediction | Free tier | `https://eos.com/products/crop-monitoring/` |
| **Global Forest Watch API** | Deforestation alerts, fire alerts | Fully free | `https://www.globalforestwatch.org/` |
| **OpenAQ** | Air quality from ground + satellite | Fully free | `https://openaq.org/` |
| **FIRMS (NASA)** | Active fire data (near real-time) | Fully free | `https://firms.modaps.eosdis.nasa.gov/` |
| **MarineTraffic API** | AIS vessel tracking | Free tier (limited) | `https://www.marinetraffic.com/en/ais-api-services` |

---

## Verification Plan

### Automated Tests

1. **Syntax check all new Python files**:
   ```
   python -m py_compile ingestion/news/gdelt_downloader.py
   python -m py_compile ingestion/news/finnhub_news_downloader.py
   python -m py_compile ingestion/news/rss_aggregator.py
   python -m py_compile ingestion/news/newsdata_downloader.py
   ```
   Run from `e:\terminal_proj\`.

2. **Import check** — Verify all modules can be imported without errors:
   ```
   python -c "from ingestion.news.gdelt_downloader import GDELTDownloader; print('OK')"
   python -c "from ingestion.news.finnhub_news_downloader import FinnhubNewsDownloader; print('OK')"  
   python -c "from ingestion.news.rss_aggregator import RSSAggregator; print('OK')"
   python -c "from ingestion.news.newsdata_downloader import NewsDataDownloader; print('OK')"
   ```

3. **Settings validation** — Ensure the updated [Settings](file:///e:/terminal_proj/config/settings.py#4-39) class loads correctly:
   ```
   python -c "from config.settings import settings; print(settings.FINNHUB_API_KEY, settings.NEWSDATA_API_KEY)"
   ```

### Manual Verification

Since databases may not be running, manual verification is deferred to the user:

1. Run `python scripts/init_schemas.py` with all databases running to confirm the new tables/constraints are created without errors
2. Set a `FINNHUB_API_KEY` in [.env](file:///e:/terminal_proj/.env) (free at finnhub.io), then test the Finnhub downloader
3. Run the GDELT downloader (no key needed) to verify it fetches and parses the latest 15-min update
