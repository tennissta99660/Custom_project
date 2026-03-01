import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connections import Database
from sqlalchemy import text

print("🛠️ Initializing all schemas...\n")

# ─── PostgreSQL ──────────────────────────────────────────
print("📦 PostgreSQL...")
with Database.pg().connect() as conn:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(10) UNIQUE NOT NULL,
            name VARCHAR(255),
            cik VARCHAR(20)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS filings (
            id SERIAL PRIMARY KEY,
            company_id INT REFERENCES companies(id),
            form_type VARCHAR(20),
            filed_at DATE,
            url TEXT
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS filing_chunks (
            id SERIAL PRIMARY KEY,
            filing_id INT REFERENCES filings(id),
            content TEXT,
            embedding vector(384)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS port_geofences (
            id SERIAL PRIMARY KEY,
            port_name VARCHAR(255),
            geom GEOMETRY(POLYGON, 4326)
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ship_positions (
            id SERIAL PRIMARY KEY,
            mmsi VARCHAR(20),
            timestamp TIMESTAMPTZ,
            geom GEOMETRY(POINT, 4326),
            speed FLOAT,
            heading FLOAT
        );
    """))

    # ── News Tables ──
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            source_name VARCHAR(100),
            source_id VARCHAR(255),
            title TEXT,
            summary TEXT,
            content TEXT,
            url TEXT UNIQUE,
            author VARCHAR(255),
            category VARCHAR(100),
            language VARCHAR(10),
            country VARCHAR(10),
            image_url TEXT,
            published_at TIMESTAMPTZ,
            ingested_at TIMESTAMPTZ DEFAULT NOW(),
            embedding vector(384)
        );
    """))
    conn.execute(text("""
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
            event_code VARCHAR(10),
            event_description VARCHAR(255),
            goldstein_scale FLOAT,
            num_mentions INT,
            num_sources INT,
            num_articles INT,
            avg_tone FLOAT,
            geom GEOMETRY(POINT, 4326),
            source_url TEXT
        );
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS news_ticker_mentions (
            id SERIAL PRIMARY KEY,
            article_id INT REFERENCES news_articles(id),
            ticker VARCHAR(10),
            sentiment_score FLOAT,
            relevance_score FLOAT
        );
    """))
    conn.commit()
print("✅ PostgreSQL schemas done")

# ─── Neo4j ───────────────────────────────────────────────
print("📦 Neo4j...")
driver = Database.neo4j()
with driver.session(database="atlas") as s:
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.ticker IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (f:Filing) REQUIRE f.id IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Ship) REQUIRE s.mmsi IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Port) REQUIRE p.name IS UNIQUE")
    # News constraints
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Article) REQUIRE a.url IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Event) REQUIRE e.global_event_id IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (src:Source) REQUIRE src.name IS UNIQUE")
    s.run("CREATE CONSTRAINT IF NOT EXISTS FOR (cat:NewsCategory) REQUIRE cat.name IS UNIQUE")
driver.close()
print("✅ Neo4j constraints done")

# ─── ClickHouse ──────────────────────────────────────────
print("📦 ClickHouse...")
ch = Database.clickhouse()

ch.command("""
    CREATE TABLE IF NOT EXISTS market_ticks (
        symbol      String,
        price       Float64,
        volume      Float64,
        timestamp   DateTime64(3, 'UTC')
    ) ENGINE = MergeTree()
    ORDER BY (symbol, timestamp)
""")

ch.command("""
    CREATE TABLE IF NOT EXISTS ohlcv_1min (
        symbol        String,
        minute_bucket DateTime,
        open          Float64,
        high          Float64,
        low           Float64,
        close         Float64,
        volume        Float64
    ) ENGINE = MergeTree()
    ORDER BY (symbol, minute_bucket)
""")

ch.command("""
    CREATE TABLE IF NOT EXISTS ohlcv_5min (
        symbol        String,
        bucket_5min   DateTime,
        open          Float64,
        high          Float64,
        low           Float64,
        close         Float64,
        volume        Float64
    ) ENGINE = MergeTree()
    ORDER BY (symbol, bucket_5min)
""")

ch.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1min_mv
    TO ohlcv_1min AS
    SELECT
        symbol,
        toStartOfMinute(timestamp)  AS minute_bucket,
        argMin(price, timestamp)    AS open,
        max(price)                  AS high,
        min(price)                  AS low,
        argMax(price, timestamp)    AS close,
        sum(volume)                 AS volume
    FROM market_ticks
    GROUP BY symbol, minute_bucket
""")

ch.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5min_mv
    TO ohlcv_5min AS
    SELECT
        symbol,
        toStartOfFiveMinutes(timestamp) AS bucket_5min,
        argMin(price, timestamp)        AS open,
        max(price)                      AS high,
        min(price)                      AS low,
        argMax(price, timestamp)        AS close,
        sum(volume)                     AS volume
    FROM market_ticks
    GROUP BY symbol, bucket_5min
""")

# ── News Analytics Tables ──
ch.command("""
    CREATE TABLE IF NOT EXISTS news_sentiment_ts (
        ticker      String,
        source      String,
        sentiment   Float64,
        tone        Float64,
        timestamp   DateTime64(3, 'UTC')
    ) ENGINE = MergeTree()
    ORDER BY (ticker, timestamp)
""")

ch.command("""
    CREATE TABLE IF NOT EXISTS gdelt_event_volume (
        event_code      String,
        country         String,
        event_count     UInt32,
        avg_tone        Float64,
        avg_goldstein   Float64,
        bucket_hour     DateTime
    ) ENGINE = MergeTree()
    ORDER BY (event_code, country, bucket_hour)
""")

ch.close()
print("✅ ClickHouse schemas done")

print("\n🎉 ALL SCHEMAS INITIALIZED — Week 1 + News pipeline ready!")

