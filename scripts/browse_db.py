"""
Atlas Database Browser — See what's inside your databases.

Usage:
    python scripts/browse_db.py                  → Show row counts for all tables
    python scripts/browse_db.py pg               → Show all PostgreSQL tables + counts
    python scripts/browse_db.py pg news_articles  → Show recent rows from news_articles
    python scripts/browse_db.py ch               → Show all ClickHouse tables + counts
    python scripts/browse_db.py ch market_ticks   → Show recent rows from market_ticks
    python scripts/browse_db.py neo4j            → Show all Neo4j node counts
    python scripts/browse_db.py redis            → Show Redis key count + sample keys
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connections import Database
from sqlalchemy import text

# ── Helpers ─────────────────────────────────────────────────────────

def hr(char="━", width=60):
    print(char * width)


def print_table(headers, rows, max_col_width=40):
    """Print a nicely formatted table in the terminal."""
    if not rows:
        print("  (empty — no data yet)")
        return

    # Stringify and truncate
    str_rows = []
    for row in rows:
        str_row = []
        for val in row:
            s = str(val) if val is not None else "NULL"
            if len(s) > max_col_width:
                s = s[:max_col_width - 3] + "..."
            str_row.append(s)
        str_rows.append(str_row)

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(val))

    # Print header
    header_line = " │ ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(f"  {header_line}")
    print(f"  {'─┼─'.join('─' * w for w in widths)}")

    # Print rows
    for row in str_rows:
        line = " │ ".join(
            row[i].ljust(widths[i]) if i < len(row) else "".ljust(widths[i])
            for i in range(len(headers))
        )
        print(f"  {line}")


# ── PostgreSQL ──────────────────────────────────────────────────────

PG_TABLES = [
    "companies", "filings", "filing_chunks", "port_geofences",
    "ship_positions", "news_articles", "gdelt_events", "news_ticker_mentions",
]

def pg_overview():
    """Show row counts for all PostgreSQL tables."""
    print("\n🐘 POSTGRESQL")
    hr()
    engine = Database.pg()
    rows = []
    with engine.connect() as conn:
        for table in PG_TABLES:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                rows.append((table, str(count)))
            except Exception as e:
                rows.append((table, f"ERROR: {e}"))
    print_table(["Table", "Rows"], rows)
    print()


def pg_browse(table_name, limit=20):
    """Show recent rows from a PostgreSQL table."""
    if table_name not in PG_TABLES:
        print(f"❌ Unknown table '{table_name}'. Available: {', '.join(PG_TABLES)}")
        return

    print(f"\n🐘 PostgreSQL → {table_name} (last {limit} rows)")
    hr()

    engine = Database.pg()
    with engine.connect() as conn:
        # Get column names
        cols_result = conn.execute(text(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = :t ORDER BY ordinal_position"
        ), {"t": table_name})
        columns = [row[0] for row in cols_result]

        if not columns:
            print(f"  Table '{table_name}' not found or has no columns.")
            return

        # Skip embedding/geom columns (they're unreadable as text)
        display_cols = [c for c in columns if c not in ("embedding", "geom")]

        # Fetch rows
        col_list = ", ".join(display_cols)
        result = conn.execute(text(
            f"SELECT {col_list} FROM {table_name} ORDER BY id DESC LIMIT :lim"
        ), {"lim": limit})
        rows = result.fetchall()

        # Get total count
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f"  Total rows: {count}\n")

        print_table(display_cols, rows)
    print()


# ── ClickHouse ──────────────────────────────────────────────────────

CH_TABLES = [
    "market_ticks", "ohlcv_1min", "ohlcv_5min",
    "news_sentiment_ts", "gdelt_event_volume",
]

def ch_overview():
    """Show row counts for all ClickHouse tables."""
    print("\n🟡 CLICKHOUSE")
    hr()
    try:
        ch = Database.clickhouse()
        rows = []
        for table in CH_TABLES:
            try:
                result = ch.query(f"SELECT count() FROM {table}")
                count = result.first_row[0]
                rows.append((table, str(count)))
            except Exception as e:
                rows.append((table, f"ERROR: {e}"))
        print_table(["Table", "Rows"], rows)
        ch.close()
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
    print()


def ch_browse(table_name, limit=20):
    """Show recent rows from a ClickHouse table."""
    if table_name not in CH_TABLES:
        print(f"❌ Unknown table '{table_name}'. Available: {', '.join(CH_TABLES)}")
        return

    print(f"\n🟡 ClickHouse → {table_name} (last {limit} rows)")
    hr()

    try:
        ch = Database.clickhouse()

        # Get columns
        cols_result = ch.query(f"DESCRIBE TABLE {table_name}")
        columns = [row[0] for row in cols_result.result_rows]

        # Fetch rows
        result = ch.query(f"SELECT * FROM {table_name} ORDER BY tuple() LIMIT {limit}")
        rows = result.result_rows

        count_result = ch.query(f"SELECT count() FROM {table_name}")
        count = count_result.first_row[0]
        print(f"  Total rows: {count}\n")

        print_table(columns, rows)
        ch.close()
    except Exception as e:
        print(f"  ❌ Error: {e}")
    print()


# ── Neo4j ───────────────────────────────────────────────────────────

def neo4j_overview():
    """Show node counts for all labels in Neo4j."""
    print("\n🔵 NEO4J")
    hr()
    try:
        driver = Database.neo4j()
        with driver.session(database=os.getenv("NEO4J_DB", "atlas")) as session:
            result = session.run("CALL db.labels() YIELD label RETURN label")
            labels = [record["label"] for record in result]

            if not labels:
                print("  No nodes yet — graph is empty.")
            else:
                rows = []
                for label in labels:
                    count_result = session.run(f"MATCH (n:{label}) RETURN count(n) AS c")
                    count = count_result.single()["c"]
                    rows.append((label, str(count)))
                print_table(["Node Label", "Count"], rows)

            # Show relationship types too
            rel_result = session.run(
                "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
            )
            rels = [r["relationshipType"] for r in rel_result]
            if rels:
                print(f"\n  Relationship types: {', '.join(rels)}")
            else:
                print("\n  No relationships yet.")

        driver.close()
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
    print()


# ── Redis ───────────────────────────────────────────────────────────

def redis_overview():
    """Show key count and sample keys from Redis."""
    print("\n🟢 REDIS")
    hr()
    try:
        r = Database.redis()
        db_size = r.dbsize()
        print(f"  Total keys: {db_size}")

        if db_size > 0:
            keys = r.keys("*")
            sample = keys[:10]
            print(f"  Sample keys:")
            for key in sample:
                key_type = r.type(key).decode("utf-8") if isinstance(r.type(key), bytes) else r.type(key)
                key_name = key.decode("utf-8") if isinstance(key, bytes) else key
                print(f"    {key_name} ({key_type})")
            if db_size > 10:
                print(f"    ... and {db_size - 10} more")
        else:
            print("  (empty — no keys stored yet)")
        r.close()
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
    print()


# ── Full Overview ───────────────────────────────────────────────────

def full_overview():
    """Show everything across all databases."""
    print()
    hr("═")
    print("  ATLAS DATABASE BROWSER")
    hr("═")
    pg_overview()
    ch_overview()
    neo4j_overview()
    redis_overview()
    hr("═")
    print("  Tip: Run 'python scripts/browse_db.py pg news_articles' to see actual rows")
    hr("═")
    print()


# ── CLI Entry Point ─────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    args = sys.argv[1:]

    if not args:
        full_overview()
    elif args[0] == "pg":
        if len(args) > 1:
            limit = int(args[2]) if len(args) > 2 else 20
            pg_browse(args[1], limit)
        else:
            pg_overview()
    elif args[0] == "ch":
        if len(args) > 1:
            limit = int(args[2]) if len(args) > 2 else 20
            ch_browse(args[1], limit)
        else:
            ch_overview()
    elif args[0] == "neo4j":
        neo4j_overview()
    elif args[0] == "redis":
        redis_overview()
    else:
        print(f"❌ Unknown command '{args[0]}'")
        print("Usage: python scripts/browse_db.py [pg|ch|neo4j|redis] [table_name] [limit]")
