import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connections import Database
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

print("🔧 ENV-ONLY CONNECTION TEST\n")

# PostgreSQL
with Database.pg().connect() as conn:
    result = conn.execute(text("SELECT 1 AS test"))
    print("✅ PG:", result.scalar())
# Redis
r = Database.redis()
print("✅ Redis:", "PONG" if r.ping() else "FAIL")
r.close()

# ClickHouse
client = Database.clickhouse()
result = client.query("SELECT 1 AS test")
print("✅ ClickHouse:", result.first_row)
client.close()


# Neo4j
driver = Database.neo4j()
with driver.session(database="atlas") as session:
    result = session.run("RETURN 1 AS test")
    print("✅ Neo4j:", result.single()["test"])
driver.close()

print("\n🎉 ALL DATABASES LIVE!")
