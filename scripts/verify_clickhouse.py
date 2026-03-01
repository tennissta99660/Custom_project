# scripts/verify_clickhouse.py

import sys
sys.path.append(".")

from config.settings import CH_HOST, CH_PORT, CH_USER, CH_PASSWORD

def verify_clickhouse():
    try:
        import clickhouse_connect

        print(f"Connecting to ClickHouse at {CH_HOST}:{CH_PORT}")
        print(f"User: {CH_USER}")
        print(f"Password set: {'✅ Yes' if CH_PASSWORD else '❌ No (blank)'}")

        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD
        )

        # Check version
        version = client.command("SELECT version()")
        print(f"\n✅ Connected successfully")
        print(f"✅ ClickHouse version: {version}")

        # Check auth is actually enforced
        print(f"\n--- Testing auth enforcement ---")
        try:
            bad_client = clickhouse_connect.get_client(
                host=CH_HOST,
                port=CH_PORT,
                username=CH_USER,
                password="wrongpassword123"
            )
            bad_client.command("SELECT 1")
            print("⚠️  WARNING: Wrong password still connected — password NOT enforced")
        except Exception:
            print("✅ Auth enforced: wrong password correctly rejected")

        # Check tables exist
        print(f"\n--- Checking schemas ---")
        tables = client.query("SHOW TABLES").result_rows
        if tables:
            print(f"✅ Tables found: {[t[0] for t in tables]}")
        else:
            print("⚠️  No tables yet — schemas not created (run clickhouse_schema.sql next)")

        # Check it's localhost-only (not exposed publicly)
        listen_host = client.command(
            "SELECT value FROM system.server_settings WHERE name='listen_host' LIMIT 1"
        )
        print(f"\n--- Network exposure ---")
        print(f"Listen host: {listen_host if listen_host else 'localhost (default, safe ✅)'}")

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Is ClickHouse running? → run: sudo clickhouse start (in WSL)")
        print("  2. Is CH_PASSWORD in .env matching the password you set?")
        print("  3. Did you run: pip install clickhouse-connect")

if __name__ == "__main__":
    verify_clickhouse()
