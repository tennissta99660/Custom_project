import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from neo4j import GraphDatabase
import redis
import clickhouse_connect

load_dotenv()

class Database:
    @staticmethod
    def pg():
        return create_engine(os.getenv("POSTGRES_URL"))

    @staticmethod
    def neo4j():
        return GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
        )

    @staticmethod
    def redis():
        return redis.from_url(os.getenv("REDIS_URL"))

    @staticmethod
    def clickhouse():
        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "")
        )
