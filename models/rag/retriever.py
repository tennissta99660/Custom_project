"""
RAG Retriever — Semantic search over filing chunks and news articles

Uses pgvector (HNSW) for fast cosine similarity search.
Returns the top-K most relevant chunks/articles for a given query.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db.connections import Database
from sqlalchemy import text
from sentence_transformers import SentenceTransformer


class Retriever:
    """Semantic search over pgvector-indexed documents."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        self.engine = Database.pg()

    def search_filings(self, query: str, top_k: int = 5) -> list[dict]:
        """
        Search filing chunks by semantic similarity.

        Returns list of dicts with: chunk_id, content, filing_id, form_type,
        company_ticker, similarity_score
        """
        query_embedding = self.model.encode(query, normalize_embeddings=True)
        vec_str = "[" + ",".join(str(float(x)) for x in query_embedding) + "]"

        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                    SELECT
                        fc.id AS chunk_id,
                        fc.content,
                        fc.filing_id,
                        f.form_type,
                        f.filed_at,
                        c.ticker,
                        c.name AS company_name,
                        1 - (fc.embedding <=> :query_vec::vector) AS similarity
                    FROM filing_chunks fc
                    JOIN filings f ON fc.filing_id = f.id
                    JOIN companies c ON f.company_id = c.id
                    WHERE fc.embedding IS NOT NULL
                    ORDER BY fc.embedding <=> :query_vec::vector
                    LIMIT :top_k
                """),
                {"query_vec": vec_str, "top_k": top_k},
            ).fetchall()

        return [
            {
                "chunk_id": r[0],
                "content": r[1],
                "filing_id": r[2],
                "form_type": r[3],
                "filed_at": str(r[4]) if r[4] else "",
                "ticker": r[5],
                "company_name": r[6],
                "similarity": round(float(r[7]), 4),
            }
            for r in results
        ]

    def search_news(self, query: str, top_k: int = 5) -> list[dict]:
        """Search news articles by semantic similarity."""
        query_embedding = self.model.encode(query, normalize_embeddings=True)
        vec_str = "[" + ",".join(str(float(x)) for x in query_embedding) + "]"

        with self.engine.connect() as conn:
            results = conn.execute(
                text("""
                    SELECT
                        id,
                        title,
                        summary,
                        source_name,
                        published_at,
                        url,
                        1 - (embedding <=> :query_vec::vector) AS similarity
                    FROM news_articles
                    WHERE embedding IS NOT NULL
                    ORDER BY embedding <=> :query_vec::vector
                    LIMIT :top_k
                """),
                {"query_vec": vec_str, "top_k": top_k},
            ).fetchall()

        return [
            {
                "article_id": r[0],
                "title": r[1],
                "summary": r[2],
                "source": r[3],
                "published_at": str(r[4]) if r[4] else "",
                "url": r[5],
                "similarity": round(float(r[6]), 4),
            }
            for r in results
        ]

    def search_all(self, query: str, top_k: int = 5) -> dict:
        """Search both filings and news, return combined results."""
        return {
            "filings": self.search_filings(query, top_k),
            "news": self.search_news(query, top_k),
        }
