"""
Generate Embeddings — Embed all filing chunks using sentence-transformers

Creates 384-dim embeddings for all filing_chunks and stores them in pgvector.
Also creates HNSW index for fast similarity search.

Usage:
    python scripts/generate_embeddings.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connections import Database
from sqlalchemy import text
from loguru import logger


def main():
    logger.info("━" * 50)
    logger.info("Embedding Generator — Starting")
    logger.info("━" * 50)

    # ── Load model ──
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("Loaded all-MiniLM-L6-v2 (384-dim)")

    engine = Database.pg()

    # ── Fetch chunks that need embeddings ──
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, content FROM filing_chunks WHERE embedding IS NULL AND content IS NOT NULL"
        )).fetchall()

    if not rows:
        logger.info("All chunks already have embeddings — checking total count")
        with engine.connect() as conn:
            total = conn.execute(text(
                "SELECT COUNT(*) FROM filing_chunks WHERE embedding IS NOT NULL"
            )).scalar()
        logger.info(f"Total embedded chunks: {total}")

        # Create index if not exists
        _create_index(engine)
        return

    logger.info(f"Found {len(rows)} chunks to embed")

    # ── Generate embeddings in batches ──
    batch_size = 64
    total_embedded = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        ids = [r[0] for r in batch]
        texts = [r[1] for r in batch]

        # Generate embeddings
        embeddings = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)

        # Update database
        with engine.connect() as conn:
            for chunk_id, embedding in zip(ids, embeddings):
                vec_str = "[" + ",".join(str(float(x)) for x in embedding) + "]"
                conn.execute(
                    text("UPDATE filing_chunks SET embedding = :emb WHERE id = :id"),
                    {"emb": vec_str, "id": chunk_id},
                )
            conn.commit()

        total_embedded += len(batch)
        logger.info(f"  Embedded {total_embedded}/{len(rows)} chunks")

    logger.info(f" All {total_embedded} chunks embedded")

    # ── Also embed news articles that don't have embeddings yet ──
    with engine.connect() as conn:
        news_rows = conn.execute(text(
            "SELECT id, title, summary FROM news_articles WHERE embedding IS NULL AND title IS NOT NULL LIMIT 5000"
        )).fetchall()

    if news_rows:
        logger.info(f"Found {len(news_rows)} news articles to embed")
        for i in range(0, len(news_rows), batch_size):
            batch = news_rows[i : i + batch_size]
            ids = [r[0] for r in batch]
            texts = [f"{r[1]}. {r[2] or ''}" for r in batch]

            embeddings = model.encode(texts, show_progress_bar=False, normalize_embeddings=True)

            with engine.connect() as conn:
                for article_id, embedding in zip(ids, embeddings):
                    vec_str = "[" + ",".join(str(float(x)) for x in embedding) + "]"
                    conn.execute(
                        text("UPDATE news_articles SET embedding = :emb WHERE id = :id"),
                        {"emb": vec_str, "id": article_id},
                    )
                conn.commit()

            total_embedded += len(batch)
            if (i // batch_size) % 5 == 0:
                logger.info(f"  News: {min(i + batch_size, len(news_rows))}/{len(news_rows)}")

        logger.info(f"✅ News articles embedded")

    # ── Create HNSW index for fast search ──
    _create_index(engine)

    logger.info("━" * 50)
    logger.info("Embedding Generator — Complete ✓")
    logger.info("━" * 50)


def _create_index(engine):
    """Create HNSW index on vector columns for fast cosine similarity search."""
    with engine.connect() as conn:
        # Filing chunks index
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_filing_chunks_embedding
            ON filing_chunks
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """))

        # News articles index
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_news_articles_embedding
            ON news_articles
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """))

        conn.commit()
    logger.info(" HNSW indexes created for fast similarity search")


if __name__ == "__main__":
    main()
