"""
FinBERT Sentiment Model (M2) — Fine-tune on financial news

Fine-tunes ProsusAI/finbert on:
  1. Your 2K news articles (auto-labeled by keyword heuristics)
  2. FinancialPhraseBank (4.8K labeled financial sentences)

Saves fine-tuned model to models/sentiment/finbert_atlas/

Usage:
    python models/sentiment/train_finbert.py
    python models/sentiment/train_finbert.py --epochs 5 --batch-size 16
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import torch
import numpy as np
from datasets import Dataset, DatasetDict, load_dataset
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    TrainingArguments,
    Trainer,
)
from sklearn.metrics import accuracy_score, f1_score
from loguru import logger
from db.connections import Database
from sqlalchemy import text

MODEL_NAME = "ProsusAI/finbert"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "finbert_atlas")
LABEL_MAP = {"positive": 0, "negative": 1, "neutral": 2}
ID_TO_LABEL = {v: k for k, v in LABEL_MAP.items()}

# Keywords for auto-labeling unlabeled articles
POSITIVE_KEYWORDS = [
    "beat", "surge", "record high", "upgrade", "growth", "rally",
    "profit", "outperform", "bullish", "breakout", "strong demand",
]
NEGATIVE_KEYWORDS = [
    "crash", "plunge", "downgrade", "loss", "layoff", "recession",
    "bearish", "default", "bankruptcy", "miss", "warning", "decline",
]


def auto_label(text_str: str) -> int:
    """Simple keyword-based auto-labeling for articles without human labels."""
    t = text_str.lower()
    pos = sum(1 for kw in POSITIVE_KEYWORDS if kw in t)
    neg = sum(1 for kw in NEGATIVE_KEYWORDS if kw in t)
    if pos > neg:
        return LABEL_MAP["positive"]
    elif neg > pos:
        return LABEL_MAP["negative"]
    return LABEL_MAP["neutral"]


def load_atlas_news() -> list[dict]:
    """Load news articles from PostgreSQL and auto-label them."""
    engine = Database.pg()
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT title, summary FROM news_articles WHERE title IS NOT NULL LIMIT 5000"
        )).fetchall()

    articles = []
    for r in rows:
        text_str = f"{r[0]}. {r[1] or ''}"
        articles.append({
            "text": text_str[:512],
            "label": auto_label(text_str),
        })

    logger.info(f"Loaded {len(articles)} articles from PostgreSQL")
    pos = sum(1 for a in articles if a["label"] == 0)
    neg = sum(1 for a in articles if a["label"] == 1)
    neu = sum(1 for a in articles if a["label"] == 2)
    logger.info(f"  Distribution: {pos} positive, {neg} negative, {neu} neutral")

    return articles


def load_financial_phrasebank() -> list[dict]:
    """Load FinancialPhraseBank from HuggingFace."""
    try:
        ds = load_dataset("financial_phrasebank", "sentences_50agree", trust_remote_code=True)
        items = []
        for row in ds["train"]:
            items.append({
                "text": row["sentence"][:512],
                "label": row["label"],  # 0=negative, 1=neutral, 2=positive in FPB
            })
        # Remap FPB labels (0=neg,1=neu,2=pos) to our format (0=pos,1=neg,2=neu)
        for item in items:
            if item["label"] == 0:
                item["label"] = LABEL_MAP["negative"]
            elif item["label"] == 1:
                item["label"] = LABEL_MAP["neutral"]
            else:
                item["label"] = LABEL_MAP["positive"]

        logger.info(f"Loaded {len(items)} sentences from FinancialPhraseBank")
        return items
    except Exception as e:
        logger.warning(f"Could not load FinancialPhraseBank: {e}")
        return []


def compute_metrics(eval_pred):
    """Compute accuracy and F1 for the trainer."""
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    return {
        "accuracy": accuracy_score(labels, predictions),
        "f1_macro": f1_score(labels, predictions, average="macro"),
    }


def train(epochs: int = 3, batch_size: int = 16, lr: float = 2e-5):
    """Fine-tune FinBERT on combined dataset."""

    logger.info("━" * 50)
    logger.info("FinBERT Fine-tuning — Starting")
    logger.info("━" * 50)

    # ── Load data ──
    atlas_data = load_atlas_news()
    fpb_data = load_financial_phrasebank()
    all_data = atlas_data + fpb_data

    if len(all_data) < 100:
        logger.error("Not enough training data. Need at least 100 samples.")
        return

    # ── Split: 85% train, 15% eval ──
    np.random.shuffle(all_data)
    split = int(0.85 * len(all_data))
    train_data = all_data[:split]
    eval_data = all_data[split:]

    logger.info(f"Train: {len(train_data)} | Eval: {len(eval_data)}")

    dataset = DatasetDict({
        "train": Dataset.from_list(train_data),
        "eval": Dataset.from_list(eval_data),
    })

    # ── Tokenize ──
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    def tokenize(batch):
        return tokenizer(batch["text"], truncation=True, padding="max_length", max_length=256)

    tokenized = dataset.map(tokenize, batched=True, remove_columns=["text"])

    # ── Model ──
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME,
        num_labels=3,
        id2label=ID_TO_LABEL,
        label2id=LABEL_MAP,
    )

    # ── Training args ──
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size * 2,
        learning_rate=lr,
        weight_decay=0.01,
        eval_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1_macro",
        logging_steps=50,
        fp16=torch.cuda.is_available(),
        report_to="none",
    )

    # ── Train ──
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized["train"],
        eval_dataset=tokenized["eval"],
        compute_metrics=compute_metrics,
    )

    logger.info(f"Training on {'GPU' if torch.cuda.is_available() else 'CPU'}...")
    trainer.train()

    # ── Save ──
    trainer.save_model(OUTPUT_DIR)
    tokenizer.save_pretrained(OUTPUT_DIR)
    logger.info(f"✅ Model saved to {OUTPUT_DIR}")

    # ── Final eval ──
    metrics = trainer.evaluate()
    logger.info(f"Final eval — Accuracy: {metrics['eval_accuracy']:.4f}, F1: {metrics['eval_f1_macro']:.4f}")

    logger.info("━" * 50)
    logger.info("FinBERT Fine-tuning — Complete ✓")
    logger.info("━" * 50)


def score_articles():
    """Score all news articles with the fine-tuned model."""
    from transformers import pipeline

    if not os.path.exists(OUTPUT_DIR):
        logger.error(f"No fine-tuned model at {OUTPUT_DIR}. Run training first.")
        return

    logger.info("Scoring articles with fine-tuned FinBERT...")
    pipe = pipeline("text-classification", model=OUTPUT_DIR, device=0 if torch.cuda.is_available() else -1)

    engine = Database.pg()
    import clickhouse_connect
    ch = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, title, summary, published_at FROM news_articles WHERE title IS NOT NULL"
        )).fetchall()

    logger.info(f"Scoring {len(rows)} articles...")

    batch_size = 32
    insert_rows = []

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        texts = [f"{r[1]}. {r[2] or ''}"[:512] for r in batch]

        results = pipe(texts, batch_size=batch_size, truncation=True, max_length=256)

        for r, result in zip(batch, results):
            label = result["label"]
            score = result["score"]
            sentiment_val = score if label == "positive" else (-score if label == "negative" else 0)

            # Extract ticker mentions from title
            tickers = _extract_tickers(r[1])
            for ticker in tickers:
                insert_rows.append([
                    ticker, "finbert", sentiment_val, 0.0,
                    r[3] if r[3] else "2025-01-01",
                ])

    if insert_rows:
        ch.insert("news_sentiment_ts", insert_rows, column_names=[
            "ticker", "source", "sentiment", "tone", "timestamp",
        ])
        logger.info(f"✅ Scored {len(insert_rows)} ticker-sentiment pairs → ClickHouse")

    ch.close()


def _extract_tickers(title: str) -> list[str]:
    """Extract stock tickers mentioned in a title."""
    known = {
        "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "amazon": "AMZN",
        "nvidia": "NVDA", "meta": "META", "tesla": "TSLA", "netflix": "NFLX",
        "reliance": "RELIANCE", "tcs": "TCS", "infosys": "INFY", "hdfc": "HDFCBANK",
        "icici": "ICICIBANK", "sbi": "SBIN", "itc": "ITC",
    }
    title_lower = title.lower()
    return [ticker for name, ticker in known.items() if name in title_lower] or ["MARKET"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--score", action="store_true", help="Score articles with trained model")
    args = parser.parse_args()

    if args.score:
        score_articles()
    else:
        train(epochs=args.epochs, batch_size=args.batch_size)
