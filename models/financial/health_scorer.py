"""
XBRL Financial Health Scorer (M7) — LightGBM on company metrics

Extracts financial metrics from Neo4j, computes derived ratios,
and trains a LightGBM model to produce a health score (0–100).

Usage:
    python models/financial/health_scorer.py
    python models/financial/health_scorer.py --score  # Score all companies
"""

import os
import sys
import json
import argparse
import pickle

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import numpy as np
from loguru import logger
from db.connections import Database

MODEL_PATH = os.path.join(os.path.dirname(__file__), "health_model.pkl")


def extract_metrics() -> dict:
    """Extract financial metrics from Neo4j for all companies."""

    driver = Database.neo4j()
    companies = {}

    with driver.session(database="atlas") as s:
        # Get all companies and their metrics
        result = s.run("""
            MATCH (c:Company)-[:REPORTED]->(m:Metric)
            RETURN c.ticker AS ticker, c.name AS name,
                   m.name AS metric_name, m.value AS value,
                   m.period AS period
            ORDER BY c.ticker, m.period DESC
        """)

        for record in result:
            ticker = record["ticker"]
            if ticker not in companies:
                companies[ticker] = {"ticker": ticker, "name": record["name"], "metrics": {}}

            metric_key = record["metric_name"]
            if metric_key not in companies[ticker]["metrics"]:
                companies[ticker]["metrics"][metric_key] = []
            companies[ticker]["metrics"][metric_key].append({
                "value": record["value"],
                "period": record["period"],
            })

    driver.close()
    logger.info(f"Extracted metrics for {len(companies)} companies")
    return companies


def compute_features(company: dict) -> dict:
    """Compute ML features from raw financial metrics."""

    metrics = company.get("metrics", {})

    def latest(metric_name: str, default: float = 0) -> float:
        vals = metrics.get(metric_name, [])
        if vals and vals[0].get("value") is not None:
            try:
                return float(vals[0]["value"])
            except (ValueError, TypeError):
                return default
        return default

    def growth(metric_name: str) -> float:
        vals = metrics.get(metric_name, [])
        if len(vals) >= 2:
            try:
                curr = float(vals[0]["value"])
                prev = float(vals[1]["value"])
                if prev != 0:
                    return (curr - prev) / abs(prev)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        return 0

    revenue = latest("Revenues") or latest("RevenueFromContractWithCustomerExcludingAssessedTax")
    net_income = latest("NetIncomeLoss")
    total_assets = latest("Assets")
    total_liabilities = latest("Liabilities")
    stockholder_equity = latest("StockholdersEquity")
    eps = latest("EarningsPerShareBasic")
    shares = latest("CommonStockSharesOutstanding")
    cash = latest("CashAndCashEquivalentsAtCarryingValue")
    operating_cf = latest("NetCashProvidedByOperatingActivities")

    features = {
        "revenue": revenue,
        "net_income": net_income,
        "total_assets": total_assets,
        "equity": stockholder_equity,

        # Profitability
        "profit_margin": net_income / revenue if revenue else 0,
        "roe": net_income / stockholder_equity if stockholder_equity else 0,
        "roa": net_income / total_assets if total_assets else 0,

        # Leverage
        "debt_to_equity": total_liabilities / stockholder_equity if stockholder_equity else 0,
        "debt_to_assets": total_liabilities / total_assets if total_assets else 0,

        # Liquidity
        "cash_ratio": cash / total_liabilities if total_liabilities else 0,

        # Growth
        "revenue_growth": growth("Revenues") or growth("RevenueFromContractWithCustomerExcludingAssessedTax"),
        "income_growth": growth("NetIncomeLoss"),
        "asset_growth": growth("Assets"),

        # Per share
        "eps": eps,
        "book_value_per_share": stockholder_equity / shares if shares else 0,

        # Cash flow
        "operating_cf": operating_cf,
        "cf_to_revenue": operating_cf / revenue if revenue else 0,

        # Count of metrics available (data quality signal)
        "num_metrics": len(metrics),
    }

    return features


def compute_health_score(features: dict) -> float:
    """
    Rule-based health score (0–100) used as training label.
    Combines profitability, leverage, growth, and cash flow signals.
    """
    score = 50  # Start at neutral

    # Profitability (+/- 20 pts)
    if features["profit_margin"] > 0.15:
        score += 15
    elif features["profit_margin"] > 0.05:
        score += 8
    elif features["profit_margin"] < 0:
        score -= 15

    if features["roe"] > 0.15:
        score += 5
    elif features["roe"] < 0:
        score -= 5

    # Leverage (+/- 15 pts)
    if features["debt_to_equity"] < 0.5:
        score += 10
    elif features["debt_to_equity"] > 2:
        score -= 10

    if features["cash_ratio"] > 0.3:
        score += 5
    elif features["cash_ratio"] < 0.05:
        score -= 5

    # Growth (+/- 15 pts)
    if features["revenue_growth"] > 0.1:
        score += 10
    elif features["revenue_growth"] < -0.1:
        score -= 10

    if features["income_growth"] > 0.1:
        score += 5
    elif features["income_growth"] < -0.1:
        score -= 5

    # Cash flow (+/- 10 pts)
    if features["cf_to_revenue"] > 0.15:
        score += 10
    elif features["operating_cf"] < 0:
        score -= 10

    return max(0, min(100, score))


def train_model():
    """Train LightGBM health scorer."""

    logger.info("━" * 50)
    logger.info("Financial Health Scorer — Training")
    logger.info("━" * 50)

    companies = extract_metrics()

    if len(companies) < 5:
        logger.error("Not enough companies with metrics to train")
        return

    # Build feature matrix
    X, y, tickers = [], [], []
    feature_names = None

    for ticker, company in companies.items():
        features = compute_features(company)
        label = compute_health_score(features)

        if feature_names is None:
            feature_names = list(features.keys())

        X.append([features[f] for f in feature_names])
        y.append(label)
        tickers.append(ticker)

    X = np.array(X, dtype=np.float32)
    y = np.array(y, dtype=np.float32)

    # Replace NaN/Inf
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    logger.info(f"Training on {len(X)} companies, {len(feature_names)} features")

    try:
        import lightgbm as lgb

        model = lgb.LGBMRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            verbose=-1,
        )
        model.fit(X, y)

        # Feature importance
        importances = sorted(
            zip(feature_names, model.feature_importances_),
            key=lambda x: x[1], reverse=True,
        )
        logger.info("Feature importance:")
        for name, imp in importances[:10]:
            logger.info(f"  {name}: {imp}")

    except ImportError:
        logger.warning("LightGBM not installed — using basic scoring")
        model = None

    # Save model and metadata
    model_data = {
        "model": model,
        "feature_names": feature_names,
        "scores": {t: float(s) for t, s in zip(tickers, y)},
    }
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model_data, f)

    logger.info(f"✅ Model saved to {MODEL_PATH}")

    # Print scores
    logger.info("\nCompany Health Scores:")
    scored = sorted(zip(tickers, y), key=lambda x: x[1], reverse=True)
    for ticker, score in scored:
        bar = "█" * int(score / 5) + "░" * (20 - int(score / 5))
        logger.info(f"  {ticker:12s} {bar} {score:.0f}/100")

    logger.info("━" * 50)
    logger.info("Financial Health Scorer — Complete ✓")
    logger.info("━" * 50)


def score_all():
    """Score all companies and save to Neo4j."""
    if not os.path.exists(MODEL_PATH):
        logger.error("No trained model found. Run training first.")
        return

    with open(MODEL_PATH, "rb") as f:
        model_data = pickle.load(f)

    scores = model_data["scores"]
    driver = Database.neo4j()

    with driver.session(database="atlas") as s:
        for ticker, score in scores.items():
            s.run("""
                MATCH (c:Company {ticker: $ticker})
                SET c.health_score = $score
            """, ticker=ticker, score=score)

    driver.close()
    logger.info(f"✅ Updated health scores for {len(scores)} companies in Neo4j")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--score", action="store_true", help="Score companies and save to Neo4j")
    args = parser.parse_args()

    if args.score:
        score_all()
    else:
        train_model()
