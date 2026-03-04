"""
Build Features — ClickHouse materialized views for ML feature engineering

Creates technical indicators (RSI, MACD, Bollinger, ATR, OBV) from market ticks
and rolling sentiment aggregations from news data.

Usage:
    python scripts/build_features.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connections import Database
from loguru import logger


def build_clickhouse_features():
    """Create ClickHouse tables and views for ML features."""

    ch = Database.clickhouse()
    logger.info("━" * 50)
    logger.info("Feature Engineering — ClickHouse")
    logger.info("━" * 50)

    # ── 1. OHLCV Daily (from ticks) ──
    logger.info("Creating daily OHLCV table...")
    ch.command("""
        CREATE TABLE IF NOT EXISTS ohlcv_daily (
            symbol        String,
            trade_date    Date,
            open          Float64,
            high          Float64,
            low           Float64,
            close         Float64,
            volume        Float64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (symbol, trade_date)
    """)

    ch.command("""
        INSERT INTO ohlcv_daily
        SELECT
            symbol,
            toDate(timestamp) AS trade_date,
            argMin(price, timestamp) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, timestamp) AS close,
            sum(volume) AS volume
        FROM market_ticks
        GROUP BY symbol, trade_date
        HAVING trade_date > '2000-01-01'
    """)
    logger.info("  ✅ ohlcv_daily populated")

    # ── 2. Technical Indicators Table ──
    logger.info("Creating technical indicators table...")
    ch.command("""
        CREATE TABLE IF NOT EXISTS technical_indicators (
            symbol          String,
            trade_date      Date,
            close           Float64,
            volume          Float64,

            -- Moving Averages
            sma_10          Float64,
            sma_20          Float64,
            sma_50          Float64,
            ema_12          Float64,
            ema_26          Float64,

            -- RSI components
            avg_gain_14     Float64,
            avg_loss_14     Float64,
            rsi_14          Float64,

            -- MACD
            macd_line       Float64,
            macd_signal     Float64,
            macd_histogram  Float64,

            -- Bollinger Bands
            bb_upper        Float64,
            bb_middle       Float64,
            bb_lower        Float64,
            bb_width        Float64,

            -- Volatility
            atr_14          Float64,
            daily_return    Float64,
            volatility_20   Float64,

            -- Volume
            obv             Float64,
            volume_sma_20   Float64,
            volume_ratio    Float64,

            -- Momentum
            roc_10          Float64,
            momentum_10     Float64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (symbol, trade_date)
    """)

    # Populate with window functions
    ch.command("""
        INSERT INTO technical_indicators
        SELECT
            symbol,
            trade_date,
            close,
            volume,

            -- SMAs
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 9 PRECEDING) AS sma_10,
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS sma_20,
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 49 PRECEDING) AS sma_50,

            -- EMA approximations (using SMA as proxy — true EMA needs iterative calc)
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 11 PRECEDING) AS ema_12,
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 25 PRECEDING) AS ema_26,

            -- RSI components (14-period)
            avg(CASE WHEN close > lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                THEN close - lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                ELSE 0 END)
            OVER (PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING) AS avg_gain_14,

            avg(CASE WHEN close < lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                THEN lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) - close
                ELSE 0 END)
            OVER (PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING) AS avg_loss_14,

            -- RSI
            CASE WHEN avg(CASE WHEN close < lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                THEN lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) - close
                ELSE 0 END) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING) = 0
                THEN 100
                ELSE 100 - (100 / (1 + (
                    avg(CASE WHEN close > lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                        THEN close - lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                        ELSE 0 END) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING) /
                    avg(CASE WHEN close < lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                        THEN lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) - close
                        ELSE 0 END) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING)
                )))
            END AS rsi_14,

            -- MACD
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 11 PRECEDING) -
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 25 PRECEDING) AS macd_line,

            avg(
                avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 11 PRECEDING) -
                avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 25 PRECEDING)
            ) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 8 PRECEDING) AS macd_signal,

            (avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 11 PRECEDING) -
             avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 25 PRECEDING)) -
            avg(
                avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 11 PRECEDING) -
                avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 25 PRECEDING)
            ) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 8 PRECEDING) AS macd_histogram,

            -- Bollinger Bands (20-period)
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) +
                2 * stddevPop(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS bb_upper,
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS bb_middle,
            avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) -
                2 * stddevPop(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS bb_lower,
            4 * stddevPop(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) /
                avg(close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS bb_width,

            -- ATR (14-period, simplified using daily range)
            avg(high - low) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 13 PRECEDING) AS atr_14,

            -- Daily return
            (close - lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)) /
                lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) AS daily_return,

            -- 20-day volatility (std of returns)
            stddevPop(
                (close - lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)) /
                lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
            ) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS volatility_20,

            -- OBV (simplified — cumulative signed volume)
            sum(CASE
                WHEN close > lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) THEN volume
                WHEN close < lagInFrame(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) THEN -volume
                ELSE 0
            END) OVER (PARTITION BY symbol ORDER BY trade_date ROWS UNBOUNDED PRECEDING) AS obv,

            -- Volume SMA
            avg(volume) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS volume_sma_20,

            -- Volume ratio
            volume / avg(volume) OVER (PARTITION BY symbol ORDER BY trade_date ROWS 19 PRECEDING) AS volume_ratio,

            -- Rate of Change (10-period)
            (close - lagInFrame(close, 10) OVER (PARTITION BY symbol ORDER BY trade_date)) /
                lagInFrame(close, 10) OVER (PARTITION BY symbol ORDER BY trade_date) * 100 AS roc_10,

            -- Momentum (10-period)
            close - lagInFrame(close, 10) OVER (PARTITION BY symbol ORDER BY trade_date) AS momentum_10

        FROM ohlcv_daily
        WHERE trade_date > '2000-01-01'
        ORDER BY symbol, trade_date
    """)
    logger.info("  ✅ technical_indicators populated")

    # ── 3. Rolling Sentiment per Ticker ──
    logger.info("Creating rolling sentiment views...")
    ch.command("""
        CREATE TABLE IF NOT EXISTS sentiment_rolling (
            ticker          String,
            bucket_date     Date,
            avg_sentiment_1d  Float64,
            avg_sentiment_7d  Float64,
            avg_sentiment_30d Float64,
            article_count_1d  UInt32,
            article_count_7d  UInt32,
            sentiment_momentum Float64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, bucket_date)
    """)

    ch.command("""
        INSERT INTO sentiment_rolling
        SELECT
            ticker,
            bucket_date,
            avg_s_1d,
            avg(avg_s_1d) OVER (PARTITION BY ticker ORDER BY bucket_date ROWS 6 PRECEDING) AS avg_sentiment_7d,
            avg(avg_s_1d) OVER (PARTITION BY ticker ORDER BY bucket_date ROWS 29 PRECEDING) AS avg_sentiment_30d,
            cnt_1d,
            sum(cnt_1d) OVER (PARTITION BY ticker ORDER BY bucket_date ROWS 6 PRECEDING) AS article_count_7d,
            avg_s_1d - avg(avg_s_1d) OVER (PARTITION BY ticker ORDER BY bucket_date ROWS 6 PRECEDING) AS sentiment_momentum
        FROM (
            SELECT
                ticker,
                toDate(timestamp) AS bucket_date,
                avg(sentiment) AS avg_s_1d,
                count() AS cnt_1d
            FROM news_sentiment_ts
            GROUP BY ticker, bucket_date
        )
        ORDER BY ticker, bucket_date
    """)
    logger.info("  ✅ sentiment_rolling populated")

    # ── 4. GDELT Event Spike Detector ──
    logger.info("Creating GDELT spike detector...")
    ch.command("""
        CREATE TABLE IF NOT EXISTS gdelt_spikes (
            country         String,
            event_code      String,
            spike_date      Date,
            daily_count     UInt32,
            avg_count_30d   Float64,
            spike_ratio     Float64,
            avg_tone        Float64,
            avg_goldstein   Float64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (country, spike_date, event_code)
    """)

    ch.command("""
        INSERT INTO gdelt_spikes
        SELECT
            country,
            event_code,
            spike_date,
            daily_count,
            avg_30d,
            daily_count / greatest(avg_30d, 1) AS spike_ratio,
            tone,
            goldstein
        FROM (
            SELECT
                country,
                event_code,
                toDate(bucket_hour) AS spike_date,
                sum(event_count) AS daily_count,
                avg(sum(event_count)) OVER (
                    PARTITION BY country, event_code
                    ORDER BY toDate(bucket_hour)
                    ROWS 29 PRECEDING
                ) AS avg_30d,
                avg(avg_tone) AS tone,
                avg(avg_goldstein) AS goldstein
            FROM gdelt_event_volume
            GROUP BY country, event_code, toDate(bucket_hour)
        )
        WHERE daily_count > 0
        ORDER BY country, spike_date
    """)
    logger.info("  ✅ gdelt_spikes populated")

    # ── Summary ──
    for table in ["ohlcv_daily", "technical_indicators", "sentiment_rolling", "gdelt_spikes"]:
        count = ch.command(f"SELECT count() FROM {table}")
        logger.info(f"  {table}: {count} rows")

    ch.close()
    logger.info("━" * 50)
    logger.info("Feature Engineering — Complete ✓")
    logger.info("━" * 50)


if __name__ == "__main__":
    build_clickhouse_features()
