"""
Apache Spark analytics jobs for Acme Financial Data Warehouse.

Two jobs:
  1. spark_aggregations(series_id)  — compute min/max/avg/stddev using Spark DataFrames
  2. spark_predict_next_day(series_id) — train a linear regression model with Spark MLlib
                                         and predict the next closing price

Requirements:
    pip install pyspark

Usage (standalone):
    python3.11 -m analytics.spark_jobs --series-id <series_id>

Or called from the REST API / MCP tool.
"""
from __future__ import annotations

import os
import sys
import argparse
import json
from datetime import datetime, timedelta
from typing import Any

# ── PySpark imports ───────────────────────────────────────────────────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType, TimestampType
    )
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.evaluation import RegressionEvaluator
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


# ── Spark session factory ─────────────────────────────────────────────────────

def _get_spark(app_name: str = "AcmeFinancialDW") -> "SparkSession":
    if not SPARK_AVAILABLE:
        raise RuntimeError(
            "PySpark is not installed. Run: pip install pyspark"
        )
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Schema ────────────────────────────────────────────────────────────────────

TS_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("open",      DoubleType(),    True),
    StructField("high",      DoubleType(),    True),
    StructField("low",       DoubleType(),    True),
    StructField("close",     DoubleType(),    False),
    StructField("volume",    LongType(),      True),
])


# ── Job 1: Aggregations ───────────────────────────────────────────────────────

def spark_aggregations(series_id: str) -> dict[str, Any]:
    """
    Read time-series points for `series_id` from MongoDB via the repository,
    load them into a Spark DataFrame, and compute aggregations using Spark SQL.

    Returns a dict with min/max/avg/stddev for close prices and volume.
    """
    from db.repository import get_ts_points

    points = get_ts_points(series_id, limit=100_000)
    if not points:
        return {"error": "No data found for series", "series_id": series_id}

    spark = _get_spark("AcmeAggregations")

    # Build rows for Spark
    rows = [
        (
            p["timestamp"],
            float(p.get("open") or 0),
            float(p.get("high") or 0),
            float(p.get("low") or 0),
            float(p.get("close") or 0),
            int(p.get("volume") or 0),
        )
        for p in points
    ]

    df = spark.createDataFrame(rows, schema=TS_SCHEMA)
    df.createOrReplaceTempView("time_series")

    # Spark SQL aggregations
    agg_df = spark.sql("""
        SELECT
            COUNT(*)            AS count,
            MIN(close)          AS min_close,
            MAX(close)          AS max_close,
            AVG(close)          AS avg_close,
            STDDEV(close)       AS std_close,
            MIN(volume)         AS min_volume,
            MAX(volume)         AS max_volume,
            AVG(volume)         AS avg_volume,
            MIN(timestamp)      AS from_date,
            MAX(timestamp)      AS to_date
        FROM time_series
    """)

    row = agg_df.collect()[0]

    result = {
        "engine": "Apache Spark",
        "series_id": series_id,
        "count": int(row["count"]),
        "from_date": str(row["from_date"]),
        "to_date": str(row["to_date"]),
        "close": {
            "min": round(float(row["min_close"]), 4),
            "max": round(float(row["max_close"]), 4),
            "avg": round(float(row["avg_close"]), 4),
            "std": round(float(row["std_close"] or 0), 4),
        },
        "volume": {
            "min": int(row["min_volume"]),
            "max": int(row["max_volume"]),
            "avg": round(float(row["avg_volume"]), 2),
        },
    }

    spark.catalog.dropTempView("time_series")
    return result


# ── Job 2: Spark MLlib Linear Regression Prediction ───────────────────────────

def spark_predict_next_day(series_id: str, lookback: int = 60) -> dict[str, Any]:
    """
    Train a Spark MLlib LinearRegression model on the last `lookback` closing prices
    and predict the next day's closing price.

    Features: [day_index] → Label: close
    """
    from db.repository import get_ts_points

    points = get_ts_points(series_id, limit=100_000)
    if len(points) < 10:
        return {"error": "Not enough data for Spark ML prediction", "series_id": series_id}

    recent = points[-lookback:]
    spark = _get_spark("AcmeMLPrediction")

    # Build feature rows: (day_index float, close float)
    rows = [(float(i), float(p["close"])) for i, p in enumerate(recent)]

    schema = StructType([
        StructField("day_index", DoubleType(), False),
        StructField("close",     DoubleType(), False),
    ])
    df = spark.createDataFrame(rows, schema=schema)

    # Assemble features vector
    assembler = VectorAssembler(inputCols=["day_index"], outputCol="features")
    df_feat = assembler.transform(df)

    # Train / test split (80/20)
    train_df, test_df = df_feat.randomSplit([0.8, 0.2], seed=42)
    if train_df.count() < 2:
        train_df = df_feat  # use all data if too small

    # Train Spark MLlib Linear Regression
    lr = LinearRegression(featuresCol="features", labelCol="close",
                          maxIter=100, regParam=0.01, elasticNetParam=0.0)
    model = lr.fit(train_df)

    # Predict next day (day_index = len(recent))
    next_idx = float(len(recent))
    next_row = [(next_idx,)]
    next_schema = StructType([StructField("day_index", DoubleType(), False)])
    next_df = spark.createDataFrame(next_row, schema=next_schema)
    next_feat = assembler.transform(next_df)
    prediction_df = model.transform(next_feat)
    predicted_close = prediction_df.collect()[0]["prediction"]

    last_close = recent[-1]["close"]
    last_ts: datetime = recent[-1]["timestamp"]
    next_date = (last_ts + timedelta(days=1)).date()

    # Evaluate on test set if available
    rmse = None
    if test_df.count() >= 2:
        evaluator = RegressionEvaluator(labelCol="close", predictionCol="prediction",
                                        metricName="rmse")
        test_predictions = model.transform(test_df)
        rmse = round(evaluator.evaluate(test_predictions), 4)

    result = {
        "engine": "Apache Spark MLlib (LinearRegression)",
        "series_id": series_id,
        "lookback_points": len(recent),
        "last_close": round(float(last_close), 4),
        "last_date": str(last_ts.date() if hasattr(last_ts, "date") else last_ts),
        "predicted_close": round(float(predicted_close), 4),
        "predicted_date": str(next_date),
        "change_pct": round((predicted_close - last_close) / last_close * 100, 3),
        "model": {
            "coefficients": [round(float(c), 6) for c in model.coefficients],
            "intercept": round(float(model.intercept), 4),
            "rmse_test": rmse,
        },
    }

    return result


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark analytics jobs")
    parser.add_argument("--series-id", required=True, help="MongoDB series_id")
    parser.add_argument("--job", choices=["agg", "predict", "both"], default="both")
    parser.add_argument("--lookback", type=int, default=60)
    args = parser.parse_args()

    if args.job in ("agg", "both"):
        print("\n=== Spark Aggregations ===")
        result = spark_aggregations(args.series_id)
        print(json.dumps(result, indent=2, default=str))

    if args.job in ("predict", "both"):
        print("\n=== Spark ML Prediction ===")
        result = spark_predict_next_day(args.series_id, lookback=args.lookback)
        print(json.dumps(result, indent=2, default=str))
