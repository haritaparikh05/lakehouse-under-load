"""
Step 6, sink 3 of 3: stream the `bid` topic into an Iceberg table.

Same bid stream as sink_parquet.py/sink_delta.py, but structurally
different in three ways, confirmed against Iceberg's own Spark
Structured Streaming docs:

  - the target table must already exist -- Iceberg's streaming sink
    does not auto-create it, so this script issues a CREATE TABLE
    before starting the stream (Parquet/Delta sinks needed no such
    step; outputMode("append") + a path was enough there)
  - partitioning is "hidden": instead of an explicit event_date column
    plus .partitionBy() on the write, the table is created with
    PARTITIONED BY (days(event_time)) directly on the timestamp column
    -- Iceberg derives the partition value itself and tracks it in
    table metadata, not in the physical file path
  - the docs recommend a trigger interval of at least 1 minute for
    streaming writes (to avoid excessive small-file commits), so this
    sink uses a 60-second trigger and a longer default run duration,
    unlike the 10-second trigger used for Parquet/Delta

Run with: python -m streaming.sink_iceberg --duration 180
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import LongType, StringType, StructField, StructType

from streaming.spark_session import get_spark_session

BID_SCHEMA = StructType(
    [
        StructField("auction", LongType()),
        StructField("bidder", LongType()),
        StructField("price", LongType()),
        StructField("date_time", LongType()),
        StructField("extra", StringType(), True),
    ]
)

TABLE_NAME = "local.db.bid"
CHECKPOINT_PATH = "./checkpoints/sink_iceberg"


class ThroughputListener(StreamingQueryListener):
    """Captures Spark's own per-micro-batch progress metrics."""

    def __init__(self):
        self.batches = []

    def onQueryStarted(self, event):
        print(f"[metrics] query started: {event.id}")

    def onQueryProgress(self, event):
        p = event.progress
        self.batches.append(p)
        print(
            f"[metrics] batch {p.batchId}: {p.numInputRows} rows, "
            f"{p.inputRowsPerSecond:.1f} in/s, "
            f"{p.processedRowsPerSecond:.1f} processed/s"
        )

    def onQueryTerminated(self, event):
        print(f"[metrics] query terminated: {event.id}")

    def summary(self):
        total_rows = sum(b.numInputRows for b in self.batches)
        rates = [b.inputRowsPerSecond for b in self.batches if b.inputRowsPerSecond > 0]
        avg_rate = sum(rates) / len(rates) if rates else 0.0
        print("---- throughput summary ----")
        print(f"batches observed: {len(self.batches)}")
        print(f"total input rows: {total_rows}")
        print(f"average input rate: {avg_rate:.1f} rows/sec")
        return {
            "batches_observed": len(self.batches),
            "total_input_rows": total_rows,
            "average_input_rate_rows_per_sec": round(avg_rate, 1),
        }


def main(duration_seconds: float):
    run_started = datetime.now(timezone.utc)
    spark = get_spark_session("sink-iceberg")

    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            date_time BIGINT,
            extra STRING,
            event_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        """
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bid")
        .option("startingOffsets", "latest")
        .load()
    )

    bids = (
        raw.select(F.from_json(F.col("value").cast("string"), BID_SCHEMA).alias("bid"))
        .select("bid.*")
        .withColumn("event_time", (F.col("date_time") / 1000).cast("timestamp"))
    )

    listener = ThroughputListener()
    spark.streams.addListener(listener)

    query = (
        bids.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("fanout-enabled", "true")
        .trigger(processingTime="60 seconds")
        .toTable(TABLE_NAME)
    )

    print(f"[sink-iceberg] running for {duration_seconds:.0f}s...")
    time.sleep(duration_seconds)

    print("[sink-iceberg] stopping query...")
    query.stop()
    print("[sink-iceberg] stopped cleanly")

    stats = listener.summary()

    result = {
        "sink": "iceberg",
        "run_started_utc": run_started.isoformat(),
        "duration_seconds": duration_seconds,
        **stats,
    }
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / f"sink_iceberg_{run_started.strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"[sink-iceberg] results written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=180.0)
    args = parser.parse_args()
    main(args.duration)
