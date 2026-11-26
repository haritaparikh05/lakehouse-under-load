"""
Step 6, sink 2 of 3: stream the `bid` topic into a Delta Lake table.

Same bid stream, same event_date partitioning, same metrics/results
pattern as sink_parquet.py -- only the output format differs. This is
the pairing needed for Step 7's write-performance comparison: identical
input, identical partitioning, different storage format.

Run with: python -m streaming.sink_delta --duration 60
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

OUTPUT_PATH = "./warehouse/delta/bid"
CHECKPOINT_PATH = "./checkpoints/sink_delta"


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
    spark = get_spark_session("sink-delta")

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
        .withColumn("event_date", F.to_date("event_time"))
    )

    listener = ThroughputListener()
    spark.streams.addListener(listener)

    query = (
        bids.writeStream.outputMode("append")
        .format("delta")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("event_date")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"[sink-delta] running for {duration_seconds:.0f}s...")
    time.sleep(duration_seconds)

    print("[sink-delta] stopping query...")
    query.stop()
    print("[sink-delta] stopped cleanly")

    stats = listener.summary()

    result = {
        "sink": "delta",
        "run_started_utc": run_started.isoformat(),
        "duration_seconds": duration_seconds,
        **stats,
    }
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / f"sink_delta_{run_started.strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"[sink-delta] results written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60.0)
    args = parser.parse_args()
    main(args.duration)
