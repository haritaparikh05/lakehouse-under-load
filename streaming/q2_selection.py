"""
Nexmark Q2 (Selection): keep bids for every auctionSkip'th auction,
projecting just (auction, price).

Predicate confirmed against Beam's reference implementation
(Query2.java / NexmarkConfiguration.java): `auction % auctionSkip == 0`,
default auctionSkip = 123.

Unlike Q5, this is a stateless filter -- no windowing, no watermark,
no aggregation. Same fixed-duration run pattern as Q5 (sleep then
stop() directly) to avoid the Ctrl+C/SIGINT reentrancy issue.

Run with: python -m streaming.q2_selection --duration 60
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

AUCTION_SKIP = 123


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
    spark = get_spark_session("q2-selection")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bid")
        .option("startingOffsets", "latest")
        .load()
    )

    bids = raw.select(
        F.from_json(F.col("value").cast("string"), BID_SCHEMA).alias("bid")
    ).select("bid.*")

    selected = bids.filter(F.col("auction") % AUCTION_SKIP == 0).select("auction", "price")

    listener = ThroughputListener()
    spark.streams.addListener(listener)

    query = (
        selected.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "./checkpoints/q2_selection")
        .start()
    )

    print(f"[q2] running for {duration_seconds:.0f}s to collect metrics...")
    time.sleep(duration_seconds)

    print("[q2] stopping query...")
    query.stop()
    print("[q2] stopped cleanly")

    stats = listener.summary()

    result = {
        "query": "q2_selection",
        "run_started_utc": run_started.isoformat(),
        "duration_seconds": duration_seconds,
        **stats,
    }
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / f"q2_selection_{run_started.strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"[q2] results written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60.0)
    args = parser.parse_args()
    main(args.duration)
