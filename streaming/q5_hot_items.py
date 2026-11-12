"""
Nexmark Q5 (Hot Items) with throughput/latency metrics.

On top of the Step 3 windowed bid count, this adds:
  - per-micro-batch throughput, captured via a StreamingQueryListener
    (Spark's own internal progress reporting -- not something we're
    estimating ourselves)
  - an ingestion-latency estimate (processing time minus the event's
    own date_time), averaged per (window, auction) group per batch
  - a fixed-duration controlled run: sleeps for --duration seconds,
    then calls query.stop() directly, rather than relying on Ctrl+C.
    (PySpark's SIGINT handler has a known reentrancy bug when it fires
    while awaitTermination() is mid-read on the same py4j connection --
    calling stop() directly from our own code sidesteps it entirely.)

Run with: python -m streaming.q5_hot_items --duration 120
"""
import argparse
import time

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

WINDOW_DURATION = "1 minute"
WINDOW_SLIDE = "30 seconds"
WATERMARK_DELAY = "1 minute"


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


def main(duration_seconds: float):
    spark = get_spark_session("q5-hot-items")

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

    bids_with_event_time = bids.withColumn(
        "event_time", (F.col("date_time") / 1000).cast("timestamp")
    ).withColumn(
        "ingestion_latency_sec",
        F.unix_timestamp(F.current_timestamp()) - (F.col("date_time") / 1000),
    )

    hot_items = (
        bids_with_event_time.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(F.window("event_time", WINDOW_DURATION, WINDOW_SLIDE), "auction")
        .agg(
            F.count("*").alias("bid_count"),
            F.avg("ingestion_latency_sec").alias("avg_latency_sec"),
            F.max("ingestion_latency_sec").alias("max_latency_sec"),
        )
    )

    listener = ThroughputListener()
    spark.streams.addListener(listener)

    query = (
        hot_items.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "./checkpoints/q5_hot_items")
        .start()
    )

    print(f"[q5] running for {duration_seconds:.0f}s to collect metrics...")
    time.sleep(duration_seconds)

    print("[q5] stopping query...")
    query.stop()
    print("[q5] stopped cleanly")

    listener.summary()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=120.0)
    args = parser.parse_args()
    main(args.duration)
