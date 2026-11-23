"""
Nexmark Q3 (Local Item Suggestion): join Auction to Person on seller/id,
filtered to category 10 and person state OR/ID/CA, projecting
(name, city, state, auction id).

Canonical SQL (from Beam's own Javadoc on Query3.java):
    SELECT P.name, P.city, P.state, A.id
    FROM Auction A, Person P
    WHERE A.seller = P.id
      AND (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA')
      AND A.category = 10

Deliberate simplification vs. Beam's reference implementation: Beam uses a
custom stateful DoFn that caches each person indefinitely per seller id and
expires that cache after maxAuctionsWaitingTime (600s of event time, via a
timer) -- a pattern with no direct Structured Streaming equivalent. We use
Spark's native stream-stream join instead: both sides carry a watermark,
and a one-directional time-range join condition (an auction's event time
falls within AUCTION_WAIT_MINUTES after its seller/person's event time)
bounds join state the same way Beam's timeout does. This relies on the
generator's own invariant that a person always exists in the stream before
it's ever referenced as a seller (same invariant the bid generator relies
on for bidder/auction references).

Run with: python -m streaming.q3_local_items --duration 60
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

PERSON_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("date_time", LongType()),
    ]
)

AUCTION_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("seller", LongType()),
        StructField("category", LongType()),
        StructField("date_time", LongType()),
    ]
)

CATEGORY = 10
STATES = ("OR", "ID", "CA")

WATERMARK_DELAY = "1 minute"
AUCTION_WAIT_MINUTES = 10  # matches Beam's maxAuctionsWaitingTime default (600s)


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
    spark = get_spark_session("q3-local-items")

    persons_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "person")
        .option("startingOffsets", "latest")
        .load()
    )
    persons = (
        persons_raw.select(F.from_json(F.col("value").cast("string"), PERSON_SCHEMA).alias("p"))
        .select("p.*")
        .filter(F.col("state").isin(STATES))
        .withColumn("event_time", (F.col("date_time") / 1000).cast("timestamp"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    auctions_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "auction")
        .option("startingOffsets", "latest")
        .load()
    )
    auctions = (
        auctions_raw.select(F.from_json(F.col("value").cast("string"), AUCTION_SCHEMA).alias("a"))
        .select("a.*")
        .filter(F.col("category") == CATEGORY)
        .withColumn("event_time", (F.col("date_time") / 1000).cast("timestamp"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    joined = (
        auctions.alias("a")
        .join(
            persons.alias("p"),
            F.expr(
                f"""
                a.seller = p.id
                AND a.event_time >= p.event_time
                AND a.event_time <= p.event_time + interval {AUCTION_WAIT_MINUTES} minutes
                """
            ),
        )
        .select(
            F.col("p.name").alias("name"),
            F.col("p.city").alias("city"),
            F.col("p.state").alias("state"),
            F.col("a.id").alias("auction_id"),
        )
    )

    listener = ThroughputListener()
    spark.streams.addListener(listener)

    query = (
        joined.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", "./checkpoints/q3_local_items")
        .start()
    )

    print(f"[q3] running for {duration_seconds:.0f}s to collect metrics...")
    time.sleep(duration_seconds)

    print("[q3] stopping query...")
    query.stop()
    print("[q3] stopped cleanly")

    stats = listener.summary()

    result = {
        "query": "q3_local_items",
        "run_started_utc": run_started.isoformat(),
        "duration_seconds": duration_seconds,
        **stats,
    }
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / f"q3_local_items_{run_started.strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"[q3] results written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60.0)
    args = parser.parse_args()
    main(args.duration)
