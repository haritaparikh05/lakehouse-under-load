"""
Publishes Nexmark-style events to Kafka at a configured rate.

Usage:
    python -m generator.producer --rate 200 --duration 30
"""
import argparse
import json
import time

from confluent_kafka import Producer

from generator.nexmark_generator import NexmarkGenerator


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for {msg.topic()}: {err}")


def run(bootstrap_servers: str, rate: float, duration: float, seed):
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    generator = NexmarkGenerator(seed=seed)

    interval = 1.0 / rate
    start = time.monotonic()
    next_send = start
    sent = 0

    while time.monotonic() - start < duration:
        topic, event = generator.next_event()
        producer.produce(topic, value=json.dumps(event).encode("utf-8"), callback=delivery_report)
        producer.poll(0)
        sent += 1

        next_send += interval
        sleep_for = next_send - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

        if sent % 1000 == 0:
            elapsed = time.monotonic() - start
            print(f"[producer] sent {sent} events in {elapsed:.1f}s ({sent / elapsed:.1f}/s)")

    producer.flush(10)
    elapsed = time.monotonic() - start
    print(f"[producer] done: {sent} events in {elapsed:.1f}s ({sent / elapsed:.1f}/s average)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nexmark-style Kafka event producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=100.0, help="events per second")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds to run")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    run(args.bootstrap_servers, args.rate, args.duration, args.seed)
