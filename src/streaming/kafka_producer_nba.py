"""src/streaming/kafka_producer_nba.py.

Produces NBA event messages to a Kafka topic.
"""

import csv
import os
from pathlib import Path
import time

from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header
from dotenv import load_dotenv

LOG = get_logger("P04_NBA", level="DEBUG")


def main() -> None:
    """Main entry point for the Kafka producer."""
    log_header(LOG, "P04_NBA")
    load_dotenv(override=True)
    settings = KafkaSettings.from_env()

    LOG.info(f"Preparing topic: {settings.topic}")
    prepare_producer_topic(settings)

    LOG.info(f"Creating producer for topic: {settings.topic}")
    producer = create_producer(settings)

    csv_path = Path.cwd() / "data" / "events.csv"
    LOG.info(f"Reading from {csv_path}")

    interval = float(os.getenv("PRODUCER_MESSAGE_INTERVAL_SECONDS", "0.1"))

    try:
        with csv_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                produce_kafka_message(
                    producer=producer,
                    topic=settings.topic,
                    key=row.get("play_id", "unknown"),
                    message=row,
                )
                LOG.info(f"Sent Event: {row['play_id']} | Action: {row['action_type']}")
                time.sleep(interval)
    except KeyboardInterrupt:
        LOG.warning("Producer interrupted by user.")
    finally:
        producer.flush()
        LOG.info("Producer flushed and closed.")


if __name__ == "__main__":
    main()
