"""src/streaming/kafka_consumer_nba.py.

Consumes NBA event messages, validates data contracts, and plots live analytics.
"""

import csv
import os
from pathlib import Path

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger
from dotenv import load_dotenv

from streaming.data_validation.data_contract_nba import (
    CONSUMED_FIELDNAMES,
    validate_nba_record,
)
from streaming.visualizations.live_visualizations_nba import (
    close_live_chart,
    init_live_chart,
    save_live_chart,
    update_live_chart,
)

LOG = get_logger("C04_NBA", level="DEBUG")

TIMEOUT_SECONDS = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))
OUTPUT_CSV = Path("data/output/consumed_nba_events.csv")
OUTPUT_CHART = Path("data/output/nba_events_chart.png")
PLAYERS_CSV = Path("data/players.csv")


def main() -> None:
    """Main entry point for the Kafka consumer."""
    load_dotenv(override=True)
    settings = KafkaSettings.from_env()

    LOG.info("Verifying Kafka connection...")
    verify_kafka_connection(settings)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    LOG.info("Loading players reference data...")
    player_to_team = {}
    if PLAYERS_CSV.exists():
        with PLAYERS_CSV.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                player_to_team[row["player_id"]] = row["team_name"]

    LOG.info("Initializing NBA live clustered chart...")
    figure, axis, team_stats = init_live_chart()

    LOG.info(f"Subscribing to topic: {settings.topic}")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [TopicPartition(p.topic, p.partition, OFFSET_BEGINNING) for p in partitions]
        ),
    )

    consumed_count = 0
    try:
        while consumed_count < MAX_MESSAGES:
            row = consume_kafka_message(
                consumer=consumer, timeout_seconds=TIMEOUT_SECONDS
            )
            if row is None:
                break

            validation = validate_nba_record(row)
            if not validation.is_valid:
                LOG.warning(f"Invalid record bypassed: {validation.errors}")
                continue

            player_id = row.get("player_id", "")
            team = player_to_team.get(player_id, "Unknown")
            action_type = row.get("action_type", "")

            if team not in ["OKC", "SAS"] or action_type not in [
                "Rebound",
                "Foul",
                "Turnover",
            ]:
                continue

            update_live_chart(
                figure=figure,
                axis=axis,
                team_stats=team_stats,
                player_to_team=player_to_team,
                message=row,
            )

            append_csv_row(
                path=OUTPUT_CSV,
                row={f: row.get(f, "") for f in CONSUMED_FIELDNAMES},
                fieldnames=CONSUMED_FIELDNAMES,
            )
            consumed_count += 1
            LOG.info(
                f"Accepted NBA event: {row['play_id']} | Team: {team} | Action: {action_type}"
            )

        save_live_chart(figure=figure, chart_path=OUTPUT_CHART)
    finally:
        consumer.close()
        close_live_chart()


if __name__ == "__main__":
    main()
