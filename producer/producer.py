import csv
import json
import os
import time
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaProducer


# Конфігурація

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker1:9092,broker2:9092"
)

TOPIC_1 = os.getenv("KAFKA_TOPIC_1", "Topic1")
TOPIC_2 = os.getenv("KAFKA_TOPIC_2", "Topic2")

CSV_PATH = os.getenv(
    "CSV_PATH",
    "/app/data/Divvy_Trips_2019_Q4.csv"
)

SEND_DELAY_SECONDS = float(
    os.getenv("SEND_DELAY_SECONDS", "0.0")
)

MAX_RECORDS = int(
    os.getenv("MAX_RECORDS", "0")
)


# Допоміжні функції

def get_bootstrap_servers() -> list[str]:
    """Повертає список Kafka broker'ів."""
    return [server.strip() for server in BOOTSTRAP_SERVERS.split(",")]


def create_event(row_number: int, row: Dict[str, Any]) -> Dict[str, Any]:
    """Створює Kafka event."""
    return {
        "event_id": row_number,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "payload": row,
    }


def validate_csv_file() -> None:
    """Перевіряє існування CSV файлу."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")


# Kafka Producer

def build_producer(
    max_attempts: int = 20,
    pause_seconds: float = 3.0
) -> KafkaProducer:
    """
    Створює Kafka producer з повторними спробами підключення.
    """
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=get_bootstrap_servers(),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks="all",
                retries=5,
            )

            print(
                f"[PRODUCER] Connected to Kafka "
                f"on attempt {attempt}."
            )

            return producer

        except Exception as exc:  # noqa: BLE001
            last_error = exc

            print(
                f"[PRODUCER] Kafka connection failed "
                f"(attempt {attempt}/{max_attempts}): {exc}"
            )

            time.sleep(pause_seconds)

    raise RuntimeError(
        f"Could not connect to Kafka after "
        f"{max_attempts} attempts: {last_error}"
    )


# Відправка повідомлень

def send_event(
    producer: KafkaProducer,
    key: str,
    event: Dict[str, Any]
) -> None:
    """Відправляє event у два Kafka topic'и."""

    producer.send(TOPIC_1, key=key, value=event)
    producer.send(TOPIC_2, key=key, value=event)

    print(
        f"[PRODUCER] Sent event_id={event['event_id']} "
        f"to {TOPIC_1} and {TOPIC_2}"
    )


def process_csv_rows(producer: KafkaProducer) -> int:
    """
    Зчитує CSV та відправляє записи в Kafka.
    Повертає кількість відправлених повідомлень.
    """
    sent_records = 0

    with open(CSV_PATH, "r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)

        for row_number, row in enumerate(reader, start=1):
            event = create_event(row_number, row)
            key = str(row_number)

            send_event(producer, key, event)

            sent_records += 1

            # Перевірка ліміту записів
            if MAX_RECORDS > 0 and sent_records >= MAX_RECORDS:
                print(
                    f"[PRODUCER] MAX_RECORDS={MAX_RECORDS} reached. "
                    f"Stopping producer."
                )
                break

            # Затримка між повідомленнями
            if SEND_DELAY_SECONDS > 0:
                time.sleep(SEND_DELAY_SECONDS)

    return sent_records


# Основна функція

def produce_rows() -> None:
    """Основний процес producer'а."""

    validate_csv_file()

    producer = build_producer()

    try:
        total_sent = process_csv_rows(producer)

        producer.flush()

        print(
            f"[PRODUCER] Completed. "
            f"Total events sent: {total_sent}"
        )

    finally:
        producer.close()


# Entry Point

if __name__ == "__main__":
    produce_rows()