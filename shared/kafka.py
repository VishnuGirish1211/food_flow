"""Kafka connection helpers.

Provides factory functions for creating Kafka producers and consumers.
No base classes, no inheritance — just connection setup.
"""

import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


async def create_producer() -> AIOKafkaProducer:
    """Create and start a Kafka producer.

    Serializes values as JSON bytes. The caller is responsible
    for calling producer.stop() on shutdown.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    return producer


async def create_consumer(topics: list[str], group_id: str) -> AIOKafkaConsumer:
    """Create and start a Kafka consumer.

    Uses manual commit (enable_auto_commit=False) so consumers
    only commit after successful processing. Deserializes values
    from JSON bytes to Python dicts.

    The caller is responsible for calling consumer.stop() on shutdown.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    return consumer
