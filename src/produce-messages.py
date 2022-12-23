import logging
import sys
from time import sleep
from uuid import uuid4

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField

from shared import Settings, get_latest_schemas

stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)


if __name__ == "__main__":
    settings = Settings()
    key_schema, value_schema = get_latest_schemas(settings)

    # Build producer
    avroProducer = AvroProducer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_url,
            "schema.registry.url": settings.schema_registry_url,
        }
    )

    # Produce messages
    ids = [str(uuid4()) for _ in range(10)]
    record_types = ["truck", "trailer", "driver"]

    try:
        for i, record_id in enumerate(ids):
            record_type = record_types[i % len(record_types)]
            if record_type == "truck":
                value = {
                    "id": record_id,
                    "license_plate": f"ABC{i}",
                    "type": f"TruckType{i}",
                    "body_type": f"BodyType{i}",
                }
            elif record_type == "trailer":
                value = {
                    "id": record_id,
                    "license_plate": f"ABC{i}",
                    "body_type": f"BodyType{i}",
                }
            else:
                value = {
                    "id": record_id,
                    "phone_number": f"{i}012345",
                    "name": f"name-{i}",
                }

            value = {"data": value}

            key = {"record_id": record_id}

            logger.info(f"key: {key} value: {value}")

            avroProducer.produce(
                topic=settings.kafka_topic,
                value=value,
                key=key,
                key_schema=key_schema,
                value_schema=value_schema,
            )
            sleep(0.01)

        avroProducer.flush()
    except Exception as e:
        logger.exception(e)
