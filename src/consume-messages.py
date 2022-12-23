import logging
import sys
from typing import Dict, List, Optional, Union
from uuid import UUID

from confluent_kafka import Consumer
from confluent_kafka.error import (KeyDeserializationError,
                                   ValueDeserializationError)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel

from shared import Settings, get_latest_schemas

stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)


class Key(BaseModel):
    record_id: UUID

    @staticmethod
    def from_dict(input: Dict, _: SerializationContext) -> "Key":
        logger.info(f"Parsing key: {input}")
        return Key.parse_obj(input)


class Driver(BaseModel):
    id: UUID
    phone_number: Optional[str]
    name: Optional[str]


class Trailer(BaseModel):
    id: UUID
    license_plate: str
    body_type: Optional[str]


class Truck(BaseModel):
    id: UUID
    license_plate: str
    type: str
    body_type: Optional[str]


class Value(BaseModel):
    # data: Union[Driver, Trailer, Truck] <-- will not work
    # because pydantic checks types one by one and picks the first matching
    # as Driver has id, and the rest fields are optional, it will be picked up every time
    data: Union[Truck, Trailer, Driver]

    @staticmethod
    def from_dict(input: Dict, _: SerializationContext) -> "Value":
        logger.info(f"Parsing value: {input}")
        return Value.parse_obj(input)


if __name__ == "__main__":
    settings = Settings()
    print(settings)
    key_schema, value_schema = get_latest_schemas(settings)
    print(key_schema)
    print(value_schema)
    # Build consumer
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_url,
            "group.id": "group-1",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.kafka_topic])

    # Build deserializers
    value_deserializer = AvroDeserializer(
        schema_registry_client=settings.get_schema_registry_client(),
        schema_str=value_schema,
        from_dict=Value.from_dict,
    )

    key_deserializer = AvroDeserializer(
        schema_registry_client=settings.get_schema_registry_client(),
        schema_str=key_schema,
        from_dict=Key.from_dict,
    )

    # Build context
    key_ctx = SerializationContext(settings.kafka_topic, MessageField.KEY)
    value_ctx = SerializationContext(settings.kafka_topic, MessageField.VALUE)

    try:
        while True:
            logger.info("Poll")
            message = consumer.poll(timeout=1.0)
            if message is None:
                logger.info("Received no message")
            elif message.error() is not None:
                logger.error(
                    f"Message with an error is received from the {message.topic()} topic: {message.error()}",
                    extra={"error": message.error()},
                )
            else:
                logger.info(f"Received message: {message}")
                message_key: BaseModel = key_deserializer(
                    message.key(), key_ctx
                )  # noqa: typing
                message_value: BaseModel = value_deserializer(
                    message.value(), value_ctx
                )  # noqa: typing
                logger.info(f"key: {message_key}, value: {message_value}")
    except Exception as e:
        logger.exception(e)
