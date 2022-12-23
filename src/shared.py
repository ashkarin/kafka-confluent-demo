import json
import os
from typing import Tuple

from confluent_kafka import avro
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.serialization import MessageField
from pydantic import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_url: str
    kafka_topic: str
    schema_registry_url: str
    value_schema_filepath: str
    key_schema_filepath: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def get_schema_registry_client(self) -> SchemaRegistryClient:
        return SchemaRegistryClient({"url": self.schema_registry_url})

    @property
    def key_schema_name(self) -> str:
        return f"{self.kafka_topic}-{MessageField.KEY}"

    @property
    def value_schema_name(self) -> str:
        return f"{self.kafka_topic}-{MessageField.VALUE}"


def delete_schemas(settings: Settings) -> None:
    schema_registry = settings.get_schema_registry_client()
    schema_registry.delete_subject(settings.key_schema_name)
    schema_registry.delete_subject(settings.value_schema_name)


def register_schemas(settings: Settings) -> Tuple[str, str]:
    schema_registry = settings.get_schema_registry_client()
    avro_schema_key = avro.load(settings.key_schema_filepath)
    avro_schema_value = avro.load(settings.value_schema_filepath)
    schema_key = Schema(
        schema_str=json.dumps(avro_schema_key.to_json()), schema_type="AVRO"
    )

    schema_value = Schema(
        schema_str=json.dumps(avro_schema_value.to_json()), schema_type="AVRO"
    )
    key_id = schema_registry.register_schema(settings.key_schema_name, schema_key)
    value_id = schema_registry.register_schema(settings.value_schema_name, schema_value)
    print(key_id, value_id)
    return schema_key.schema_str, schema_value.schema_str


def get_latest_schemas(settings: Settings) -> Tuple[str, str]:
    schema_registry = settings.get_schema_registry_client()
    registered_schema_value = schema_registry.get_latest_version(
        settings.value_schema_name
    )
    registered_schema_key = schema_registry.get_latest_version(settings.key_schema_name)

    if not registered_schema_key or not registered_schema_value:
        return register_schema(settings)

    schema_value = registered_schema_value.schema
    schema_key = registered_schema_key.schema
    return schema_key.schema_str, schema_value.schema_str
