from typing import Union

from confluent_kafka.schema_registry import SchemaRegistryClient
from mypy_boto3_sns import SNSServiceResource
from mypy_boto3_sqs import SQSServiceResource

from eventmsg_adaptor.config import Config
from eventmsg_adaptor.config.kafka import KafkaConfig
from eventmsg_adaptor.config.sqs import SQSConfig
from eventmsg_adaptor.serializers import Serializer
from eventmsg_adaptor.serializers.confluent_protobuf_serializer import (
    ConfluentProtobufSerializer,
)
from eventmsg_adaptor.serializers.protobuf_serializer import ProtobufSerializer
from eventmsg_adaptor.serializers.pydantic_serializer import PydanticSerializer

from eventmsg_adaptor.adapters.base import BaseAdapter, BaseAsyncAdapter
from eventmsg_adaptor.adapters.kafka.aiokafka import AIOKafkaAdapter
from eventmsg_adaptor.adapters.sqs.sqs import SQSAdapter


def factory(adapter_name: str, config: Config) -> Union[BaseAdapter, BaseAsyncAdapter]:
    """Factory function that creates an adapter based on the provided adapter name and configuration

    Args:
        adapter_name (str): name of adapter.
        config (Config): Configuration to setup adapter.

    Raises:
        Exception: If the adapter provided does not have an accompanying configuration or the adapter is not supported

    Returns:
        Union[BaseAdapter, BaseAsyncAdapter]: Adapter
    """
    adapter_configs = config.adapters

    if not adapter_configs:
        raise Exception("You must configure at least one adapter in the config.")

    if adapter_name == "sqs":
        if not adapter_configs.sqs:
            raise Exception("You must specify an SQSConfig when using the sqs adapter.")

        return _make_sqs_adapter(sqs_config=adapter_configs.sqs, base_config=config)

    elif adapter_name == "aiosqs":
        if not adapter_configs.sqs:
            raise Exception(
                "You must specify an SQSConfig when using the aiosqs adapter."
            )

        # TODO: remove comment to ignore any return once aiosqs adaptor is configured
        return _make_aiosqs_adapter(sqs_config=adapter_configs.sqs, base_config=config)  # type: ignore[no-any-return]

    elif adapter_name == "aiokafka":
        if not adapter_configs.kafka:
            raise Exception(
                "You must specify a KafkaConfig when using the aiokafka adapter."
            )

        return _make_aiokafka_adapter(
            kafka_config=adapter_configs.kafka, base_config=config
        )
    else:
        raise Exception(f"The adapter {adapter_name} is not supported.")


def _make_sqs_adapter(sqs_config: SQSConfig, base_config: Config) -> SQSAdapter:
    try:
        import boto3

        sqs: SQSServiceResource = boto3.resource(
            "sqs",
            region_name=sqs_config.region_name,
            aws_secret_access_key=sqs_config.secret_key,
            aws_access_key_id=sqs_config.access_key_id,
        )

        sns: SNSServiceResource = boto3.resource(
            "sns",
            region_name=sqs_config.region_name,
            aws_secret_access_key=sqs_config.secret_key,
            aws_access_key_id=sqs_config.access_key_id,
        )

        if sqs_config.environment:
            subscription_prefix = f"{sqs_config.environment}-{base_config.service_name}"
        else:
            subscription_prefix = base_config.service_name

        return SQSAdapter(
            sqs=sqs,
            sns=sns,
            serializer=PydanticSerializer(),
            topic_prefix=sqs_config.environment,
            subscription_prefix=subscription_prefix,
        )
    except ImportError:
        raise Exception(
            "The boto3 package is not instealled. Please run 'poetry add boto3'."
        )


# TODO: configure aiosqs adaptor
def _make_aiosqs_adapter(sqs_config: SQSConfig, base_config: Config):  # type: ignore[no-untyped-def]
    pass


def _make_aiokafka_adapter(
    kafka_config: KafkaConfig, base_config: Config
) -> AIOKafkaAdapter:
    try:
        import aiokafka  # noqa: F401

        serializer: Serializer

        if kafka_config.schema_registry:
            if kafka_config.schema_registry.schema_registry_url:
                schema_registry_client = SchemaRegistryClient(
                    {
                        "url": kafka_config.schema_registry.schema_registry_url,
                        "basic.auth.user.info": kafka_config.schema_registry.schema_registry_user_info,
                    }
                )

                serializer = ConfluentProtobufSerializer(
                    schema_registry_client=schema_registry_client
                )
        else:
            serializer = ProtobufSerializer()

        return AIOKafkaAdapter(
            bootstrap_servers=kafka_config.bootstrap_servers,
            group_id=base_config.service_name,
            sasl_username=kafka_config.security.sasl_username
            if kafka_config.security
            else None,
            sasl_password=kafka_config.security.sasl_password
            if kafka_config.security
            else None,
            serializer=serializer,
        )

    except ImportError:
        raise Exception(
            "The aiokafka adapter package is not installed. Please run 'poetry add aiokafka'."
        )


__all__ = [
    "AIOKafkaAdapter",
    "AIOSQSAdapter",
    "BaseAdapter",
    "BaseAsyncAdapter",
    "SQSAdapter",
    "factory",
]
