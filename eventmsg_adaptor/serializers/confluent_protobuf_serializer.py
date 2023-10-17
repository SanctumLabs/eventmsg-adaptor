from typing import Optional, TypeVar, Type, cast

from google.protobuf.message import Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer as BaseConfluentProtobufDeserializer,
)
from confluent_kafka.schema_registry.protobuf import (
    ProtobufSerializer as BaseConfluentProtobufSerializer,
)
from confluent_kafka.serialization import MessageField
from confluent_kafka.serialization import (
    SerializationContext as ConfluentSerializationContext,
)

from eventmsg_adaptor.schema import (
    SerializationContext,
    SerializedMessage,
    SerializationFormat,
)

from . import SerializationError, Serializer

ProtobufMessage_T = TypeVar("ProtobufMessage_T", bound=Message)


class ConfluentProtobufSerializer(Serializer):
    """Confluent specific protobuf serializer"""

    def __init__(self, schema_registry_client: Optional[SchemaRegistryClient] = None):
        self.schema_registry_client = schema_registry_client

    def serialize(
        self, value: Message, serialization_context: SerializationContext
    ) -> SerializedMessage:
        if not self.schema_registry_client:
            raise SerializationError(
                "a schema registry client is required when serializing a message"
            )

        confluent_serialization_context = ConfluentSerializationContext(
            topic=serialization_context.destination.topic
            if serialization_context.destination
            else None,
            field=MessageField.VALUE,
        )

        try:
            serializer = BaseConfluentProtobufSerializer(
                msg_type=value.__class__,
                schema_registry_client=self.schema_registry_client,
                conf={
                    "use.deprecated.format": False,
                    "auto.register.schemas": False,
                    "skip.known.types": True,
                    "use.latest.version": True,
                },
            )

            message = serializer(message=value, ctx=confluent_serialization_context)

            return SerializedMessage(
                serialization_format=SerializationFormat.PROTOBUF, message=message
            )
        except Exception as e:
            raise SerializationError from e

    def deserialize(
        self, value: bytes, model: Type[ProtobufMessage_T]
    ) -> ProtobufMessage_T:
        try:
            deserialzer = BaseConfluentProtobufDeserializer(
                message_type=model, conf={"use.deprecated.format": False}
            )

            message = deserialzer(data=value, ctx=None)
            return cast(ProtobufMessage_T, message)
        except Exception as e:
            raise SerializationError from e
