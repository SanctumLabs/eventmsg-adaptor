from typing import TypeVar, Type

from google.protobuf.message import Message

from eventmsg_adaptor.schema import (
    SerializationContext,
    SerializationFormat,
    SerializedMessage,
)

from . import Serializer, SerializationError

ProtobufMessage_T = TypeVar("ProtobufMessage_T", bound=Message)


class ProtobufSerializer(Serializer):
    """Protobuf serializer that handles serialization & deserialization of protobuf messages"""

    def serialize(
        self, value: Message, serialization_context: SerializationContext
    ) -> SerializedMessage:
        try:
            message = value.SerializeToString()

            return SerializedMessage(
                serialization_format=SerializationFormat.PROTOBUF, message=message
            )
        except Exception as e:
            raise SerializationError from e

    def deserialize(
        self, value: bytes, model: Type[ProtobufMessage_T]
    ) -> ProtobufMessage_T:
        try:
            message = model()
            message.ParseFromString(value)
            return message
        except Exception as e:
            raise SerializationError from e
