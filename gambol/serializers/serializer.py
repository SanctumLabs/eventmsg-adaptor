from typing import Any, Protocol, TypeVar, cast

from google.protobuf.message import Message

from gambol.schema import (
    PydanticEventBody,
    SerializationContext,
    SerializedMessage,    
)

class SerializationError(Exception):
    """Serialization error thrown when there is a failure to serialize a message"""
    pass


ProtobufMessage_T = TypeVar("ProtobufMessage_T", bound=Message)

class Serializer(Protocol):
    """Defines a serializer which handles the serialization and deserialization of a message. Note that it typically has 2 methods, Serialize
    and deserialize. This class defines a protocol that serializers follow and can be used as a type annotation when defining a serializer. Especially
    the case for serializer classes that do not define their types.
    """
    def serialize(self, value: Any, serialization_context: SerializationContext) -> SerializedMessage:
        """Defines serialization of a value/message given serialization context and returns a serialized message.

        Args:
            value (Any): Message or value to serialize
            serialization_context (SerializationContext): context used to serialize message

        Returns:
            SerializedMessage: Message to serialize
        """
        pass

    def deserialize(self, value: bytes, model: Any) -> Any:
        """Deserialize bytes, using the provided model and return the deserialized message

        Args:
            value (bytes): bytes to deserialize or a message in the form of bytes
            model (Any): Model to deserialize to

        Returns:
            Any: deserialized message
        """
        pass
