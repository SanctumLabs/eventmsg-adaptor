"""Pydantic Serializer"""

from typing import Any, Type, TypeVar
import orjson

from gambol.schema import (
    PydanticEventBody,
    SerializationContext,
    SerializationFormat,
    SerializedMessage,    
)

from .serializer import Serializer, SerializationError


PydanticModel_T = TypeVar("PydanticModel_T", bound=PydanticEventBody)


class PydanticSerializer(Serializer):
    """Pydantic Serializer handles serialization & deserialization of pydantic type of messages"""
    
    def serialize(self, value: Any, serialization_context: SerializationContext) -> SerializedMessage:
        try:
            message = orjson.dumps(value.dict())
            return SerializedMessage(
                serialization_format=SerializationFormat.JSON, message=message
            )
        except Exception as e:
            raise SerializationError from e
    
    def deserialize(self, value: bytes, model: Type[PydanticModel_T]) -> PydanticModel_T:
        try:
            dict_attrs = orjson.loads(value)
            return model.model_validate(dict_attrs)
        except Exception as e:
            raise SerializationError from e
