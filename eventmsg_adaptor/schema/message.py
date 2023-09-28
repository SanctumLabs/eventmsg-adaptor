"""Contains message schema passed along in the wire"""
from typing import TypeVar, Mapping, Generic

from pydantic import BaseModel
from pydantic.generics import GenericModel

from eventmsg_adaptor.strenum import StrEnum
from .event import Event


RawMessage_T = TypeVar("RawMessage_T")
MessageAttributes_T = TypeVar("MessageAttributes_T", bound=Mapping)


class InboundMessage(GenericModel, Generic[RawMessage_T, MessageAttributes_T]):
    """InboundMessage is a generic model representation of an inbound message. This wraps the event along the raw representation of the message
    that is not yet serialized to a specific format. It further contains attributes that could be used by receivers and senders to add more
    context to a message on delivery.

    Args:
        event (Event): object that contains event information of the message
        raw_message (Generic): Raw message representation
        attributes (Mapping): dictionary mapping of key value pair mapping of attributes of an inbound message
    """
    event: Event
    raw_message: RawMessage_T
    attributes: MessageAttributes_T


class SerializationFormat(StrEnum):
    """Different types of serialization formats to use
    """
    JSON = "json"
    PROTOBUF = "protobuf"
    
    
class SerializedMessage(BaseModel):
    """Serialized message along with the serialization format used to serialized the message. This allows the consumers of a message to deserialize this
    message to the correct format. Note that the message itself is encoded as bytes.
    """
    serialization_format: SerializationFormat
    message: bytes

    @property
    def message_as_str(self) -> str:
        """Message decoded as a string in UTF-8 format

        Returns:
            str: UTF-8 decoded message
        """
        return self.message.decode("utf-8")
