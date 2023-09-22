import inspect
from typing import Dict, Tuple, Type, Union, cast
from unittest.mock import AsyncMock

from google.protobuf.message import Message

from msgsync.schema import (
    AsyncEventCallback,
    Destination,
    Event,
    EventBody,
    EventCallback,
    ListenExpression,
    PydanticEventBody,
)


def parse_listen_expression_str(listen_expression_str: str) -> ListenExpression:
    """Parses a listen expression string and creates a ListenExpression. The expression should have the format <destination>/<event_name>/<version>
    Note the / which are used to build the ListenExpression after splitting along this delimeter. If not all parts can be gathered from the split
    The parts will be returned as None on the ListenExpression object

    Args:
        listen_expression_str (str): Listen expression string

    Returns:
        ListenExpression: Constructed listen expression.
    """
    slash_parts = listen_expression_str.split("/")

    if len(slash_parts) == 1:
        # The listen_expression_str does not contain a specific event type/version:
        # merchant_profile
        # merchant_profile.*
        # merchant_profile.business.*
        event_name = None
        version = None
        destination_str = listen_expression_str
    elif len(slash_parts) == 2:
        # The listen_expression_str contains an event type:
        # merchant_profile.business/business_registered
        event_name = slash_parts[1]
        version = None
        destination_str = slash_parts[0]
    else:
        # The listen_expression_str contains an event type and version:
        # merchant_profile.business/business_registered/1.0
        event_name = slash_parts[1]
        version = slash_parts[2]
        destination_str = slash_parts[0]

    return ListenExpression(
        destination=parse_destination_str(destination_str),
        event_name=event_name,
        version=version,
    )


def normalise_destination(destination: Union[str, Destination, Dict[str, str]]) -> Destination:
    """Normalizes into a destination object. The passed in argument could be a string, Destination or a key-value pairing(dictionary) that could
    be mapped to a Destination. This validates and constructs a Destination object

    Args:
        destination (Union[str, Destination, Dict[str, str]]): destination object to normalize

    Returns:
        Destination: Normalized destination
    """
    if isinstance(destination, Destination):
        return destination
    elif type(destination) == str:
        return parse_destination_str(destination)
    else:
        return Destination.model_validate(destination)


def parse_destination_str(destination_str: str) -> Destination:
    """Parses a destination string and returns a Destination.

    A destination_str input may look something like:

    merchant_profile
    merchant_profile.*
    merchant_profile.business
    merchant_profile::fifo
    merchant_profile.*::fifo
    merchant_profile.business::fifo

    Args:
        destination_str (str): Destination string

    Returns:
        Destination: Destination object
    """

    is_fifo = destination_str.endswith("::fifo")

    if is_fifo:
        destination_str = destination_str[0:-6]

    position_of_dot = destination_str.find(".")

    if position_of_dot >= 0:
        topic = destination_str[0:position_of_dot]
        subtopic = destination_str[position_of_dot + 1 :]

        if subtopic == "*" or subtopic == "":
            parsed_subtopic = None
        elif subtopic.endswith(".*"):
            parsed_subtopic = subtopic[0:-2]
        else:
            parsed_subtopic = subtopic
    else:
        topic = destination_str
        parsed_subtopic = None

    return Destination(topic=topic, subtopic=parsed_subtopic, is_fifo=is_fifo)


def parse_event_name_and_version_from_protobuf_message(message: Message) -> Tuple[str, str]:
    """Parse a protobuf message extracting the event name and version of the message

    Args:
        message (Message): Protobuf message

    Raises:
        Exception: When the payload type is missing from the message
        Exception: When the payload extracted from the message is not an instance of a Protobuf class
        Exception: When the full message type does not contain a valid event type and version

    Returns:
        Tuple[str, str]: tuple of event name and version in the form (event_name, version)
    """
    # We can extract the event type and version from the protobuf message

    payload_type = message.WhichOneof("payload")
    # payload_type = vetting_started

    if not payload_type:
        raise Exception("The protobuf message does not have a `payload` set.")

    payload = getattr(message, payload_type)
    # payload = <class 'sanctumlabs.events.profile.kyc.vetting.v1.events_pb2.VettingStarted'>

    if not isinstance(payload, Message):
        raise Exception(f"The protobuf message attribute {payload_type} is not set.")

    full_message_type = payload.DESCRIPTOR.full_name
    # full_message_type = sanctumlabs.events.profile.kyc.vetting.v1.VettingStarted
    message_type_parts = full_message_type.split(".")
    # message_type_parts = ['sanctumlabs', 'events', 'profile', 'kyc', 'vetting', 'v1', 'VettingStarted']

    if len(message_type_parts) < 2:
        raise Exception(
            f"The message type {full_message_type} does not appear to contain a valid event type and version."
        )

    event_name = message_type_parts[-1]
    version = normalise_version(message_type_parts[-2])

    # event_name = VettingStarted
    # version = V1

    return event_name, version


def normalise_version(version: str) -> str:
    """Normalizes a version string and returns a semantic version in the form of Major.Minor.Patch without any prefixes such as `v`.

    Args:
        version (str): version string to normalize

    Returns:
        str: normalized version string
    """
    version = version.lower()

    if version.startswith("v"):
        version = version[1:]

    if version.isdigit():
        version += ".0"

    return version


def parse_event_body_type_from_subscribe_callback(callback: Union[EventCallback, AsyncEventCallback]) -> Type[EventBody]:
    # This is a hack for compat with tests. If you inspect an `AsyncMock`, it raises a `unsupported callable` error.
    if isinstance(callback, AsyncMock):
        return PydanticEventBody

    arg_spec = inspect.getfullargspec(callback)
    event_cls = arg_spec.annotations.get("event")

    if event_cls:
        if not issubclass(event_cls, Event):
            raise Exception(
                "The subscribe callback's `event` arg is not of type Event."
            )

        annotations = event_cls.__annotations__
        body_type = annotations["body"]

        if body_type == "EventBody_T":
            # This is a safeguard and allows for backwards compat where all events contained a body
            # like a PydanticEventBody
            return PydanticEventBody
        elif issubclass(body_type, PydanticEventBody):
            return cast(Type[PydanticEventBody], body_type)
        elif issubclass(body_type, Message):
            return cast(Type[Message], body_type)
        else:
            raise Exception(f"The type {body_type} is not supported.")
    else:
        # This allows for backwards compat where we didn't need typed annotations in the callback
        return PydanticEventBody
