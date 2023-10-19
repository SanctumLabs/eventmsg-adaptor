from abc import ABC
from typing import Optional, Type
from typing_extensions import TypedDict
import logging

from eventmsg_adaptor.serializers import SerializationError, Serializer
from eventmsg_adaptor.schema import (
    Event,
    EventBody,
    InboundMessage,
)
from eventmsg_adaptor.adapters.sqs.utils import _parse_event_headers
from eventmsg_adaptor.adapters.sqs.schema import MessageTypeDef


logger = logging.getLogger(__name__)


class BaseSQSAdapter(ABC):
    def __init__(
        self,
        serializer: Serializer,
        topic_prefix: Optional[str] = None,
        subscription_prefix: Optional[str] = None,
        subscriber_wait_time_in_seconds: int = 2,
        max_number_of_messages_to_return: int = 5,
    ):
        self._serializer = serializer
        self._topic_prefix: Optional[str] = topic_prefix
        self._subscription_prefix: Optional[str] = subscription_prefix
        self._subscriber_wait_time_in_seconds: int = subscriber_wait_time_in_seconds
        self._max_number_of_messages_to_return: int = max_number_of_messages_to_return

    @property
    def topic_prefix(self) -> Optional[str]:
        return self._topic_prefix

    @property
    def subscription_prefix(self) -> Optional[str]:
        return self._subscription_prefix

    @property
    def subscriber_wait_time_in_seconds(self) -> Optional[int]:
        return self._subscriber_wait_time_in_seconds

    @property
    def max_number_of_messages_to_return(self) -> Optional[int]:
        return self._max_number_of_messages_to_return

    def _parse_message(
        self,
        message: MessageTypeDef,
        sqs_queue_url: str,
        event_body_type: Type[EventBody],
    ) -> Optional[InboundMessage]:
        try:
            message_attributes = message.get("MessageAttributes") or {}
            headers = _parse_event_headers(message_attributes)

            body_as_bytes = message["Body"].encode("utf-8")
            body = self._serializer.deserialize(body_as_bytes, model=event_body_type)

            return InboundMessage[MessageTypeDef, SQSInboundMessageAttributes](
                event=Event(headers=headers, body=body),
                raw_message=message,
                attributes=SQSInboundMessageAttributes(queue_url=sqs_queue_url),
            )
        except SerializationError as e:
            logger.debug(
                f"Unable to deserialize raw message to event headers and body: {message}"
            )
            logger.exception(e)

            return None


class SQSInboundMessageAttributes(TypedDict):
    queue_url: str
