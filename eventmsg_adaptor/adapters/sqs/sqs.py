import json
import logging
from typing import Dict, List, Optional, Type

from mypy_boto3_sns import SNSServiceResource
from mypy_boto3_sns.service_resource import Subscription, Topic
from mypy_boto3_sns.type_defs import PublishResponseTypeDef
from mypy_boto3_sqs import SQSServiceResource
from mypy_boto3_sqs.literals import QueueAttributeNameType
from mypy_boto3_sqs.service_resource import Queue
from mypy_boto3_sqs.type_defs import EmptyResponseMetadataTypeDef, MessageTypeDef

from eventmsg_adaptor.adapters.base import BaseAdapter
from eventmsg_adaptor.serializers import Serializer
from eventmsg_adaptor.adapters.sqs import BaseSQSAdapter, SQSInboundMessageAttributes
from eventmsg_adaptor.adapters.sqs.utils import (
    _make_message_attributes_from_event_headers,
    _make_subscription_attributes,
    _make_subscription_queue_name,
    _make_subscription_queue_policy,
    _make_topic_name,
)
from eventmsg_adaptor.schema import (
    Destination,
    Event,
    EventBody,
    InboundMessage,
    ListenerCallback,
    ListenExpression,
    SerializationContext,
)

logger = logging.getLogger(__name__)


class SQSAdapter(BaseAdapter, BaseSQSAdapter):
    def __init__(
        self,
        sqs: SQSServiceResource,
        sns: SNSServiceResource,
        serializer: Serializer,
        topic_prefix: Optional[str] = None,
        subscription_prefix: Optional[str] = None,
        subscriber_wait_time_in_seconds: int = 2,
        max_number_of_messages_to_return: int = 5,
    ):
        self._sqs = sqs
        self._sns = sns
        self._sns_topics: Dict[str, Topic] = {}

        super().__init__(
            serializer=serializer,
            topic_prefix=topic_prefix,
            subscription_prefix=subscription_prefix,
            subscriber_wait_time_in_seconds=subscriber_wait_time_in_seconds,
            max_number_of_messages_to_return=max_number_of_messages_to_return,
        )

    @property
    def sqs(self) -> SQSServiceResource:
        return self._sqs

    @property
    def sns(self) -> SNSServiceResource:
        return self._sns

    def publish(self, destination: Destination, event: Event) -> PublishResponseTypeDef:
        topic = _make_topic_name(destination, prefix=self._topic_prefix)
        sns_topic = self._get_or_create_topic(topic)
        serialized_message = self._serializer.serialize(
            event.body,
            serialization_context=SerializationContext(destination=destination),
        )

        logger.debug(
            f"Publishing message {serialized_message.message_as_str} to topic {topic}"
        )

        message_attributes = _make_message_attributes_from_event_headers(event.headers)

        publish_args = {
            # TODO: This needs to be refactored to handle sending protobuf (binary)
            "Message": serialized_message.message_as_str,
            "MessageAttributes": message_attributes,
        }

        if destination.is_fifo and event.headers.group_id:
            publish_args["MessageGroupId"] = event.headers.group_id

        response = sns_topic.publish(**publish_args)

        logger.debug(f"Message published: {response}")

        return response

    def _get_or_create_topic(self, topic: str) -> Topic:
        logger.debug(f"Checking if topic {topic} already exists in local cache")

        if topic in self._sns_topics:
            sns_topic = self._sns_topics[topic]

            logger.debug(f"Resolved topic {topic} from cache: {sns_topic}")

            return sns_topic

        return self._create_topic(topic)

    def _create_topic(self, topic: str) -> Topic:
        logger.debug(f"Creating topic {topic}")

        attributes = {}

        if topic.endswith(".fifo"):
            attributes["FifoTopic"] = "true"
            attributes["ContentBasedDeduplication"] = "true"

        sns_topic = self._sns.create_topic(Name=topic, Attributes=attributes)

        logger.debug(f"Topic created: {sns_topic}")

        self._sns_topics[topic] = sns_topic

        return sns_topic

    def subscribe(
        self,
        listen_expression: ListenExpression,
        callback: ListenerCallback,
        event_body_type: Type[EventBody],
    ) -> None:
        topic = _make_topic_name(
            listen_expression.destination, prefix=self._topic_prefix
        )

        sns_topic = self._get_or_create_topic(topic)
        sqs_queue = self._create_subscription_queue(
            sns_topic=sns_topic, listen_expression=listen_expression
        )
        _create_subscription(
            sqs_queue=sqs_queue,
            sns_topic=sns_topic,
            listen_expression=listen_expression,
        )

        logger.debug(
            f"Listening for new messages on queue {sqs_queue.attributes['QueueArn']}..."
        )

        while True:
            response = self._sqs.meta.client.receive_message(
                QueueUrl=sqs_queue.url,
                WaitTimeSeconds=self._subscriber_wait_time_in_seconds,
                MaxNumberOfMessages=self._max_number_of_messages_to_return,
                MessageAttributeNames=["All"],
            )
            messages: List[MessageTypeDef] = response.get("Messages") or []

            for message in messages:
                logger.debug(
                    f"Received message on queue {sqs_queue.attributes['QueueArn']}: {message}"
                )

                inbound_message = self._parse_message(
                    message=message,
                    sqs_queue_url=sqs_queue.url,
                    event_body_type=event_body_type,
                )

                if inbound_message:
                    callback(inbound_message)
                else:
                    # We failed to deserialize the message, so we'll just delete it
                    self._delete_message(
                        sqs_queue_url=sqs_queue.url,
                        message_receipt_handle=message["ReceiptHandle"],
                    )

    def _create_subscription_queue(
        self, sns_topic: Topic, listen_expression: ListenExpression
    ) -> Queue:
        queue_name = _make_subscription_queue_name(
            listen_expression, prefix=self._subscription_prefix
        )

        logger.debug(f"Creating queue {queue_name}")

        attributes: Dict[QueueAttributeNameType, str] = {}

        if listen_expression.destination.is_fifo:
            attributes["FifoQueue"] = "true"

        sqs_queue = self._sqs.create_queue(QueueName=queue_name, Attributes=attributes)

        logger.debug(f"Queue created: {sqs_queue}")

        sqs_queue.set_attributes(
            Attributes={
                "Policy": json.dumps(
                    _make_subscription_queue_policy(
                        queue_arn=sqs_queue.attributes["QueueArn"],
                        topic_arn=sns_topic.arn,
                    )
                )
            }
        )

        return sqs_queue

    def ack(
        self,
        inbound_message: InboundMessage[MessageTypeDef, SQSInboundMessageAttributes],
    ) -> EmptyResponseMetadataTypeDef:
        return self._delete_message(
            sqs_queue_url=inbound_message.attributes["queue_url"],
            message_receipt_handle=inbound_message.raw_message["ReceiptHandle"],
        )

    def _delete_message(
        self, sqs_queue_url: str, message_receipt_handle: str
    ) -> EmptyResponseMetadataTypeDef:
        logger.debug(
            f"Deleting message {message_receipt_handle} from queue {sqs_queue_url}"
        )

        return self._sqs.meta.client.delete_message(
            QueueUrl=sqs_queue_url,
            ReceiptHandle=message_receipt_handle,
        )

    def nack(self, inbound_message: InboundMessage) -> None:
        # SQS does not support nacking a message. A message is however automatically released back to the queue if not
        # deleted within the message visibility timeout (which defaults to 30 seconds)
        pass


def _create_subscription(
    sqs_queue: Queue, sns_topic: Topic, listen_expression: ListenExpression
) -> Subscription:
    logger.debug(
        f"Creating subscription from queue {sqs_queue} to topic {sns_topic} for listen expression {listen_expression}"
    )

    sns_subscription = sns_topic.subscribe(
        Protocol="sqs",
        Endpoint=sqs_queue.attributes["QueueArn"],
        Attributes=_make_subscription_attributes(listen_expression),
    )

    logger.debug(f"Subscription created: {sns_subscription}")

    return sns_subscription
