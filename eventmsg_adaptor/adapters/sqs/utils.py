import hashlib
import json
import re
from typing import Any, Dict, Optional

from eventmsg_adaptor.schema import (
    Destination,
    EventHeaders,
    ListenExpression,
)
from eventmsg_adaptor.utils import normalise_destination
from eventmsg_adaptor.adapters.sqs.schema import MessageAttributeValueTypeDef


def _make_message_attribute(
    value: Any,
) -> MessageAttributeValueTypeDef:
    return {"DataType": "String", "StringValue": str(value)}


def _make_message_attributes_from_event_headers(
    headers: EventHeaders,
) -> Any:
    message_attributes = {
        "id": str(headers.id),
        "event_name": headers.event_name,
        # 'event_type' is a legacy field and has since been renamed to `event_name`
        # We need to keep this as a message attribute for existing subscription filters to continue working.
        "event_type": headers.event_name,
        "destination": str(headers.destination),
        "destination.topic": headers.destination.topic,
        "destination.sub_topic": headers.destination.sub_topic,
        "version": headers.version,
        "timestamp": headers.timestamp.isoformat(),
        "source": headers.source,
        "correlation_id": headers.correlation_id,
        "group_id": headers.group_id,
        "subject": headers.subject,
        "signature": headers.signature,
    }

    return {k: _make_message_attribute(v) for k, v in message_attributes.items() if v}


def _sanitise_sqs_queue_or_topic_name(value: str) -> str:
    value = value.replace(".", "-")
    value = re.sub("[^0-9a-z_-]+", "", value, re.IGNORECASE)
    return value


def _make_subscription_queue_policy(queue_arn: str, topic_arn: str) -> Dict[str, Any]:
    # By default, AWS does not grant permission for SNS to write to an SQS queue.
    # We therefore have to adjust the policy.
    # See https://stackoverflow.com/questions/48738509/aws-empty-sqs-queue-when-subscribed-to-sns-via-boto3

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": "sqs:SendMessage",
                "Principal": {"AWS": "*"},
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": topic_arn,
                    }
                },
            }
        ],
    }


def _make_subscription_filter_policy(
    listen_expression: ListenExpression,
) -> Dict[str, Any]:
    filters = {}

    if listen_expression.event_name:
        filters["event_name"] = [listen_expression.event_name]

    destination = listen_expression.destination

    filters["destination.topic"] = [destination.topic]

    if destination.sub_topic:
        filters["destination.sub_topic"] = [destination.sub_topic]

    return filters


def _make_subscription_attributes(
    listen_expression: ListenExpression,
) -> Dict[str, Any]:
    # When using MessageAttributes, we must enable RawMessageDelivery for the attributes to be forwarded
    # to the SQS queue, and for the subscriber to read the attributes when a message is received.

    return {
        "RawMessageDelivery": "true",
        "FilterPolicy": json.dumps(_make_subscription_filter_policy(listen_expression)),
    }


def _make_subscription_queue_name(
    listen_expression: ListenExpression, prefix: Optional[str] = None
) -> str:
    parts = []

    if prefix:
        # hello
        # camel-hello
        parts.append(prefix)

    # merchant_profile
    # merchant_profile.business
    destination = listen_expression.destination

    parts.append(destination.path)

    if listen_expression.event_name:
        # business_registered
        parts.append(listen_expression.event_name)

    # We land up with the following parts:
    # ["camel-hello", "profile.business", "business_registered"]

    queue_name = "-".join(parts)
    # camel-hello-merchant_profile.business-business_registered

    # SQS only allows alphanumeric characters, hyphens (-), and underscores (_), and the max queue name length
    # is 80 chars. Since our destination can contain sub_topics, delimited by ., we need to replace .'s with -'s.

    queue_name = _sanitise_sqs_queue_or_topic_name(queue_name)
    # camel-hello-merchant_profile-business-business_registered

    # Examples:
    # hello-merchant_profile-business-business_registered (56 chars)
    # hello-merchant_profile-business
    # hello-merchant_profile
    # hello-merchant_account
    # hello-merchant_account-payment
    # hello-merchant_account-payment-payment_accepted
    # kyc-merchant_account-payment-payment_accepted
    # logistics-merchant_account-payment-payment_accepted
    # reader-service-merchant_account-payment-payment_accepted (56 chars)
    # camel-hello-merchant_profile-business-business_registered (62 chars)
    # matthewgoslett-hello-merchant_profile-business-business_registered (71 chars)

    # For a fifo queue, the queue name needs to be suffixed with .fifo
    max_length = 75 if destination.is_fifo else 80

    if len(queue_name) > max_length:
        # The queue name exceeds the maximum SQS queue length
        # We'll keep the first max_length - 33 chars, and then append a dash followed by an md5 hash (32 chars).
        chars_to_keep = max_length - 33

        # matthewgoslett-hello-merchant_profile-business-business_registered_and_this_is_too_long
        first_part_of_queue = queue_name[0:chars_to_keep]

        hash_of_queue_name = hashlib.md5(
            queue_name.encode("utf-8")
        ).hexdigest()  # 5e9abba6fe1157e00f25245bf5c52cda

        # matthewgoslett-hello-merchant_profile-busi-5e9abba6fe1157e00f25245bf5c52cda
        queue_name = f"{first_part_of_queue}-{hash_of_queue_name}"

    if destination.is_fifo:
        queue_name = f"{queue_name}.fifo"

    return queue_name


def _make_topic_name(destination: Destination, prefix: Optional[str] = None) -> str:
    topic = destination.topic
    topic_prefix = prefix or ""

    topic_name = topic if not topic_prefix else f"{topic_prefix}-{topic}"

    topic_name = _sanitise_sqs_queue_or_topic_name(topic_name)

    if destination.is_fifo:
        topic_name = f"{topic_name}.fifo"

    return topic_name


def _parse_event_headers(
    message_attributes: Dict[str, MessageAttributeValueTypeDef]
) -> EventHeaders:
    headers = {}

    for name, attribute in message_attributes.items():
        if attribute["DataType"] == "String" or attribute["DataType"] == "Number":
            value: Any = attribute["StringValue"]

            if name == "destination":
                if value.endswith("."):
                    value = value[:-1]

                value = normalise_destination(value)
            elif name == "event_type":
                name = "event_name"

            if value == "":
                value = None

            headers[name] = value

    return EventHeaders.parse_obj(headers)
