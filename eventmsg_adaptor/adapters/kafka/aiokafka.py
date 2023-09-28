from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any, List, Optional, Dict, Type

from aiokafka import AIOKafkaConsumer, TopicPartition, AIOKafkaProducer, ConsumerRecord

from eventmsg_adaptor.adapters.base import BaseAsyncAdapter
from eventmsg_adaptor.serializers import Serializer, SerializationError
from eventmsg_adaptor.strenum import StrEnum
from eventmsg_adaptor.schema import (
    AsyncListener,
    Destination,
    Event,
    SerializationContext,
    EventBody,
    InboundMessage,
)

from .default_rebalancer import DefaultRebalancer
from .utils import (
    key_serializer,
    make_kafka_headers_from_event_headers,
    parse_event_headers,
)


logger = logging.getLogger(__name__)


class ConsumerState(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    REBALANCING = "rebalancing"


class AIOKafkaAdapter(BaseAsyncAdapter):
    def __init__(
        self,
        bootstrap_servers: List[str],
        group_id: str,
        serializer: Serializer,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        producer_request_timeout_in_ms: int = 10000,
    ) -> None:
        self._bootstrap_servers: List[str] = bootstrap_servers
        self._group_id: str = group_id
        self._serializer: Serializer = serializer
        self._sasl_username: Optional[str] = sasl_username
        self._sasl_password: Optional[str] = sasl_password
        self._producer_request_timeout_in_ms: int = producer_request_timeout_in_ms
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._consumer_state: ConsumerState = ConsumerState.STOPPED
        self._rebalance_listener: DefaultRebalancer = self._make_rebalance_listener()

    @property
    def bootstrap_servers(self) -> List[str]:
        return self._bootstrap_servers

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def sasl_username(self) -> Optional[str]:
        return self._sasl_username

    @property
    def sasl_password(self) -> Optional[str]:
        return self.sasl_password

    @property
    def consumer(self) -> AIOKafkaConsumer:
        if not self._consumer:
            self._consumer = AIOKafkaConsumer(
                group_id=self._group_id,
                bootstrap_servers=self._bootstrap_servers,
                sasl_plain_username=self._sasl_password,
                sasl_plain_password=self._sasl_password,
                security_protocol="SASL_SSL"
                if self.sasl_username and self.sasl_password
                else "PLAINTEXT",
                ssl_context=ssl.create_default_context(),
                enable_auto_commit=False,
            )
        return self._consumer

    @property
    def consumer_state(self) -> ConsumerState:
        return self._consumer_state

    @property
    def rebalance_listener(self) -> DefaultRebalancer:
        return self._rebalance_listener

    async def publish(self, destination: Destination, event: Event) -> Any:
        def value_serializer(value: Any) -> bytes:
            serialized_message = self._serializer.serialize(
                value,
                serialization_context=SerializationContext(destination=destination),
            )

            return serialized_message.message

        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            sasl_plain_username=self._sasl_username,
            sasl_plain_password=self._sasl_password,
            security_protocol="SASL_SSL"
            if self._sasl_username and self._sasl_password
            else "PLAINTEXT",
            ssl_context=ssl.create_default_context(),
            enable_idempotence=True,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            request_timeout_ms=self._producer_request_timeout_in_ms,
        )

        await producer.start()

        try:
            return await producer.send_and_wait(
                topic=destination.topic,
                value=event.body,
                key=event.headers.group_id,
                headers=make_kafka_headers_from_event_headers(event.headers),
            )
        finally:
            await producer.stop()

    @property
    def _is_consumer_running(self) -> bool:
        return self._consumer_state in [
            ConsumerState.RUNNING,
            ConsumerState.REBALANCING,
        ]

    async def listen(self, listeners: List[AsyncListener]) -> None:
        if self._consumer_state == ConsumerState.STARTING:
            raise Exception("The consumer is already in the process of starting up")
        elif self._is_consumer_running:
            raise Exception("The consumer is already running")

        try:
            self._consumer_state = ConsumerState.STARTING

            # We're going to create one consumer which we'll bind to all topics
            # When a message comes in, we need to be able to deserialize the message to the relevant type of event
            # We'll therefore create a dictionary mapping topics to listeners
            topic_mappings: Dict[str, AsyncListener] = {
                listener.listen_expression.destination.topic: listener
                for listener in listeners
            }

            topics = list(topic_mappings.keys())

            logger.debug("Consumer has started successfully")

            self._consumer_state = ConsumerState.RUNNING

            try:
                while self._is_consumer_running:
                    data = await self.consumer.getmany(timeout_ms=1000)

                    for topic_partition, messages in data.items():
                        for message in messages:
                            # We fetch messages in batches. When we're busy processing a message, we may receive
                            # a re-balance where topics partitions are both assigned and unassigned from this consumer.
                            # When this happens, we avoid processing any messages, finishing up what we're doing neatly,
                            # and signal to the broker that we're ready to take on the new partitions.
                            if self._consumer_state == ConsumerState.REBALANCING:
                                logger.debug(
                                    f"Ignoring message on topic {message.topic} because re-balance is in progress: {message}"
                                )
                                continue

                            logger.debug(
                                f"Received message on topic: {message.topic}: {message}"
                            )

                            listener = topic_mappings[message.topic]

                            inbound_message = self._parse_message(
                                message=message,
                                event_body_type=listener.event_body_type,
                            )

                            if inbound_message:
                                await listener.callback(inbound_message)

                    # If we've been tidying up during a re-balance, reaching this point means we're now safe to
                    # pass control back to the re-balance listener and let it continue with the partition assignment.
                    if self._consumer_state == ConsumerState.REBALANCING:
                        self._rebalance_listener.continue_with_partition_assignment()

                    await asyncio.sleep(0.1)
            finally:
                logger.debug("Stopping consumer")

                await self.consumer.stop()

                logger.debug("Consumer has now stopped")
        finally:
            self._consumer_state = ConsumerState.STOPPED

    async def close(self) -> None:
        self._consumer_state = ConsumerState.STOPPING

    async def ack(self, message: InboundMessage[ConsumerRecord, Dict[str, Any]]) -> Any:
        consumer_record = message.raw_message

        tp = TopicPartition(consumer_record.topic, consumer_record.partition)

        await self.consumer.commit({tp: consumer_record.offset + 1})

    async def nack(self, message: InboundMessage) -> Any:
        # There is no concept of nacking a message in kafka. When we're finished handling a message, we commit the
        # current offset. The next time we start consuming messages, we read from our offset. Therefore, we simply
        # don't commit our offset if we want to nack a message and have it retried.
        pass

    def _make_rebalance_listener(self) -> DefaultRebalancer:
        async def on_partitions_revoked_callback(
            topic_partitions: List[TopicPartition], listener: DefaultRebalancer
        ) -> None:
            if self._consumer_state == ConsumerState.RUNNING:
                self._consumer_state = ConsumerState.REBALANCING
            else:
                # when the consumer is starting, it immediately receives a callback causing a block. Therefore, when the consumer
                # isn't yet fully running, we immediately pass control back to the rebalancer
                listener.continue_with_partition_assignment()

        async def on_partitions_assigned_callback(
            topic_partitions: List[TopicPartition], listener: DefaultRebalancer
        ) -> None:
            if self._consumer_state == ConsumerState.REBALANCING:
                self._consumer_state = ConsumerState.RUNNING

        rebalance_listener = DefaultRebalancer(
            on_partitions_revoked_callback=on_partitions_revoked_callback,
            on_partitions_assigned_callback=on_partitions_assigned_callback,
        )

        return rebalance_listener

    def _parse_message(
        self, message: ConsumerRecord, event_body_type: Type[EventBody]
    ) -> Optional[InboundMessage]:
        try:
            body = self._serializer.deserialize(
                value=message.value, model=event_body_type
            )

            headers = parse_event_headers(event_body=body, message=message)

            # This is unfortunately a little hacky. The `ConsumerRecord` class is of type `dataclass` which is validated
            # by pydantic. When we construct our `InboundMessage` and pass in the record, it throws an exception
            # because the `checksum` field cannot be `None`. This is a deprecated field that is by default always `None`.
            # It is expected to be an `int`, so we force it to a `0` value.
            message.checksum = 0

            return InboundMessage[ConsumerRecord, Dict[str, Any]](
                event=Event(headers=headers, body=body),
                raw_message=message,
                attributes={},
            )
        except SerializationError as e:
            logger.debug(
                f"Unable to deserialize raw message to event headers and body: {message}"
            )
            logger.exception(e)
            return None
