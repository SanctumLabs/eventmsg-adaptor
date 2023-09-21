from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any, List, Optional

from aiokafka import AIOKafkaConsumer, TopicPartition, AIOKafkaProducer

from gambol.adapters.base import BaseAsyncAdapter
from gambol.serializers.serializer import Serializer
from gambol.strenum import StrEnum
from gambol.schema import Destination, Event, SerializationContext

from .default_rebalancer import DefaultRebalancer
from .utils import key_serializer, make_kafka_headers_from_event_headers


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
