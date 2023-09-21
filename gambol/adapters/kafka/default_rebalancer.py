from __future__ import annotations
import logging
import asyncio
from typing import Optional, Callable, List, Awaitable

from aiokafka import ConsumerRebalanceListener, TopicPartition

logger = logging.getLogger(__name__)


PartitionsRevokedCallback = Callable[
    [List[TopicPartition], "DefaultRebalancer"], Awaitable[None]
]
PartitionsAssignedCallback = Callable[
    [List[TopicPartition], "DefaultRebalancer"], Awaitable[None]
]


class DefaultRebalancer(ConsumerRebalanceListener):
    def __init__(
        self,
        on_partitions_revoked_callback: Optional[PartitionsRevokedCallback],
        on_partitions_assigned_callback: Optional[PartitionsAssignedCallback],
    ) -> None:
        self._on_partitions_revoked_callback: Optional[
            PartitionsRevokedCallback
        ] = on_partitions_revoked_callback
        self._on_partitions_assigned_callback: Optional[
            PartitionsAssignedCallback
        ] = on_partitions_assigned_callback
        self._event: asyncio.Event = asyncio.Event()

    async def on_partitions_revoked(self, revoked: List[TopicPartition]) -> None:
        """Called when the consumer starts up. It is invoked with an empty set. There is no need for it to handle anything, so, it's gracefully ignored here.

        Args:
            revoked (List[TopicPartition]): revoked topic partitions
        """
        if revoked:
            logger.debug(
                f"ConsumerRebalanceListener received callback to revoke partitions: {revoked}"
            )

            self._event.clear()

            if self._on_partitions_revoked_callback:
                logger.debug("Triggering user's on_partitions_revoked_callback")

                await self._on_partitions_revoked_callback(revoked, self)

            logger.debug(
                "Waiting for signal from user before continuing with re-balance handling"
            )

            await self._event.wait()

            logger.debug(
                "Received signal that consumer is done handling the re-balance - continuing"
            )

            self._event.clear()

    async def on_partitions_assigned(self, assigned):
        logger.debug(
            f"ConsumerRebalanceListener received callback to assign partitions: {assigned}"
        )

        if self._on_partitions_assigned_callback:
            logger.debug("Triggering user's on_partitions_assigned_callback")

            await self._on_partitions_assigned_callback(assigned, self)

    def continue_with_partition_assignment(self) -> None:
        self._event.set()
