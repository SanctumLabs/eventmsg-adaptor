import asyncio
import logging
from asyncio import AbstractEventLoop, Task
from datetime import datetime
from typing import Any, Callable, Coroutine, List, Optional, Union
from unittest.mock import Mock

from eventmsg_adaptor.adapters.base import BaseAsyncAdapter
from eventmsg_adaptor.event_streams.base import BaseEventStream
from eventmsg_adaptor.schema import (
    AsyncEventCallback,
    AsyncExceptionHandler,
    AsyncListener,
    Destination,
    Event,
    EventBody,
    InboundMessage,
    ListenerState,
)
from eventmsg_adaptor.utils import (
    normalise_destination,
    parse_event_body_type_from_subscribe_callback,
    parse_listen_expression_str,
)

logger = logging.getLogger(__name__)


class AsyncEventStream(BaseEventStream):
    def __init__(
        self,
        adapter: BaseAsyncAdapter,
        event_source: str,
        subscriber_exception_handler: Optional[AsyncExceptionHandler] = None,
        event_loop: Optional[
            Union[AbstractEventLoop, Callable[[], AbstractEventLoop]]
        ] = None,
    ):
        self._adapter = adapter
        self._subscriber_exception_handler = subscriber_exception_handler
        self._listener_state = ListenerState.STOPPED
        self._event_loop = event_loop
        self._listeners: List[AsyncListener] = []
        self._async_tasks: List[Task] = []

        super().__init__(event_source)

    @property
    def listener_state(self) -> ListenerState:
        return self._listener_state

    @property
    def event_loop(self) -> AbstractEventLoop:
        if isinstance(self._event_loop, AbstractEventLoop):
            return self._event_loop
        elif callable(self._event_loop) and not isinstance(self._event_loop, Mock):
            return self._event_loop()
        else:
            return asyncio.get_running_loop()

    async def _safely_exec_async_task(self, coroutine: Coroutine) -> Any:
        """Safely executes a given task. First creates the task adds it to the async task list, executes the task and returns the result

        Args:
            coroutine (Coroutine): Task to execute

        Returns:
            Any: Result ot executed task
        """
        task = self.event_loop.create_task(coroutine)
        self._async_tasks.append(task)
        result = await task
        self._async_tasks.remove(task)

        return result

    async def publish(
        self,
        destination: Union[str, Destination],
        event_body: EventBody,
        timestamp: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
        group_id: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Any:
        """Asynchronously publishes an event

        Args:
            destination (Union[str, Destination]): Destination of the event
            event_body (EventBody): Event body
            timestamp (Optional[datetime], optional): Timestamp. Defaults to None.
            correlation_id (Optional[str], optional): Correlation ID. Defaults to None.
            group_id (Optional[str], optional): Group ID. Defaults to None.
            subject (Optional[str], optional): Subject of event. Defaults to None.
        """
        destination = normalise_destination(destination=destination)

        event = self._make_outbound_event(
            destination=destination,
            event_body=event_body,
            timestamp=timestamp,
            correlation_id=correlation_id,
            group_id=group_id,
            subject=subject,
        )

        task = self.publish_raw(destination, event)

        return await self._safely_exec_async_task(task)

    async def publish_raw(
        self, destination: Union[str, Destination], event: Event
    ) -> Any:
        destination = normalise_destination(destination)

        logger.debug(f"Publishing event {event} to {destination}")

        return await self._safely_exec_async_task(
            self._adapter.publish(destination=destination, event=event)
        )

    def subscribe(
        self,
        listen_expression_str: str,
        exception_handler: Optional[AsyncExceptionHandler] = None,
    ) -> Callable[[AsyncEventCallback], AsyncListener]:
        def decorator(func: AsyncEventCallback) -> AsyncListener:
            listen_expression = parse_listen_expression_str(listen_expression_str)
            event_body_type = parse_event_body_type_from_subscribe_callback(func)

            async def listener_callback(inbound_message: InboundMessage) -> None:
                try:
                    if listen_expression.matches(inbound_message.event):
                        logger.debug(f"Received event {inbound_message.event}")

                        await self._safely_exec_async_task(func(inbound_message.event))  # type: ignore[arg-type]

                    await self._safely_exec_async_task(
                        self._adapter.ack(inbound_message)
                    )
                except Exception as e:
                    await self._safely_exec_async_task(
                        self._adapter.nack(inbound_message)
                    )

                    final_exception_handler = (
                        exception_handler or self._subscriber_exception_handler
                    )

                    if final_exception_handler:
                        await self._safely_exec_async_task(final_exception_handler(e))  # type: ignore[arg-type]
                    else:
                        logger.exception(e)

            logger.debug(
                f"Registering listener to events matching listen expression {listen_expression} ({listen_expression.model_dump()})"
            )

            listener = AsyncListener(
                listen_expression=listen_expression,
                callback=listener_callback,
                event_body_type=event_body_type,
            )

            self._listeners.append(listener)

            return listener

        return decorator

    async def listen(self) -> None:
        if self._listener_state != ListenerState.STOPPED:
            raise Exception(
                "The AsyncEventStream cannot be started given the current state."
            )

        logger.debug("Starting listeners...")

        self._listener_state = ListenerState.RUNNING

        if self._listeners:
            await self._safely_exec_async_task(self._adapter.listen(self._listeners))

        logger.debug("All listeners have now stopped")

        self._listener_state = ListenerState.STOPPED

    async def close(self) -> None:
        if self._listener_state == ListenerState.STOPPING:
            raise Exception(
                "The AsyncEventStream is already in the process of stopping."
            )

        try:
            if self._listener_state == ListenerState.RUNNING:
                logger.debug("Stopping AsyncEventStream...")
                self._listener_state = ListenerState.STOPPING

                await self._adapter.close()

                try:
                    # TODO: Refactor to use an asyncio.Event
                    async with asyncio.timeout(30):
                        logger.debug("Polling for stopped state...")

                        while self._listener_state != ListenerState.STOPPED:
                            await asyncio.sleep(0.1)

                        logger.debug("AsyncEventStream has now stopped")
                finally:
                    self._listener_state = ListenerState.STOPPED
            else:
                # We always let the adapter do a clean-up upon close
                await self._adapter.close()
        finally:
            if self._async_tasks:
                logger.debug(
                    f"Waiting for {len(self._async_tasks)} pending tasks to complete"
                )
                logger.debug(self._async_tasks)

                await asyncio.wait_for(
                    asyncio.gather(*self._async_tasks, return_exceptions=True),
                    timeout=30,
                )

                logger.debug("All pending tasks are now complete")

                # Whilst we're finishing up our pending tasks, these may create additional tasks. As an example,
                # a subscriber doing its thing may cause an ack to happen during shutdown. We therefore want to be
                # a good citizen and handle those here too; otherwise we'll end up destroying pending tasks on
                # shutdown.
                if self._async_tasks:
                    logger.debug(
                        f"Waiting for {len(self._async_tasks)} pending shutdown tasks to complete"
                    )
                    logger.debug(self._async_tasks)

                    await asyncio.wait_for(
                        asyncio.gather(*self._async_tasks, return_exceptions=True),
                        timeout=15,
                    )

                    logger.debug("All pending shutdown tasks are now complete")
