import logging
from datetime import datetime
from typing import Any, Callable, Optional, Union
from eventmsg_adaptor.adapters.base import BaseAdapter
from eventmsg_adaptor.event_streams.base import BaseEventStream
from eventmsg_adaptor.schema import (
    Destination,
    Event,
    EventBody,
    EventCallback,
    ExceptionHandler,
    InboundMessage,
)
from eventmsg_adaptor.utils import (
    normalise_destination,
    parse_event_body_type_from_subscribe_callback,
    parse_listen_expression_str,
)


logger = logging.getLogger(__name__)


class EventStream(BaseEventStream):
    def __init__(self, adapter: BaseAdapter, event_source: str):
        self._adapter: BaseAdapter = adapter

        super().__init__(event_source)

    def publish(
        self,
        destination: Union[str, Destination],
        event_body: EventBody,
        timestamp: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
        group_id: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Any:
        """Publishes an event to a given destination

        Args:
            destination (Union[str, Destination]): Destination to publish event
            event_body (EventBody): Body of event
            timestamp (Optional[datetime], optional): timestamp of event. Defaults to None.
            correlation_id (Optional[str], optional): correlation ID. Defaults to None.
            group_id (Optional[str], optional): Group ID. Defaults to None.
            subject (Optional[str], optional): Subject of event. Defaults to None.

        Returns:
            Any: Catch all for what the publisher will return
        """
        destination = normalise_destination(destination)
        
        event = self._make_outbound_event(
            destination=destination,
            event_body=event_body,
            timestamp=timestamp,
            correlation_id=correlation_id,
            group_id=group_id,
            subject=subject
        )
        
        return self.publish_raw(destination, event)
    
    def publish_raw(self, destination: Union[str, Destination], event: Event) -> Any:
        """Publishes the raw event using the provided adapter to the given destination

        Args:
            destination (Union[str, Destination]): Destination of event
            event (Event): Event to publish

        Returns:
            Any: Catch all for what the adapter will return when publishing event
        """
        destination = normalise_destination(destination=destination)
        
        logger.debug(f"Publishing event {event} to destination {destination}")
        
        return self._adapter.publish(destination=destination, event=event)
    
    def subscribe(self, listen_expression_str: str, exception_handler: Optional[ExceptionHandler] = None) -> Callable[[EventCallback], None]:
        """Subscribes to topics given a listen expression string

        Args:
            listen_expression_str (str): Expression string to use to subscribe to events
            exception_handler (Optional[ExceptionHandler], optional): Exception Handler. Defaults to None.

        Returns:
            Callable[[EventCallback], None]: Decorator callback that handles subscription to topics
        """
        def decorator(func: EventCallback) -> None:
            """Decorator that parses listen expression and is used to pass messages to adaptor

            Args:
                func (EventCallback): Event Callback used to handle events consumed
            """
            listen_expression = parse_listen_expression_str(listen_expression_str)
            event_body_type = parse_event_body_type_from_subscribe_callback(func)
            
            def listener_callback(inbound_message: InboundMessage) -> None:
                """listener callback for inbound messages

                Args:
                    inbound_message (InboundMessage): Inbound message from a listen expression
                """
                try:
                    if listen_expression.matches(inbound_message.event):
                        logger.debug(f"Received event {inbound_message.event}")
                        
                        func(inbound_message.event)
                    
                    self._adapter.ack(inbound_message)
                except Exception as exc:
                    self._adapter.nack(inbound_message)
                    
                    if exception_handler:
                        exception_handler(exc)
                    else:
                        logger.exception(exc)
            
            logger.debug(f"Subscribing to events matching listen expression {listen_expression} ({listen_expression.model_dump()})")

            self._adapter.subscribe(
                listen_expression=listen_expression,
                callback=listener_callback,
                event_body_type=event_body_type
            )

        return decorator
