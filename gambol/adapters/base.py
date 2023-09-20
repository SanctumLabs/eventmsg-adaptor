from abc import ABC, abstractmethod
from typing import Any, List, Type
from gambol.schema import (
    AsyncListener,
    Destination,
    Event,
    EventBody,
    InboundMessage,
    ListenerCallback,
    ListenExpression,
)

class BaseAdapter(ABC):
    """Base Adapter from which other adapters will extend. This provides a uniform way for adapters to be used in the library"""
    
    @abstractmethod
    def publish(self, destination: Destination, event: Event) -> Any:
        """Publishes an event to a given destination

        Args:
            destination (Destination): Destination of event
            event (Event): Event to publish

        Returns:
            Any: A catch all for what the publisher returns on publishing an event.
        """
        pass
    
    @abstractmethod
    def subscribe(self, listen_expression: ListenExpression, callback: ListenerCallback, event_body_type: Type[EventBody]) -> None:
        """Subscribes on a given lister_expression and passes the event received to the given callback.

        Args:
            listen_expression (ListenExpression): Expression that is parsed and used to listen for messages
            callback (ListenerCallback): callback function that is called on receiving a message
            event_body_type (Type[EventBody]): type of event
        """
        pass
    
    @abstractmethod
    def ack(self, message: InboundMessage) -> Any:
        """Acknowledges a given message

        Args:
            message (InboundMessage): message to acknowledge

        Returns:
            Any: Catch all for any type on performing the acknowledgement of a message
        """
        pass
    
    @abstractmethod
    def nack(self, message: InboundMessage) -> Any:
        """None Acknowledgement of a given message

        Args:
            message (InboundMessage): message to not acknowledge

        Returns:
            Any: Catch all for any type on performing a non-acknowledgement of a message
        """
        pass


class BaseAsyncAdapter(ABC):
    """Base Async Adapter from which other adapters can extend when handling async operations. 
    This provides a uniform way for adapters to be used in the library in an async fashion"""
    
    @abstractmethod
    async def publish(self, destination: Destination, event: Event) -> Any:
        """Publishes an event to a given destination

        Args:
            destination (Destination): Destination of event
            event (Event): Event to publish

        Returns:
            Any: A catch all for what the publisher returns on publishing an event.
        """
        pass
    
    @abstractmethod
    async def subscribe(self, listen_expression: ListenExpression, callback: ListenerCallback, event_body_type: Type[EventBody]) -> None:
        """Subscribes on a given lister_expression and passes the event received to the given callback.

        Args:
            listen_expression (ListenExpression): Expression that is parsed and used to listen for messages
            callback (ListenerCallback): callback function that is called on receiving a message
            event_body_type (Type[EventBody]): type of event
        """
        pass
    
    @abstractmethod
    async def ack(self, message: InboundMessage) -> Any:
        """Acknowledges a given message

        Args:
            message (InboundMessage): message to acknowledge

        Returns:
            Any: Catch all for any type on performing the acknowledgement of a message
        """
        pass
    
    @abstractmethod
    async def nack(self, message: InboundMessage) -> Any:
        """None Acknowledgement of a given message

        Args:
            message (InboundMessage): message to not acknowledge

        Returns:
            Any: Catch all for any type on performing a non-acknowledgement of a message
        """
        pass
