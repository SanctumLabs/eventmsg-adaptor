"""Listener schema. Representing listeners
"""
from typing import Type
from pydantic import BaseModel

from gambol.strenum import StrEnum

from .event import EventBody
from .listen_expression import ListenExpression
from .callbacks import AsyncListenerCallback


class AsyncListener(BaseModel):
    """Represents an async listener along with the listen expression, callback and the event body type
    """
    listen_expression: ListenExpression
    callback: AsyncListenerCallback
    event_body_type: Type[EventBody]


class ListenerState(StrEnum):
    """The State of the listener, either RUNNNING, STOPPING OR STOPPED
    """
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
