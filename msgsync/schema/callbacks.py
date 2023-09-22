"""Callbacks in the system used to represent the different types of functions this library supports
"""
from typing import Callable, Awaitable

from .message import InboundMessage
from .event import Event

ListenerCallback = Callable[[InboundMessage], None]
AsyncListenerCallback = Callable[[InboundMessage], Awaitable[None]]

ExceptionHandler = Callable[[Exception], None]
AsyncExceptionHandler = Callable[[Exception], Awaitable[None]]

EventCallback = Callable[[Event], None]
AsyncEventCallback = Callable[[Event], Awaitable[None]]