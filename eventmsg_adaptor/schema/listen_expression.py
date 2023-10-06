from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from .destination import Destination
from .event import Event

class ListenExpression(BaseModel):
    destination: Destination
    event_name: Optional[str] = None
    version: Optional[str] = None

    def matches(self, event: Event) -> bool:
        """Checks if this listen expressions event matches the event that is passed to it. Checks against the destination of the event
        and the destination of this expression. Returns True if the event is the same

        Args:
            event (Event): Event to check for match

        Returns:
            bool: True if the event is the same, False otherwise
        """
        event_destination = event.headers.destination

        if self.destination.is_fifo != event_destination.is_fifo:
            return False

        if self.destination.topic != event_destination.topic:
            return False

        if (
            self.destination.sub_topic
            and self.destination.sub_topic != event_destination.sub_topic
        ):
            return False

        if self.event_name and self.event_name != event.headers.event_name:
            return False

        if self.version:
            if self.version != event.headers.version:
                return False

        return True

    def __str__(self) -> str:
        """Returns the stringified representation of this expression

        Returns:
            str: stringified listen expression
        """
        parts = [str(self.destination)]

        if self.event_name:
            parts.append(f"/{self.event_name}")

            if self.version:
                parts.append(f"/{self.version}")

        return "".join(parts)
