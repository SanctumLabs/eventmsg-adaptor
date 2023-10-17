"""Contexts
"""
from typing import Optional

from pydantic import BaseModel

from .destination import Destination


class SerializationContext(BaseModel):
    """Serialization context for a destination"""

    destination: Optional[Destination] = None
