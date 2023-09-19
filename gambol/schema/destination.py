from typing import Optional

from pydantic import BaseModel

class Destination(BaseModel):
    """Represents a destination. This will contain the topic information as well as the sub topic information, Further containing information whether
    this is a FIFO type of destination or not.
    """
    topic: str
    sub_topic: Optional[str] = None
    is_fifo: bool = False
    
    @property
    def paths(self) -> str:
        """Returns the full path of the destination. If a subtopic is included it will be appended and returned
        in the format topic.sub_topic e.g. if topic=deliveries and sub_topic=tacos it will be: deliveries.tacos

        Returns:
            str: topic including subtopic if available
        """
        parts = [self.topic]
        
        if self.sub_topic:
            parts.append(self.sub_topic)
        
        return ".".join(parts)

    def __str__(self) -> str:
        """Returns the string representation of this destination. Note that if this is a FIFO type destination ::fifo is appended to the string

        Returns:
            str: string representation of destination
        """
        destination_str = self.paths
        
        if self.is_fifo:
            destination_str = f"{destination_str}::fifo"

        return destination_str