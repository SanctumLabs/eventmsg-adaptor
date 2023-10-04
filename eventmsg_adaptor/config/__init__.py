"""Configuration for Root Adapter
"""
from typing import Optional, Annotated, TypeVar
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from .kafka import KafkaConfig
from .sqs import SQSConfig

T = TypeVar("T")

EnvField = Annotated[T, Field]

class AdapterConfigs(BaseModel):
    kafka: Optional[KafkaConfig] = KafkaConfig()
    sqs: Optional[SQSConfig] = SQSConfig()


class Config(BaseSettings):
    """Configuration for Adapters. Sets up the service name, default adapter and configuration to available adapter configs
    """
    service_name: str = "sanctumlabs"
    default_adapter: str = "kafka"
    adapters: Optional[AdapterConfigs] = AdapterConfigs()
    
    env_prefix: EnvField[str] = "eventing_"
    env_nested_delimiter: EnvField[str] = "__"
