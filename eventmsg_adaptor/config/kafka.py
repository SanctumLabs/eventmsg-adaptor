"""Kafka Configuration
"""
from typing import List, Optional
from enum import Enum

from pydantic import BaseModel


class SecurityProtocol(Enum):
    """Security Protocol types to connect to brokers

    Args:
        PLAINTEXT (str): Plain text security protocol
        SASL_PLAINTEXT (str): sasl plain text
        SASL_SSL (str): sasl ssl
    """

    PLAINTEXT = "plaintext"
    SASL_PLAINTEXT = "sasl_plaintext"
    SASL_SSL = "sasl_ssl"


class KafkaSecurityProtocolConfig(BaseModel):
    """Security protocol configuration. This is used for setting up security settings to use while connecting to Kafka

    Args:
        security_protocol (SecurityProtocol): Security protocol to use
        sasl_mechanisms: (str): sasl mechanism to use
        sasl_username: (str): sasl username to use
        sasl_password: (str): sasl password to use
    """

    security_protocol: Optional[SecurityProtocol] = None
    sasl_mechanisms: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None


class KafkaSchemaRegistryConfig(BaseModel):
    """Schema Registry configuration

    Args:
        schema_registry_url (str): Schema registry URL
        schema_registry_user_info (str): Schema Registry User information used for authentication
    """

    schema_registry_url: Optional[str] = None
    schema_registry_user_info: Optional[str] = None


class KafkaConfig(BaseModel):
    """KafkaConfig is a class that represents Kafka configuration that is passed to the Kafka Adapter

    Args:
        bootstrap_servers (list): list of host to port mappings of Kafka Brokers in the format host:port
    """

    # bootstrap_servers is a list of host to port mappings of Kafka Brokers in the format host:port
    bootstrap_servers: List[str] = ["localhost:9092"]
    client_id: Optional[str] = None
    security: Optional[KafkaSecurityProtocolConfig] = None
    schema_registry: Optional[KafkaSchemaRegistryConfig] = None
