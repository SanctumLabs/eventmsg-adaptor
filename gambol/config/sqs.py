"""SQS Configuration
"""
from typing import Optional

from pydantic import BaseModel


class SQSConfig(BaseModel):
    """SQSConfig to setup configuration connection details to AWS SQS
    """
    region_name: str = "eu-west-1"
    # The creds below are optional to allow falling back on creds of assumed IAM roles where applicable
    # (EKS service account for e.g.)
    access_key_id: Optional[str] = None
    secret_key: Optional[str] = None
    environment: Optional[str] = None
