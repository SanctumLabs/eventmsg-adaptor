# from typing import Union

# TODO: Uncomment the below once aiobotocore is installed & setup correctly
# from types_aiobotocore_sqs.type_defs import (
#     MessageAttributeValueTypeDef as MessageAttributeValueTypeDef2,
# )
# from types_aiobotocore_sqs.type_defs import MessageTypeDef as MessageTypeDef2
from mypy_boto3_sqs.type_defs import (
    MessageAttributeValueTypeDef as MessageAttributeValueTypeDef1,
)
from mypy_boto3_sqs.type_defs import MessageTypeDef as MessageTypeDef1

MessageAttributeValueTypeDef = MessageAttributeValueTypeDef1

# TODO: uncomment once aiobotocore is installed & setup & update the above line
# MessageAttributeValueTypeDef = Union[
#     MessageAttributeValueTypeDef1, MessageAttributeValueTypeDef2
# ]

MessageTypeDef = MessageTypeDef1

# TODO: uncomment once aiobotocore is installed & setup & update the above line
# MessageTypeDef = Union[MessageTypeDef1, MessageTypeDef2]
