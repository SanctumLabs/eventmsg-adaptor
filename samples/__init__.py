import json
import logging
import os

from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from eventmsg_adaptor.config import Config
from eventmsg_adaptor.schema import Event

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
path_to_dot_env = os.path.join(CURRENT_DIR, ".env")

load_dotenv(path_to_dot_env)

logging.basicConfig(
    format="{asctime} - {module} - {levelname}: {message}",
    style="{",
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# The following loggers are really noisy, so make sure we never output debug logging.
for module in ["boto3", "botocore", "s3transfer", "urllib3", "aiokafka"]:
    logging.getLogger(module).setLevel(logging.WARNING)

config = Config()


def print_event(event: Event) -> None:
    print("We received a new event:")

    print(event.headers.json(indent=4))

    if isinstance(event.body, Message):
        print(
            json.dumps(
                MessageToDict(event.body, preserving_proto_field_name=True), indent=4
            )
        )
    else:
        print(event.body.json(indent=4))

    print("-" * 200)
