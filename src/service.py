import logging
import sys
from os import getenv
from typing import Dict, List, Optional
from uuid import UUID
import structlog
from json import JSONDecodeError
from marshmallow import ValidationError
from confluent_kafka import Consumer, Message, KafkaException
from messaging.events import BaseMessageEvent, ContextAddedToCapabilityEventMessage
from github import Github, Repository

# Logging
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        # structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

LOG: structlog.BoundLogger = structlog.get_logger()

# Github integration
GITHUB_REPO_PATH: str = "wcarlsen/py2github"
github_access: Github = Github(getenv("GITHUB_USERNAME"), getenv("GITHUB_PASSWORD"))
github_repo: Repository = github_access.get_repo(GITHUB_REPO_PATH)

# Terraform module version
TF_MODULE_VERSION: Optional[str] = getenv("TF_MODULE_VERSION")

# Loading Terraform template
with open("src/templates/template.tfvars") as template_file:
    TEMPLATE: str = template_file.read()


# Capability context aws account tfvars class
class AWSAccountTfvars(object):
    """
    AWS account tfvars
    """

    def __init__(
        self,
        template: str,
        context_added_message: ContextAddedToCapabilityEventMessage,
        module_version: Optional[str],
    ) -> None:
        self.template: str = template
        self.account_name: str = context_added_message.payload.capability_root_id
        self.context_id: UUID = context_added_message.payload.context_id
        self.correlation_id: UUID = context_added_message.correlation_id
        self.capability_name: str = context_added_message.payload.capability_name
        self.capability_root_id: str = context_added_message.payload.capability_root_id
        self.context_name: str = context_added_message.payload.context_name
        self.capability_id: UUID = context_added_message.payload.capability_id
        self.module_version: Optional[str] = module_version

    def generate_tfvars(self) -> str:
        return (
            self.template.replace("ACCOUNTNAME", self.account_name)
            .replace("CONTEXT_ID", str(self.context_id))
            .replace("CORRELATION_ID", str(self.correlation_id))
            .replace("CAPABILITY_NAME", self.capability_name)
            .replace("CAPABILITY_ROOT_ID", self.capability_root_id)
            .replace("CONTEXT_NAME", self.context_name)
            .replace("CAPABILITY_ID", str(self.capability_id))
            .replace("TF_MODULE_VERSION", str(self.module_version))
        )


# Event name in scope
CONTEXT_ADDED_EVENT: str = "context_added_to_capability"

# Kafka config
KAFKA_CONFIG: Dict = {
    "bootstrap.servers": getenv("KAFKA_BROKER"),
    "group.id": "aws-account-manifest-service-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# Kafka topics
KAFKA_TOPICS: List = [
    "build.selfservice.events.capabilities",
    "build.selfservice.events.aws",
]

# Init consumer
LOG.info("Init consumer", kafka_config=KAFKA_CONFIG)
consumer: Consumer = Consumer(KAFKA_CONFIG)

# Add topic subscriptions
LOG.info("Init topic subscritions", kafka_topics=KAFKA_TOPICS)
consumer.subscribe(KAFKA_TOPICS)

# Start Consumer
LOG.info("Starting polling")
try:
    while True:
        msg: Message = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            LOG.error("Kafka error", error_message=msg.error())
            raise KafkaException(msg.error())

        else:
            try:
                # Message recieved
                MESSAGE = msg.value().decode("utf-8")
                LOG.info("Message recieved", message=MESSAGE)

                # Deserialize message
                BASE_MESSAGE: BaseMessageEvent = BaseMessageEvent.Schema().loads(
                    MESSAGE
                )

                # Message consumed
                if BASE_MESSAGE.event == CONTEXT_ADDED_EVENT:
                    CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE: ContextAddedToCapabilityEventMessage = ContextAddedToCapabilityEventMessage.Schema().loads(  # noqa: E501
                        MESSAGE
                    )
                    repo_content = github_repo.get_contents("")
                    if str(
                        CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE.payload.context_id
                    ) not in [content.name for content in repo_content]:
                        github_repo.create_file(
                            str(
                                CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE.payload.context_id
                            )
                            + "/terraform.tfvars",
                            "Added AWS account tfvars for "
                            + CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE.payload.capability_name,
                            AWSAccountTfvars(
                                template=TEMPLATE,
                                context_added_message=CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE,
                                module_version=TF_MODULE_VERSION,
                            ).generate_tfvars(),
                            branch="master",
                        )
                        LOG.info(
                            "Tfvars pushed to Github",
                            message=CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE,
                        )
                    else:
                        LOG.warning(
                            "Tfvars already exists",
                            message=CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE,
                        )
                    consumer.commit(message=msg)
                    LOG.info(
                        "Message consumed",
                        message=CONTEXT_ADDED_TO_CAPABILITY_EVENT_MESSAGE,
                    )

                else:
                    consumer.commit(message=msg)
                    LOG.info(
                        "Message out of scope", message=BASE_MESSAGE, commited=True
                    )

            except JSONDecodeError as json_err:
                consumer.commit(message=msg)
                LOG.error(
                    "Message not valid JSON format",
                    message=MESSAGE,
                    commited=True,
                    error_message=json_err.msg,
                )

            except ValidationError as valid_err:
                consumer.commit(message=msg)
                LOG.error(
                    "Message cannot be validated",
                    message=MESSAGE,
                    commited=True,
                    error_message=valid_err.messages,
                )

except KeyboardInterrupt:
    LOG.warning("Polling stopped by user")

finally:
    LOG.info("Closing consumer")
    consumer.close()
