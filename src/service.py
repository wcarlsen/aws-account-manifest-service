import logging
import sys
from os import getenv
from typing import Dict, List
import structlog
from json import JSONDecodeError
from marshmallow import ValidationError
from confluent_kafka import Consumer, Message, KafkaException
from messaging.events import BaseMessageEvent, ContextAddedToCapabilityEventMessage
from terraform.aws_account_tfvars import AWSAccountTfvars
from github import Github, GithubException
from state.github_repo_uploader import GithubRepoUploader

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
            # Message recieved
            MESSAGE = msg.value().decode("utf-8")
            LOG.info("Message recieved", message=MESSAGE)

            try:
                # Deserialize message
                BASE_MESSAGE: BaseMessageEvent = BaseMessageEvent.Schema().loads(
                    MESSAGE
                )

                # Message consume
                if BASE_MESSAGE.event == CONTEXT_ADDED_EVENT:
                    CATCEM: ContextAddedToCapabilityEventMessage = ContextAddedToCapabilityEventMessage.Schema().loads(  # noqa: E501
                        MESSAGE
                    )
                    grl: GithubRepoUploader = GithubRepoUploader(
                        github_access,
                        GITHUB_REPO_PATH,
                        CATCEM.payload.context_id,
                        CATCEM.payload.capability_name,
                        AWSAccountTfvars(
                            CATCEM, "src/templates/template.tfvars"
                        ).generate_tfvars(),
                    )
                    if not grl.context_exists_in_repo():
                        grl.upload_context_aws_account()
                        LOG.info("Tfvars pushed to Github", message=CATCEM)
                    else:
                        LOG.warning(
                            "Tfvars already exists, skipping upload", message=CATCEM
                        )
                    consumer.commit(message=msg)
                    LOG.info("Message consumed", message=CATCEM)

                else:
                    consumer.commit(message=msg)
                    LOG.info(
                        "Message out of scope", message=BASE_MESSAGE, commited=True
                    )

            # Error commit strategy
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

            except GithubException as github_err:
                LOG.error("Github error", commited=False, error_message=github_err.data)

except KeyboardInterrupt:
    LOG.warning("Polling stopped by user")

finally:
    LOG.info("Closing consumer")
    consumer.close()
