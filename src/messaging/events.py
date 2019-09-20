from marshmallow import EXCLUDE, RAISE, Schema, validate  # noqa: F401
from marshmallow_dataclass import dataclass
from dataclasses import field
from uuid import UUID
from typing import ClassVar, Type

# Messages class's
@dataclass
class BaseMessageEvent:
    """
    Marshmallow dataclass for parsing the base event
    """

    class Meta:
        unknown: str = EXCLUDE

    version: str = field(
        metadata={
            "required": True,
            "data_key": "version",
            "validate": validate.Length(min=1),
        }
    )
    correlation_id: UUID = field(
        metadata={"required": True, "data_key": "x-correlationId"}
    )
    sender: str = field(
        metadata={
            "required": True,
            "data_key": "x-sender",
            "validate": validate.Length(min=1),
        }
    )
    event: str = field(
        metadata={
            "required": True,
            "data_key": "eventName",
            "validate": validate.Length(min=1),
        }
    )
    Schema: ClassVar[Type[Schema]] = Schema  # noqa: F811
    # See https://pypi.org/project/marshmallow-dataclass/ for explanation of Schema


@dataclass
class ContextAddedToCapabilityPayload:
    """
    Payload marshmallow dataclass for eventName context_added_to_capability
    """

    capability_id: UUID = field(metadata={"required": True, "data_key": "capabilityId"})
    context_id: UUID = field(metadata={"required": True, "data_key": "contextId"})
    capability_root_id: str = field(
        metadata={
            "required": True,
            "data_key": "capabilityRootId",
            "validate": validate.Length(min=1),
        }
    )
    capability_name: str = field(
        metadata={
            "required": True,
            "data_key": "capabilityName",
            "validate": validate.Length(min=1),
        }
    )
    context_name: str = field(
        metadata={
            "required": True,
            "data_key": "contextName",
            "validate": validate.Length(min=1),
        }
    )


@dataclass
class ContextAddedToCapabilityEventMessage(BaseMessageEvent):
    """
    Marshmallow dataclass for parsing full message from eventName
    context_added_to_capability
    """

    class Meta:
        unknown: str = RAISE

    payload: ContextAddedToCapabilityPayload = field(
        metadata={"required": True, "data_key": "payload"}
    )
