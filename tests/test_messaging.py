import pytest
from typing import Dict
from src.messaging.events import BaseMessageEvent, ContextAddedToCapabilityEventMessage
from uuid import UUID
from marshmallow import ValidationError
from json import JSONDecodeError


class TestBaseMessageEvent:
    def test_valid_message(self):
        valid_message: Dict = {
            "version": "1",
            "eventName": "context_added_to_capability",
            "x-correlationId": "a0057017-81ca-40fd-b929-2489bfee39f3",
            "x-sender": "eg. FQDN of assembly",
            "payload": {
                "capabilityId": "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c",
                "contextId": "0d03e3ad-2118-46b7-970e-0ca87b59a202",
                "capabilityRootId": "pax-bookings-A43aS",
                "capabilityName": "PAX Bookings",
                "contextName": "blue",
            },
        }
        parsed_valid_message: BaseMessageEvent = BaseMessageEvent.Schema().load(
            valid_message
        )
        assert hasattr(parsed_valid_message, "version")
        assert hasattr(parsed_valid_message, "event")
        assert hasattr(parsed_valid_message, "correlation_id")
        assert hasattr(parsed_valid_message, "sender")
        assert not hasattr(parsed_valid_message, "payload")
        assert parsed_valid_message.version == "1"
        assert parsed_valid_message.event == "context_added_to_capability"
        assert parsed_valid_message.sender == "eg. FQDN of assembly"
        assert parsed_valid_message.correlation_id == UUID(
            "a0057017-81ca-40fd-b929-2489bfee39f3"
        )

    def test_invalid_message(self):
        invalid_message: Dict = {
            "version": "1",
            "eventName": "context_added_to_capability",
            "x-sender": "eg. FQDN of assembly",
            "payload": {
                "capabilityId": "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c",
                "contextId": "0d03e3ad-2118-46b7-970e-0ca87b59a202",
                "capabilityRootId": "pax-bookings-A43aS",
                "capabilityName": "PAX Bookings",
                "contextName": "blue",
            },
        }
        with pytest.raises(ValidationError):
            BaseMessageEvent.Schema().load(invalid_message)

    def test_invalid_json_message(self):
        json_invalid_message: str = """
        {
            "version": "1",
            "eventName": "context_added_to_capability",
            "x-correlationId": "a0057017-81ca-40fd-b929-2489bfee39f3",
            "x-sender": "eg. FQDN of assembly",
            "payload": {
                "capabilityId": "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c",
                "contextId": "0d03e3ad-2118-46b7-970e-0ca87b59a202",
                "capabilityRootId": "pax-bookings-A43aS",
                "capabilityName": "PAX Bookings",
                "contextName": "blue",
            },
        }
        """
        with pytest.raises(JSONDecodeError):
            BaseMessageEvent.Schema().loads(json_invalid_message)

    def test_empty_uuid_value(self):
        empty_uuid_message: Dict = {
            "version": "1",
            "eventName": "context_added_to_capability",
            "x-correlationId": "",
            "x-sender": "eg. FQDN of assembly",
            "payload": {
                "capabilityId": "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c",
                "contextId": "0d03e3ad-2118-46b7-970e-0ca87b59a202",
                "capabilityRootId": "pax-bookings-A43aS",
                "capabilityName": "PAX Bookings",
                "contextName": "blue",
            },
        }
        with pytest.raises(ValidationError):
            BaseMessageEvent.Schema().load(empty_uuid_message)

    def test_empty_string_value(self):
        empty_string_message: Dict = {
            "version": "1",
            "eventName": "",
            "x-correlationId": "a0057017-81ca-40fd-b929-2489bfee39f3",
            "x-sender": "eg. FQDN of assembly",
            "payload": {
                "capabilityId": "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c",
                "contextId": "0d03e3ad-2118-46b7-970e-0ca87b59a202",
                "capabilityRootId": "pax-bookings-A43aS",
                "capabilityName": "PAX Bookings",
                "contextName": "blue",
            },
        }
        with pytest.raises(ValidationError):
            BaseMessageEvent.Schema().load(empty_string_message)


class TestContextAddedToCapabilityEventMessage:
    def test_valid_message(self):
        valid_message: Dict = {
            "version": "1",
            "eventName": "context_added_to_capability",
            "x-correlationId": "a0057017-81ca-40fd-b929-2489bfee39f3",
            "x-sender": "eg. FQDN of assembly",
            "payload": {
                "capabilityId": "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c",
                "contextId": "0d03e3ad-2118-46b7-970e-0ca87b59a202",
                "capabilityRootId": "pax-bookings-A43aS",
                "capabilityName": "PAX Bookings",
                "contextName": "blue",
            },
        }
        parsed_valid_message: ContextAddedToCapabilityEventMessage = ContextAddedToCapabilityEventMessage.Schema().load(  # noqa: E501
            valid_message
        )
        assert hasattr(parsed_valid_message, "version")
        assert hasattr(parsed_valid_message, "event")
        assert hasattr(parsed_valid_message, "correlation_id")
        assert hasattr(parsed_valid_message, "sender")
        assert hasattr(parsed_valid_message, "payload")
        assert hasattr(parsed_valid_message.payload, "capability_id")
        assert hasattr(parsed_valid_message.payload, "context_id")
        assert hasattr(parsed_valid_message.payload, "capability_root_id")
        assert hasattr(parsed_valid_message.payload, "capability_name")
        assert hasattr(parsed_valid_message.payload, "context_name")
        assert parsed_valid_message.version == "1"
        assert parsed_valid_message.event == "context_added_to_capability"
        assert parsed_valid_message.sender == "eg. FQDN of assembly"
        assert parsed_valid_message.correlation_id == UUID(
            "a0057017-81ca-40fd-b929-2489bfee39f3"
        )
        assert parsed_valid_message.payload.capability_id == UUID(
            "bc3f3bbe-eeee-4230-8b2f-d0e1c327c59c"
        )
        assert parsed_valid_message.payload.context_id == UUID(
            "0d03e3ad-2118-46b7-970e-0ca87b59a202"
        )
        assert parsed_valid_message.payload.capability_root_id == "pax-bookings-A43aS"
