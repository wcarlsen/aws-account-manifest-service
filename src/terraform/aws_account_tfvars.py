from typing import Optional
from uuid import UUID
from os import getenv
from messaging.events import ContextAddedToCapabilityEventMessage


# Capability context aws account tfvars class
class AWSAccountTfvars(object):
    """
    Generate AWS account tfvars
    """

    def __init__(
        self,
        context_added_message: ContextAddedToCapabilityEventMessage,
        template_path: str,
    ) -> None:
        with open(template_path) as template_file:
            self.template: str = template_file.read()
        self.account_name: str = context_added_message.payload.capability_root_id
        self.context_id: UUID = context_added_message.payload.context_id
        self.correlation_id: UUID = context_added_message.correlation_id
        self.capability_name: str = context_added_message.payload.capability_name
        self.capability_root_id: str = context_added_message.payload.capability_root_id
        self.context_name: str = context_added_message.payload.context_name
        self.capability_id: UUID = context_added_message.payload.capability_id
        self.module_version: Optional[str] = getenv("TF_MODULE_VERSION")
        if not self.module_version:
            raise ValueError(
                "Terraform module version cannot be empty, \
                consider adding TF_MODULE_VERSION environment variable"
            )

    def generate_tfvars(self) -> str:
        """
        Generate capability AWS account tfvars
        from context_added_to_capability event message
        """
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
