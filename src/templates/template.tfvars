terragrunt {
  terraform {
    source = "git::https://github.com/dfds/infrastructure-modules.git//security/org-account-context?ref=TF_MODULE_VERSION"
  }

  include = {
    path = "${find_in_parent_folders()}"
  }
}

name = "dfds-ACCOUNTNAME"

email = "aws.ACCOUNTNAME@dfds.com"

context_id = "CONTEXT_ID"

correlation_id = "CORRELATION_ID"

capability_name = "CAPABILITY_NAME"

capability_root_id = "CAPABILITY_ROOT_ID"

context_name = "CONTEXT_NAME"

capability_id = "CAPABILITY_ID"
