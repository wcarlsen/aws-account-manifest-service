from github import Github, Repository, ContentFile
from typing import List
from uuid import UUID


class GithubRepoUploader(object):
    """
    Github repo tfvars uploader
    """

    def __init__(
        self,
        github_access: Github,
        repository: str,
        context_id: UUID,
        capability_name: str,
        tfvars_content: str,
    ) -> None:
        self.repo: Repository = github_access.get_repo(repository)
        self.repo_content: List[ContentFile] = self.repo.get_contents("")
        self.context_id: UUID = context_id
        self.capability_name: str = capability_name
        self.full_filename: str = str(context_id) + "/" + "terraform.tfvars"
        self.content: str = tfvars_content

    def context_exists_in_repo(self) -> bool:
        return str(self.context_id) in [content.name for content in self.repo_content]

    def upload_context_aws_account(self) -> None:
        self.repo.create_file(
            self.full_filename,
            "Added AWS account tfvars for " + self.capability_name,
            self.content,
            branch="master",
        )
