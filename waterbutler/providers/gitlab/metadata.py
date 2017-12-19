import logging

from furl import furl

from waterbutler.core import utils
from waterbutler.core import metadata


logger = logging.getLogger(__name__)


class BaseGitLabMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path_obj = path

    @property
    def provider(self) -> str:
        return 'gitlab'

    @property
    def path(self) -> str:
        return self.build_path()

    @property
    def name(self) -> str:
        return self._path_obj.name

    @property
    def branch_name(self) -> str:
        return self._path_obj.branch_name

    @property
    def commit_sha(self) -> str:
        return self._path_obj.commit_sha

    @property
    def extra(self) -> dict:
        return {
            'commitSha': self.commit_sha,
            'branch': self.branch_name,  # may be None if revision id is a sha
        }

    def build_path(self, *args) -> str:
        return super().build_path(self._path_obj.raw_path)

    def _json_api_links(self, resource) -> dict:
        """Update JSON-API links to add branch, if available"""
        links = super()._json_api_links(resource)

        ref = None
        if self.commit_sha is not None:
            ref = {'commitSha': self.commit_sha}
        elif self.branch_name is not None:
            ref = {'branch': self.branch_name}

        if ref is not None:
            for action, link in links.items():
                links[action] = furl(link).add(ref).url

        for action in ['delete', 'upload', 'new_folder']:
            if action in links:
                links[action] = None

        return links


class GitLabFileMetadata(BaseGitLabMetadata, metadata.BaseFileMetadata):
    """Metadata for files stored on GitLab.

    GitLabFileMetadata objects constructed from tree listings (i.e. metadata objects constructed
    as part of listing the parent folder's contents) will have the ``size`` property set to `None`.
    When metadata for the file is requested directly, the ``size`` will be present.

    The GitLab provider does not support ``modified``, ``modified_utc``, or ``created_utc`` for
    any files.  GitLab also does not do content-type detection, so the content-type is guess in WB
    from the file extension.
    """

    def __init__(self, raw, path, host=None, owner=None, repo=None):
        super().__init__(raw, path)
        self._path_obj = path
        self.host = host
        self.owner = owner
        self.repo = repo

    @property
    def modified(self) -> str:
        return self.raw.get('modified', None)

    @property
    def created_utc(self) -> str:
        created = self.raw.get('created', None)
        if created is not None:
            created = utils.normalize_datetime(created)
        return created

    @property
    def content_type(self) -> str:
        return self.raw.get('mime_type', None)

    @property
    def size(self) -> int:
        return self.raw.get('size', None)

    @property
    def etag(self) -> str:
        return '{}::{}'.format(self.path, self.commit_sha or self.branch_name)

    @property
    def extra(self) -> dict:
        return dict(super().extra, **{
            'webView': self.web_view
        })

    @property
    def web_view(self) -> str:
        return '{}/{}/{}/blob/{}{}'.format(self.host, self.owner, self.repo,
                                           self.branch_name, self.path)


class GitLabFolderMetadata(BaseGitLabMetadata, metadata.BaseFolderMetadata):
    pass


class GitLabRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self) -> str:
        return 'commitSha'

    @property
    def modified(self) -> str:
        return self.raw['committed_date']

    @property
    def version(self) -> str:
        return self.raw['id']

    @property
    def extra(self) -> dict:
        return {
            'user': {
                'name': self.raw['author_name'],
            },
        }
