from furl import furl

from waterbutler.core import utils
from waterbutler.core import metadata


class BaseGitLabMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path_obj = path

    @property
    def provider(self):
        return 'gitlab'

    @property
    def path(self):
        return self.build_path()

    @property
    def name(self):
        return self._path_obj.name

    @property
    def branch_name(self):
        return self._path_obj.branch_name

    @property
    def commit_sha(self):
        return self._path_obj.commit_sha

    @property
    def extra(self):
        return {
            'commitSha': self.commit_sha,
            'branch': self.branch_name,  # may be None if revision id is a sha
        }

    def build_path(self):
        return super().build_path(self._path_obj.raw_path)

    def _json_api_links(self, resource):
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
    def modified(self):
        return self.raw['modified']

    @property
    def created_utc(self):
        return utils.normalize_datetime(self.raw['created'])

    @property
    def content_type(self):
        return self.raw.get('mime_type', None)

    @property
    def size(self):
        return self.raw.get('size', None)

    @property
    def etag(self):
        return '{}::{}'.format(self.path, self.commit_sha or self.branch_name)

    @property
    def extra(self):
        return dict(super().extra, **{
            'webView': self.web_view
        })

    @property
    def web_view(self):
        return '{}/{}/{}/blob/{}{}'.format(self.host, self.owner, self.repo, self.branch_name, self.path)


class GitLabFolderMetadata(BaseGitLabMetadata, metadata.BaseFolderMetadata):
    pass


class GitLabRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'commitSha'

    @property
    def modified(self):
        return self.raw['committed_date']

    @property
    def version(self):
        return self.raw['id']

    @property
    def extra(self):
        return {
            'user': {
                'name': self.raw['author_name'],
            },
        }
