import os

from furl import furl

from waterbutler.core import metadata


class BaseGitHubMetadata(metadata.BaseMetadata):
    """Metadata properties common to github files and folders

    *commit*:  The commit id that corresponds to this version of the entity.

    *ref*:  The ref (commit or branch name) that this entity belongs to.  For mutating
    actions, this is the ref after the action has been committed.
    """

    def __init__(self, raw, commit=None, ref=None):
        super().__init__(raw)
        self.commit = commit
        self.ref = ref

    @property
    def provider(self):
        return 'github'

    @property
    def extra(self):
        ret = {}
        if self.commit is not None:
            ret['commit'] = self.commit
        if self.ref is not None:
            ret['ref'] = self.ref
        return ret

    def build_path(self, path):
        return super().build_path(path)

    def _json_api_links(self, resource):
        """Update JSON-API links to add ref, if available"""
        links = super()._json_api_links(resource)

        if self.ref is not None:
            for action, link in links.items():
                links[action] = furl(link).add({'ref': self.ref}).url

        return links


class BaseGitHubFileMetadata(BaseGitHubMetadata, metadata.BaseFileMetadata):
    """BaseGitHubFileMetadata objects may be built from tree responses or content responses.  The
    response types have different fields, so users should code defensively when accessing the raw
    object.

    Tree: https://developer.github.com/v3/git/trees/#get-a-tree

    Content: https://developer.github.com/v3/repos/contents/#response-if-content-is-a-file
    """

    def __init__(self, raw, commit=None, web_view=None, ref=None):
        super().__init__(raw, commit=commit, ref=ref)
        self.web_view = web_view

    @property
    def path(self):
        return self.build_path(self.raw['path'])

    @property
    def modified(self):
        if not self.commit:
            return None
        return self.commit['author']['date']

    @property
    def created_utc(self):
        return None

    @property
    def content_type(self):
        return None

    @property
    def size(self):
        return self.raw['size']

    @property
    def etag(self):
        return '{}::{}'.format(self.path, self.raw['sha'])

    @property
    def extra(self):
        return dict(super().extra, **{
            'fileSha': self.raw['sha'],
            'webView': self.web_view,
            'hashes': {
                'git': self.raw['sha'],
            },
        })


class BaseGitHubFolderMetadata(BaseGitHubMetadata, metadata.BaseFolderMetadata):
    """BaseGitHubFolderMetadata objects may be built from tree responses or content responses.
    The response types have different fields, so users should code defensively when accessing the
    raw object.

    Tree: https://developer.github.com/v3/git/trees/#get-a-tree

    Content: https://developer.github.com/v3/repos/contents/#response-if-content-is-a-directory
    """
    @property
    def path(self):
        return self.build_path(self.raw['path'])


class GitHubFileContentMetadata(BaseGitHubFileMetadata):
    """Github file metadata object built from a content endpoint response."""

    @property
    def name(self):
        return self.raw['name']


class GitHubFolderContentMetadata(BaseGitHubFolderMetadata):
    """Github folder metadata object built from a content endpoint response."""

    @property
    def name(self):
        return self.raw['name']


class GitHubFileTreeMetadata(BaseGitHubFileMetadata):
    """Github file metadata object built from a tree endpoint response."""

    @property
    def name(self):
        return os.path.basename(self.raw['path'])


class GitHubFolderTreeMetadata(BaseGitHubFolderMetadata):
    """Github folder metadata object built from a tree endpoint response."""

    @property
    def name(self):
        return os.path.basename(self.raw['path'])


# TODO dates!
class GitHubRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'ref'

    @property
    def modified(self):
        return self.raw['commit']['author']['date']

    @property
    def created_utc(self):
        return None

    @property
    def version(self):
        return self.raw['sha']

    @property
    def extra(self):
        return {
            'user': {
                'name': self.raw['commit']['committer']['name']
            }
        }
