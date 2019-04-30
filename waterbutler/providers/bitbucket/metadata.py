from email.utils import parseaddr

from furl import furl

from waterbutler.core import metadata


class BaseBitbucketMetadata(metadata.BaseMetadata):
    """Metadata properties common to Bitbucket files and folders

    *commit*: The commit sha that this metadata snapshot applies to.
     The commit may not actually involve changes to the entity.

    *branch*: A branch is mutable pointer to a commit sha.  If a branch name was used to retrieve
    the metadata for this entity, this property will be set. Otherwise, it is ``None``.

    *ref*:  The ref (commit or branch name) that this entity belongs to.  For mutating
    actions, this is the ref after the action has been committed.

    """
    def __init__(self, raw, path_obj, owner=None, repo=None):
        super().__init__(raw)
        self._path_obj = path_obj
        self.owner = owner
        self.repo = repo

    @property
    def provider(self):
        return 'bitbucket'

    @property
    def path(self):
        return self.build_path()

    @property
    def name(self):
        return self._path_obj.name

    @property
    def commit_sha(self):
        return self._path_obj.commit_sha

    @property
    def branch_name(self):
        return self._path_obj.branch_name

    @property
    def extra(self):
        return {
            'commitSha': self.commit_sha,
            'branch': self.branch_name,  # may be None if revision id is a sha
        }

    def build_path(self):
        return super().build_path(self._path_obj.raw_path)

    def _json_api_links(self, resource):
        """Update JSON-API links to add commitSha or branch, if available"""
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


class BitbucketFileMetadata(BaseBitbucketMetadata, metadata.BaseFileMetadata):

    @property
    def size(self):
        return self.raw['size']

    @property
    def modified(self):
        return self.raw.get('timestamp', None)

    @property
    def created_utc(self):
        return self.raw.get('created_utc', None)

    @property
    def content_type(self):
        return None

    @property
    def etag(self):
        return '{}::{}'.format(self.path, self.commit_sha)  # FIXME: maybe last_commit_sha?

    @property
    def extra(self):
        return dict(super().extra, **{
            'webView': self.web_view,
            'lastCommitSha': self.last_commit_sha,
        })

    @property
    def last_commit_sha(self):
        return self.raw.get('revision', None)

    @property
    def web_view(self):
        return 'https://bitbucket.org/{}/{}/src/{}{}?fileviewer=file-view-default'.format(
            self.owner, self.repo, self.commit_sha, self.path,
        )


class BitbucketFolderMetadata(BaseBitbucketMetadata, metadata.BaseFolderMetadata):
    pass


class BitbucketRevisionMetadata(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'commitSha'

    @property
    def modified(self):
        return self.raw['timestamp']

    @property
    def modified_utc(self):
        return self.raw['utctimestamp']

    @property
    def version(self):
        return self.raw['raw_node']

    @property
    def extra(self):
        return {
            'user': {
                'name': parseaddr(self.raw['raw_author'])[0]  # real name only
            },
            'branch': self.raw['branch'],
        }
