import os

from waterbutler.core import metadata


class BaseGitLabMetadata(metadata.BaseMetadata):

    def __init__(self, raw, folder=None, commit=None):
        super().__init__(raw)
        self.folder = folder
        self.commit = commit

    @property
    def provider(self):
        return 'gitlab'

    @property
    def extra(self):
        ret = {}
        if self.commit is not None:
            ret['commit'] = self.commit
        return ret

    def build_path(self, path):
        if self.folder:
            path = os.path.join(self.folder, path.lstrip('/'))
        return super().build_path(path)


class BaseGitLabFileMetadata(BaseGitLabMetadata, metadata.BaseFileMetadata):

    def __init__(self, raw, folder=None, commit=None, web_view=None, thepath=None):
        super().__init__(raw, folder, commit)
        self.web_view = web_view
        self.givenpath = thepath

    @property
    def path(self):
        return self.build_path(self.givenpath.path)

    @property
    def modified(self):
        if not self.commit:
            return None
        return self.commit['author']['date']

    @property
    def content_type(self):
        return None

    @property
    def etag(self):
        return '{}::{}'.format(self.path, self.raw['id'])

    @property
    def extra(self):
        return dict(super().extra, **{
            'fileSha': self.raw['id'],
            'webView': self.web_view
        })


class BaseGitLabFolderMetadata(BaseGitLabMetadata, metadata.BaseFolderMetadata):

    def __init__(self, raw, folder=None, commit=None, thepath=None):
        super().__init__(raw, folder, commit)
        self.givenpath = thepath
        self.current_path = raw['name']

    @property
    def path(self):
        return '/' + self.givenpath.path + self.current_path + '/'


class GitLabFileContentMetadata(BaseGitLabFileMetadata):

    def __init__(self, raw, folder=None, commit=None, web_view=None, thepath=None):
        super().__init__(raw, folder, commit, web_view)
        self.givenpath = thepath

    @property
    def name(self):
        return self.raw['name']

    @property
    def size(self):
        return None


class GitLabFolderContentMetadata(BaseGitLabFolderMetadata):

    @property
    def name(self):
        return self.raw['name']


class GitLabFileTreeMetadata(BaseGitLabFileMetadata):

    @property
    def name(self):
        return os.path.basename(self.raw['path'])

    @property
    def size(self):
        return self.raw['size']


class GitLabFolderTreeMetadata(BaseGitLabFolderMetadata):

    @property
    def name(self):
        return os.path.basename(self.raw['path'])


# TODO dates!
class GitLabRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        return 'ref'

    @property
    def modified(self):
        return self.raw['commit']['author']['date']

    @property
    def version(self):
        return self.raw['id']

    @property
    def extra(self):
        return {
            'user': {
                'name': self.raw['commit']['committer']['name']
            }
        }
