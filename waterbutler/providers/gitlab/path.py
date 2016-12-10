from waterbutler.core.path import WaterButlerPath
from waterbutler.core.path import WaterButlerPathPart


class GitLabPathPart(WaterButlerPathPart):
    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], None)
        self._count += 1
        return self


class GitLabPath(WaterButlerPath):
    """WB and GL use slightly different default conventions for their paths, so we
    often have to munge our WB paths before comparison. Here is a quick overview::

        WB (dirs):  wb_dir.path == 'foo/bar/'     str(wb_dir) == '/foo/bar/'
        WB (file):  wb_file.path = 'foo/bar.txt'  str(wb_file) == '/foo/bar.txt'
        GL (dir):   'foo/bar'
        GL (file):  'foo/bar.txt'
    """
    PART_CLASS = GitLabPathPart

    @property
    def branch_ref(self):
        """Branch name or commit sha in which this file exists"""
        return self.identifier[0]

    @property
    def file_sha(self):
        """SHA-1 of this file"""
        return self.identifier[1]

    @property
    def extra(self):
        return dict(super().extra, **{
            'ref': self.branch_ref,
            'fileSha': self.file_sha,
        })

    def __init__(self, path, _ids=(), prepend=None, folder=False):
        wb_path = path
        if path is not '/':
            if not path.startswith('/'):
                wb_path = "/{}".format(path)
        if path.endswith('/'):
            folder = True
        super().__init__(wb_path, _ids=_ids, prepend=prepend, folder=folder)

    def child(self, name, _id=None, folder=False):
        """Pass current branch down to children"""
        if _id is None:
            _id = (self.branch_ref, None)
        return super().child(name, _id=_id, folder=folder)
