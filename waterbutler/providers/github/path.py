from waterbutler.core import path


class GitHubPathPart(path.WaterButlerPathPart):
    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], None)
        self._count += 1
        return self


class GitHubPath(path.WaterButlerPath):
    """The ``identifier`` for GitHubPaths are tuples of ``(branch_name, file_sha)``.  Children
    of GitHubPaths inherit their parent's ``branch_name``.  ``file_sha`` may be None if the path
    has not yet been validated.
    """
    PART_CLASS = GitHubPathPart

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

    def child(self, name, _id=None, folder=False):
        """Pass current branch down to children"""
        if _id is None:
            _id = (self.branch_ref, None)
        return super().child(name, _id=_id, folder=folder)
