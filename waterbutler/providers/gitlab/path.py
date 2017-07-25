import functools
from urllib import parse

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.path import WaterButlerPathPart


class GitLabPathPart(WaterButlerPathPart):
    DECODE = parse.unquote
    # TODO: mypy lacks a syntax to define kwargs for callables
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore

    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], self._id[1])
        self._count += 1
        return self


class GitLabPath(WaterButlerPath):
    """The ``identifier`` for GitLabPaths are tuples of ``(commit_sha, branch_name)``. Children
    of GitLabPaths inherit their parent's ``commit_sha`` and ``branch_name``.  Either one may be
    ``None``.
    """
    PART_CLASS = GitLabPathPart

    # def __init__(self, path, _ids=(), prepend=None, folder=False):
    #     wb_path = path
    #     if path is not '/':
    #         if not path.startswith('/'):
    #             wb_path = "/{}".format(path)
    #     if path.endswith('/'):
    #         folder = True
    #     super().__init__(wb_path, _ids=_ids, prepend=prepend, folder=folder)

    @property
    def commit_sha(self):
        """Commit SHA-1"""
        return self.identifier[0]

    @property
    def branch_name(self):
        """Branch name in which this file exists"""
        return self.identifier[1]

    @property
    def extra(self):
        return dict(super().extra, **{
            'commitSha': self.commit_sha,
            'branchName': self.branch_name
        })

    def child(self, name, _id=None, folder=False):
        """Pass current branch and commit down to children"""
        if _id is None:
            _id = (self.commit_sha, self.branch_name)
        return super().child(name, _id=_id, folder=folder)

    def set_commit_sha(self, commit_sha):
        for part in self.parts:
            part._id = (commit_sha, part._id[1])

    def path_tuple(self):
        return (x.value for x in self.parts[1:])
