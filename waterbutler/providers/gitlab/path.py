import logging
import functools
from urllib import parse

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.path import WaterButlerPathPart


logger = logging.getLogger(__name__)


class GitLabPathPart(WaterButlerPathPart):
    DECODE = parse.unquote
    # TODO: mypy lacks a syntax to define kwargs for callables
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore


class GitLabPath(WaterButlerPath):
    """The ``identifier`` for GitLabPaths are tuples of ``(commit_sha, branch_name)``. Children
    of GitLabPaths inherit their parent's ``commit_sha`` and ``branch_name``.  Either one may be
    ``None`` at object creation, but the provider will look up and set the commit SHA if so.
    """
    PART_CLASS = GitLabPathPart

    @property
    def commit_sha(self) -> str:
        """Commit SHA-1"""
        return self.identifier[0]

    @property
    def branch_name(self) -> str:
        """Branch name in which this file exists"""
        return self.identifier[1]

    @property
    def ref(self) -> str:
        """commit sha or branch name on which this file exists"""
        return self.commit_sha or self.branch_name

    @property
    def extra(self) -> dict:
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
