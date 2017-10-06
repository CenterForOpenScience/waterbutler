import functools
from urllib import parse

from waterbutler.core import path


class BitbucketPathPart(path.WaterButlerPathPart):

    bitbucket_safe_chars = '~`!@$^&*()_-+={}|[];:,<.>"\' '

    DECODE = parse.unquote
    # TODO: mypy lacks a syntax to define kwargs for callables
    ENCODE = functools.partial(parse.quote, safe=bitbucket_safe_chars)  # type: ignore

    def increment_name(self, _id=None):
        """Overridden to preserve branch from _id upon incrementing"""
        self._id = _id or (self._id[0], self._id[1])
        self._count += 1
        return self


class BitbucketPath(path.WaterButlerPath):
    """The ``identifier`` for BitbucketPaths are tuples of ``(commit_sha, branch_name)``. Children
    of BitbucketPaths inherit their parent's ``commit_sha`` and ``branch_name``.  Either one may be
    ``None``.
    """
    PART_CLASS = BitbucketPathPart

    @property
    def commit_sha(self):
        return self.identifier[0]

    @property
    def branch_name(self):
        """Name of branch this path is on.  May be ``None`` if commit sha was given."""
        return self.identifier[1]

    @property
    def ref(self):
        """commit sha or branch name on which this file exists"""
        return self.commit_sha or self.branch_name

    @property
    def extra(self):
        return dict(super().extra, **{
            'commitSha': self.commit_sha,
            'branchName': self.branch_name,
        })

    def child(self, name, _id=None, folder=False):
        """Pass current branch down to children"""
        if _id is None:
            _id = (self.commit_sha, self.branch_name)
        return super().child(name, _id=_id, folder=folder)

    def set_commit_sha(self, commit_sha):
        for part in self.parts:
            part._id = (commit_sha, part._id[1])

    def path_tuple(self):
        return (x.value for x in self.parts[1:])
