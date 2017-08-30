import pytest

from waterbutler.providers.bitbucket.path import BitbucketPath

COMMIT_SHA = 'abcea54a123'
BRANCH = 'develop'


class TestBitbucketPath:

    def test_id_accessors(self):
        bb_path = BitbucketPath('/foo', _ids=[(COMMIT_SHA, BRANCH), (COMMIT_SHA, BRANCH)])
        assert bb_path.commit_sha == COMMIT_SHA
        assert bb_path.branch_name == BRANCH
        assert bb_path.ref == COMMIT_SHA

    def test_id_accessors_no_sha(self):
        bb_path = BitbucketPath('/foo', _ids=[(None, BRANCH), (None, BRANCH)])
        assert bb_path.commit_sha == None
        assert bb_path.branch_name == BRANCH
        assert bb_path.ref == BRANCH

    def test_id_accessors_no_branch(self):
        bb_path = BitbucketPath('/foo', _ids=[(COMMIT_SHA, None), (COMMIT_SHA, None)])
        assert bb_path.commit_sha == COMMIT_SHA
        assert bb_path.branch_name == None
        assert bb_path.ref == COMMIT_SHA

    def test_child_inherits_id(self):
        bb_parent = BitbucketPath('/foo/', _ids=[(COMMIT_SHA, BRANCH), (COMMIT_SHA, BRANCH)])
        bb_child = bb_parent.child('foo')
        assert bb_child.commit_sha == COMMIT_SHA
        assert bb_child.branch_name == BRANCH
        assert bb_child.ref == COMMIT_SHA

    def test_child_given_explicit_branch(self):
        """This one is weird.  An explicit child id should override the id from the parent,
        but I don't know if this is sensible behavior."""

        bb_parent = BitbucketPath('/foo/', _ids=[(COMMIT_SHA, BRANCH), (COMMIT_SHA, BRANCH)])
        bb_child = bb_parent.child('foo', _id=('413006763', 'master'))

        assert bb_child.commit_sha == '413006763'
        assert bb_child.branch_name == 'master'
        assert bb_child.ref == '413006763'

        assert bb_parent.commit_sha == COMMIT_SHA
        assert bb_parent.branch_name == BRANCH
        assert bb_parent.ref == COMMIT_SHA

    def test_update_commit_sha(self):
        bb_child = BitbucketPath('/foo/bar', _ids=[(None, BRANCH), (None, BRANCH), (None, BRANCH)])
        assert bb_child.commit_sha == None
        assert bb_child.branch_name == BRANCH
        assert bb_child.ref == BRANCH

        bb_child.set_commit_sha(COMMIT_SHA)
        assert bb_child.commit_sha == COMMIT_SHA
        assert bb_child.branch_name == BRANCH
        assert bb_child.ref == COMMIT_SHA

        bb_parent = bb_child.parent
        assert bb_parent.commit_sha == COMMIT_SHA
        assert bb_parent.branch_name == BRANCH
        assert bb_parent.ref == COMMIT_SHA

        bb_grandparent = bb_parent.parent
        assert bb_grandparent.commit_sha == COMMIT_SHA
        assert bb_grandparent.branch_name == BRANCH
        assert bb_grandparent.ref == COMMIT_SHA
