import re

import pytest

from waterbutler.providers.github.path import GitHubPath


class TestGitHubPath:

    def test_id_accessors(self):
        gh_path = GitHubPath('/foo', _ids=[('master', None), ('master', 'abcea54as123')])

        assert gh_path.branch_ref == 'master'
        assert gh_path.file_sha == 'abcea54as123'

    def test_child_inherits_branch(self):
        gh_parent = GitHubPath('/foo/', _ids=[('master', None), ('master', 'abcea54as123')])
        gh_child = gh_parent.child('foo')

        assert gh_child.branch_ref == 'master'
        assert gh_child.file_sha is None

    def test_increment_name(self):
        gh_path = GitHubPath('/foo/', _ids=[('master', None), ('master', 'abcea54as123')])
        old_id = gh_path.identifier
        gh_path.increment_name()

        assert old_id[0] == gh_path.identifier[0]

    def test_child_given_explicit_branch(self):
        gh_parent = GitHubPath('/foo/', _ids=[('master', None), ('master', 'abcea54as123')])
        gh_child = gh_parent.child('foo', _id=('develop', '413006763'))

        assert gh_child.branch_ref == 'develop'
        assert gh_child.file_sha == '413006763'

    def test_child_gets_branch(self):
        parent = GitHubPath('/', _ids=[('master', None)], folder=True)

        child_file = parent.child('childfile', folder=False)
        assert child_file.identifier[0] == 'master'

        child_folder = parent.child('childfolder', folder=True)
        assert child_folder.identifier[0] == 'master'

