import pytest

from waterbutler.providers.figshare.path import FigsharePath


class TestFigsharePath:

    def test_folder_path(self):
        path = FigsharePath('/folder/', _ids=('', '142132'), folder=True, is_public=False)

        assert path.identifier_path == '142132/'
        assert path.identifier == '142132'
        assert path.is_dir is True
        assert path.is_folder is True
        assert path.kind == 'folder'
        assert path.name == 'folder'
        assert path.ext == ''
        assert path.path == 'folder/'
        assert str(path) == '/folder/'
        assert path.raw_path == 'folder/'
        assert path.full_path == 'folder/'
        assert path.materialized_path == '/folder/'
        assert path.extra == {}

    def test_file_path(self):
        path = FigsharePath('/folder/test.txt',
            _ids=('', '142132', '1595252'), folder=False, is_public=False)

        assert path.identifier_path == '142132/1595252'
        assert path.identifier == '1595252'
        assert path.is_dir is False
        assert path.is_folder is False
        assert path.kind == 'file'
        assert path.name == 'test.txt'
        assert path.ext == '.txt'
        assert path.path == 'folder/test.txt'
        assert str(path) == '/folder/test.txt'
        assert path.raw_path == 'folder/test.txt'
        assert path.full_path == 'folder/test.txt'
        assert path.materialized_path == '/folder/test.txt'
        assert path.extra == {}


    def test_root_path(self):
        root_path = FigsharePath('/', _ids=('', ), folder=True, is_public=False)
        assert root_path.identifier_path == ''


    def test_parent(self):
        root_path = FigsharePath('/', _ids=('', ), folder=True, is_public=False)
        folder_path = FigsharePath('/folder/', _ids=('', '142132'), folder=True, is_public=False)
        file_path = FigsharePath('/folder/test.txt',
            _ids=('', '142132', '1595252'), folder=False, is_public=False)

        assert root_path.parent is None
        assert folder_path.parent == root_path
        assert file_path.parent == folder_path

        path = file_path.parent

        assert path.identifier_path == '142132/'
        assert path.identifier == '142132'
        assert path.is_dir is True
        assert path.is_folder is True
        assert path.kind == 'folder'
        assert path.name == 'folder'
        assert path.ext == ''
        assert path.path == 'folder/'
        assert str(path) == '/folder/'
        assert path.raw_path == 'folder/'
        assert path.full_path == 'folder/'
        assert path.materialized_path == '/folder/'
        assert path.extra == {}

    def test_child(self):
        root_path = FigsharePath('/', _ids=('', ), folder=True, is_public=False)
        folder_path = FigsharePath('/folder/', _ids=('', '142132'), folder=True, is_public=False)
        file_path = FigsharePath('/folder/test.txt',
            _ids=('', '142132', '1595252'), folder=False, is_public=False)

        child_root_path = root_path.child('folder/', _id='142132', folder=True)
        child_folder_path = folder_path.child('test.txt', _id='1595252')

        assert child_root_path == folder_path
        assert child_folder_path == file_path

        assert child_root_path.identifier_path == '142132/'
        assert child_root_path.identifier == '142132'
        assert child_root_path.is_dir is True
        assert child_root_path.is_folder is True
        assert child_root_path.kind == 'folder'
        assert child_root_path.name == 'folder'
        assert child_root_path.ext == ''
        assert child_root_path.path == 'folder/'
        assert child_root_path.raw_path == 'folder/'
        assert child_root_path.full_path == 'folder/'
        assert child_root_path.materialized_path == '/folder/'
        assert child_root_path.extra == {}

        assert child_folder_path.identifier_path == '142132/1595252'
        assert child_folder_path.identifier == '1595252'
        assert child_folder_path.is_dir is False
        assert child_folder_path.is_folder is False
        assert child_folder_path.kind == 'file'
        assert child_folder_path.name == 'test.txt'
        assert child_folder_path.ext == '.txt'
        assert child_folder_path.path == 'folder/test.txt'
        assert child_folder_path.raw_path == 'folder/test.txt'
        assert child_folder_path.full_path == 'folder/test.txt'
        assert child_folder_path.materialized_path == '/folder/test.txt'
        assert child_folder_path.extra == {}
