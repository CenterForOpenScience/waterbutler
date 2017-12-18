import pytest

from waterbutler.providers.onedrive.path import OneDrivePath

from tests.providers.onedrive.fixtures import (path_fixtures,
                                               root_provider_fixtures,
                                               subfolder_provider_fixtures)


class TestApiIdentifier:

    def test_api_identifier_none(self):
        path = OneDrivePath('/foo', _ids=('root', None,))
        assert path.api_identifier is None

    def test_api_identifier_root(self):
        path = OneDrivePath('/', _ids=('root',))
        assert path.api_identifier == ('root',)

    def test_api_identifier_folder_id(self):
        path = OneDrivePath('/', _ids=('123456',))
        assert path.api_identifier == ('items', '123456',)

    def test_api_identifier_file_id(self):
        path = OneDrivePath('/foo', _ids=('123456','7891011',))
        assert path.api_identifier == ('items', '7891011',)


class TestNewFromResponseRootProvider:

    def test_file_in_root(self, root_provider_fixtures):
        od_path = OneDrivePath.new_from_response(root_provider_fixtures['file_metadata'], 'root')

        assert od_path.identifier == root_provider_fixtures['file_id']
        assert str(od_path) == '/toes.txt'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', root_provider_fixtures['file_id']]

    def test_folder_in_root(self, root_provider_fixtures):
        od_path = OneDrivePath.new_from_response(root_provider_fixtures['folder_metadata'], 'root')

        assert od_path.identifier == root_provider_fixtures['folder_id']
        assert str(od_path) == '/teeth/'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', root_provider_fixtures['folder_id']]

    def test_file_in_subdir(self, root_provider_fixtures):
        od_path = OneDrivePath.new_from_response(root_provider_fixtures['subfile_metadata'], 'root')

        assert od_path.identifier == root_provider_fixtures['subfile_id']
        assert str(od_path) == '/teeth/bicuspid.txt'
        assert len(od_path.parts) == 3
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root',
                       root_provider_fixtures['folder_id'],
                       root_provider_fixtures['subfile_id']]

    def test_fails_without_base_folder(self, root_provider_fixtures):
        with pytest.raises(Exception):
            od_path = OneDrivePath.new_from_response(root_provider_fixtures['file_metadata'])

    def test_insert_zero_ids(self, path_fixtures):
        file_metadata = path_fixtures['deeply_nested_file_metadata']
        od_path = OneDrivePath.new_from_response(file_metadata, 'root')

        file_id = path_fixtures['deeply_nested_file_id']
        assert od_path.identifier == file_id
        assert str(od_path) == '/deep/deeper/deepest/positively abyssyal/the kraken.txt'
        assert len(od_path.parts) == 6
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', None, None, None, 'F4D50E400DFE7D4E!298', file_id]



class TestNewFromResponseSubfolderProvider:

    def test_file_in_root(self, subfolder_provider_fixtures):
        od_path = OneDrivePath.new_from_response(subfolder_provider_fixtures['file_metadata'],
                                                 subfolder_provider_fixtures['root_id'])

        assert od_path.identifier == subfolder_provider_fixtures['file_id']
        assert str(od_path) == '/bicuspid.txt'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == [subfolder_provider_fixtures['root_id'],
                       subfolder_provider_fixtures['file_id']]


    def test_subfolder_base_is_folder(self, subfolder_provider_fixtures):
        od_path = OneDrivePath.new_from_response(subfolder_provider_fixtures['folder_metadata'],
                                                 subfolder_provider_fixtures['root_id'])

        assert od_path.identifier == subfolder_provider_fixtures['folder_id']
        assert str(od_path) == '/crushers/'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == [subfolder_provider_fixtures['root_id'],
                       subfolder_provider_fixtures['folder_id']]

    def test_file_in_subdir(self, subfolder_provider_fixtures):
        od_path = OneDrivePath.new_from_response(subfolder_provider_fixtures['subfile_metadata'],
                                                 subfolder_provider_fixtures['root_id'],
                                                 base_folder_metadata=subfolder_provider_fixtures['root_metadata'])

        assert od_path.identifier == subfolder_provider_fixtures['subfile_id']
        assert str(od_path) == '/crushers/molars.txt'
        assert len(od_path.parts) == 3
        ids = [x.identifier for x in od_path.parts]
        assert ids == [subfolder_provider_fixtures['root_id'],
                       subfolder_provider_fixtures['folder_id'],
                       subfolder_provider_fixtures['subfile_id']]

