import pytest

from waterbutler.providers.onedrive.path import OneDrivePath

from tests.providers.onedrive.fixtures import (path_fixtures,
                                               root_provider_fixtures,
                                               subfolder_provider_fixtures)


class TestNewFromResponseODPRootProvider:

    def test_file_in_root(self, path_fixtures):
        test_fixtures = path_fixtures['odp_root_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['file_metadata'], 'root')

        assert od_path.identifier == test_fixtures['file_id']
        assert str(od_path) == '/toes.txt'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', test_fixtures['file_id']]

    def test_folder_in_root(self, path_fixtures):
        test_fixtures = path_fixtures['odp_root_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['folder_metadata'], 'root')

        assert od_path.identifier == test_fixtures['folder_id']
        assert str(od_path) == '/teeth/'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', test_fixtures['folder_id']]

    def test_file_in_subdir(self, path_fixtures):
        test_fixtures = path_fixtures['odp_root_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['subfile_metadata'], 'root')

        assert od_path.identifier == test_fixtures['subfile_id']
        assert str(od_path) == '/teeth/bicuspid.txt'
        assert len(od_path.parts) == 3
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root',
                       test_fixtures['folder_id'],
                       test_fixtures['subfile_id']]

    def test_fails_without_base_folder_id(self, path_fixtures):
        test_fixtures = path_fixtures['odp_root_provider']

        with pytest.raises(Exception):
            _ = OneDrivePath.new_from_response(
                test_fixtures['file_metadata'],
                base_folder_metadata=test_fixtures['folder_metadata']
            )

    def test_fails_with_wrong_base_folder_id(self, path_fixtures):
        test_fixtures = path_fixtures['odp_root_provider']

        with pytest.raises(Exception):
            _ = OneDrivePath.new_from_response(test_fixtures['file_metadata'], '123')

    def test_insert_zero_ids(self, path_fixtures):
        test_fixtures = path_fixtures['odp_root_provider']

        file_metadata = path_fixtures['deeply_nested_file_metadata']
        od_path = OneDrivePath.new_from_response(file_metadata, 'root')

        file_id = path_fixtures['deeply_nested_file_id']
        assert od_path.identifier == file_id
        assert str(od_path) == '/deep/deeper/deepest/positively abyssyal/the kraken.txt'
        assert len(od_path.parts) == 6
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', None, None, None, 'F4D50E400DFE7D4E!298', file_id]


class TestNewFromResponseOD4BRootProvider:

    def test_file_in_root(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_root_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['file_metadata'], 'root')

        assert od_path.identifier == test_fixtures['file_id']
        assert str(od_path) == '/302m2.jpeg'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', test_fixtures['file_id']]

    def test_folder_in_root(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_root_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['folder_metadata'], 'root')

        assert od_path.identifier == test_fixtures['folder_id']
        assert str(od_path) == '/folder/'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', test_fixtures['folder_id']]

    def test_file_in_subdir(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_root_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['subfile_metadata'], 'root')

        assert od_path.identifier == test_fixtures['subfile_id']
        assert str(od_path) == '/folder/8yngNkB.jpg'
        assert len(od_path.parts) == 3
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root',
                       test_fixtures['folder_id'],
                       test_fixtures['subfile_id']]

    def test_fails_without_base_folder_id(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_root_provider']

        with pytest.raises(Exception):
            _ = OneDrivePath.new_from_response(
                test_fixtures['file_metadata'],
                base_folder_metadata=test_fixtures['folder_metadata']
            )

    def test_fails_with_wrong_base_folder_id(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_root_provider']

        with pytest.raises(Exception):
            _ = OneDrivePath.new_from_response(test_fixtures['file_metadata'], '123')

    def test_insert_zero_ids(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_root_provider']

        file_metadata = path_fixtures['deeply_nested_file_metadata']
        od_path = OneDrivePath.new_from_response(file_metadata, 'root')

        file_id = path_fixtures['deeply_nested_file_id']
        assert od_path.identifier == file_id
        assert str(od_path) == '/deep/deeper/deepest/positively abyssyal/the kraken.txt'
        assert len(od_path.parts) == 6
        ids = [x.identifier for x in od_path.parts]
        assert ids == ['root', None, None, None, 'F4D50E400DFE7D4E!298', file_id]


class TestNewFromResponseODPSubfolderProvider:

    def test_file_in_root(self, path_fixtures):
        test_fixtures = path_fixtures['odp_subfolder_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['file_metadata'],
                                                 test_fixtures['root_id'])

        assert od_path.identifier == test_fixtures['file_id']
        assert str(od_path) == '/bicuspid.txt'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == [test_fixtures['root_id'],
                       test_fixtures['file_id']]


    def test_subfolder_base_is_folder(self, path_fixtures):
        test_fixtures = path_fixtures['odp_subfolder_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['folder_metadata'],
                                                 test_fixtures['root_id'])

        assert od_path.identifier == test_fixtures['folder_id']
        assert str(od_path) == '/crushers/'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == [test_fixtures['root_id'],
                       test_fixtures['folder_id']]

    def test_file_in_subdir(self, path_fixtures):
        test_fixtures = path_fixtures['odp_subfolder_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['subfile_metadata'],
                                                 test_fixtures['root_id'],
                                                 base_folder_metadata=test_fixtures['root_metadata'])

        assert od_path.identifier == test_fixtures['subfile_id']
        assert str(od_path) == '/crushers/molars.txt'
        assert len(od_path.parts) == 3
        ids = [x.identifier for x in od_path.parts]
        assert ids == [test_fixtures['root_id'],
                       test_fixtures['folder_id'],
                       test_fixtures['subfile_id']]


class TestNewFromResponseOD4BSubfolderProvider:

    def test_file_in_root(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_subfolder_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['file_metadata'],
                                                 test_fixtures['root_id'])

        assert od_path.identifier == test_fixtures['file_id']
        assert str(od_path) == '/8yngNkB.jpg'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == [test_fixtures['root_id'],
                       test_fixtures['file_id']]


    def test_subfolder_base_is_folder(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_subfolder_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['folder_metadata'],
                                                 test_fixtures['root_id'])

        assert od_path.identifier == test_fixtures['folder_id']
        assert str(od_path) == '/subfolder/'
        assert len(od_path.parts) == 2
        ids = [x.identifier for x in od_path.parts]
        assert ids == [test_fixtures['root_id'],
                       test_fixtures['folder_id']]

    def test_file_in_subdir(self, path_fixtures):
        test_fixtures = path_fixtures['od4b_subfolder_provider']

        od_path = OneDrivePath.new_from_response(test_fixtures['subfile_metadata'],
                                                 test_fixtures['root_id'],
                                                 base_folder_metadata=test_fixtures['root_metadata'])

        assert od_path.identifier == test_fixtures['subfile_id']
        assert str(od_path) == '/subfolder/_MG_5788_acr42_1000.jpeg'
        assert len(od_path.parts) == 3
        ids = [x.identifier for x in od_path.parts]
        assert ids == [test_fixtures['root_id'],
                       test_fixtures['folder_id'],
                       test_fixtures['subfile_id']]

