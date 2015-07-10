import pytest

from tests import utils
from unittest import mock
from tests.utils import async
from waterbutler.core import metadata
from waterbutler.core import exceptions


@pytest.fixture
def provider1():
    return utils.MockProvider1({'user': 'name'}, {'pass': 'word'}, {})

@pytest.fixture
def provider2():
    return utils.MockProvider2({'user': 'name'}, {'pass': 'phrase'}, {})


class TestBaseProvider:

    def test_eq(self, provider1, provider2):
        assert provider1 == provider1
        assert provider2 == provider2
        assert provider1 != provider2

    def test_serialize(self, provider1):
        assert provider1.serialized() == {
            'name': 'MockProvider1',
            'auth': {'user': 'name'},
            'settings': {},
            'credentials': {'pass': 'word'}
        }

    def test_cant_intra_move(self, provider1):
        assert provider1.can_intra_move(provider1) is False

    def test_can_intra_move(self, provider2):
        assert provider2.can_intra_move(provider2) is True

    @async
    def test_exists(self, provider1):
        ret = yield from provider1.exists('somepath')
        assert isinstance(ret, metadata.BaseMetadata)

    @async
    def test_exits_doesnt_exist(self, provider1):
        ret = yield from provider1.exists(
            'somepath',
            throw=exceptions.MetadataError('', code=404)
        )
        assert ret is False

    @async
    def test_exits_raises_non_404(self, provider1):
        with pytest.raises(exceptions.MetadataError) as e:
            yield from provider1.exists(
                'somepath',
                throw=exceptions.MetadataError('', code=422)
            )

        assert e.value.code == 422

    @async
    def test_intra_copy_notimplemented(self, provider1):
        with pytest.raises(NotImplementedError):
            yield from provider1.intra_copy(provider1, None, None)

    @async
    def test_intra_move_notimplemented(self, provider1):
        with pytest.raises(NotImplementedError):
            yield from provider1.intra_copy(provider1, None, None)

    @async
    def test_intra_move_uses_intra_copy(self, provider1, monkeypatch):
        icopy_mock = utils.MockCoroutine()
        delete_mock = utils.MockCoroutine()
        icopy_mock.return_value = ({}, False)
        monkeypatch.setattr(provider1, 'intra_copy', icopy_mock)
        monkeypatch.setattr(provider1, 'delete', delete_mock)

        metadata, created = yield from provider1.intra_move(provider1, None, None)

        assert metadata == {}
        assert created is False
        assert delete_mock.called is True
        icopy_mock.assert_called_once_with(provider1, None, None) is True

    @async
    def test_create_folder_raises(self, provider1):
        with pytest.raises(exceptions.ProviderError) as e:
            yield from provider1.create_folder('Doesnt matter')

        assert e.value.code == 405
        assert e.value.data == {
            'message': 'Folder creation not supported.'
        }

    @async
    def test_revalidate_path_is_child(self, provider1):
        path = yield from provider1.validate_path('/this/is/a/path/')
        new_path = yield from provider1.revalidate_path(path, 'text_file.txt')

        assert new_path.is_file
        assert str(path) == str(new_path.parent)
        assert new_path.name == 'text_file.txt'


class TestHandleNameConflict:

    @async
    def test_no_replace_or_existing(self, provider1):
        path = yield from provider1.validate_path('/test/path')
        provider1.exists = utils.MockCoroutine(return_value=False)

        handled, exists = yield from provider1.handle_name_conflict(path)

        assert handled is path
        assert exists is False

    @async
    def test_replace(self, provider1):
        path = yield from provider1.validate_path('/test/path')
        provider1.exists = utils.MockCoroutine(return_value=True)

        handled, exists = yield from provider1.handle_name_conflict(path, conflict='replace')

        assert handled is path
        assert exists is True
        assert str(handled) == '/test/path'

    @async
    def test_replace_not_existing(self, provider1):
        path = yield from provider1.validate_path('/test/path')
        provider1.exists = utils.MockCoroutine(return_value=False)

        handled, exists = yield from provider1.handle_name_conflict(path, conflict='replace')

        assert handled is path
        assert str(handled) == '/test/path'
        assert exists is False

    @async
    def test_renames(self, provider1):
        path = yield from provider1.validate_path('/test/path')
        provider1.exists = utils.MockCoroutine(side_effect=(True, False))

        handled, exists = yield from provider1.handle_name_conflict(path, conflict='keep')

        assert handled is path
        assert exists is False
        assert str(handled) == '/test/path (1)'
        assert handled.name == 'path (1)'

    @async
    def test_renames_twice(self, provider1):
        path = yield from provider1.validate_path('/test/path')
        provider1.exists = utils.MockCoroutine(side_effect=(True, True, False))

        handled, exists = yield from provider1.handle_name_conflict(path, conflict='keep')

        assert handled is path
        assert exists is False
        assert str(handled) == '/test/path (2)'
        assert handled.name == 'path (2)'


class TestHandleNaming:

    @async
    def test_no_problem(self, provider1):
        src_path = yield from provider1.validate_path('/test/path/')
        dest_path = yield from provider1.validate_path('/test/path/')
        provider1.exists = utils.MockCoroutine(return_value=False)

        handled = yield from provider1.handle_naming(src_path, dest_path)

        assert handled == src_path.child('path', folder=True)
        assert handled.is_dir is True
        assert len(handled.parts) == 4  # Includes root
        assert handled.name == 'path'

    @async
    def test_rename_via_path(self, provider1):
        src_path = yield from provider1.validate_path('/test/name1')
        dest_path = yield from provider1.validate_path('/test/name2')
        provider1.exists = utils.MockCoroutine(return_value=False)

        handled = yield from provider1.handle_naming(src_path, dest_path)

        assert handled.name == 'name2'
        assert handled.is_file is True

    @async
    def test_rename_explicit(self, provider1):
        dest_path = yield from provider1.validate_path('/test/')
        src_path = yield from provider1.validate_path('/test/name1')
        provider1.exists = utils.MockCoroutine(return_value=False)

        handled = yield from provider1.handle_naming(src_path, dest_path, rename='name2')

        assert handled.name == 'name2'
        assert handled.is_file is True

    @async
    def test_no_problem_file(self, provider1):
        src_path = yield from provider1.validate_path('/test/path')
        dest_path = yield from provider1.validate_path('/test/path')
        provider1.exists = utils.MockCoroutine(return_value=False)

        handled = yield from provider1.handle_naming(src_path, dest_path)

        assert handled == dest_path  # == not is
        assert handled.is_file is True
        assert len(handled.parts) == 3  # Includes root
        assert handled.name == 'path'


class TestCopy:

    @async
    def test_handles_naming_false(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.copy(provider1, src_path, dest_path, handle_naming=False)

        assert provider1.handle_naming.called is False

    @async
    def test_handles_naming(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.copy(provider1, src_path, dest_path)

        provider1.handle_naming.assert_called_once_with(
            src_path,
            dest_path,
            rename=None,
            conflict='replace',
        )

    @async
    def test_passes_on_conflict(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.copy(provider1, src_path, dest_path, conflict='keep')

        provider1.handle_naming.assert_called_once_with(
            src_path,
            dest_path,
            rename=None,
            conflict='keep',
        )

    @async
    def test_passes_on_rename(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.copy(provider1, src_path, dest_path, rename='Baz')

        provider1.handle_naming.assert_called_once_with(
            src_path,
            dest_path,
            rename='Baz',
            conflict='replace',
        )

    @async
    def test_checks_can_intra_copy(self, provider1):
        provider1.can_intra_copy = mock.Mock(return_value=False)
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        yield from provider1.copy(provider1, src_path, dest_path)

        provider1.can_intra_copy.assert_called_once_with(provider1, src_path)

    @async
    def test_calls_intra_copy(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.can_intra_copy = mock.Mock(return_value=True)
        provider1.intra_copy = utils.MockCoroutine(return_value='Someratheruniquevalue')

        data = yield from provider1.copy(provider1, src_path, dest_path)

        assert data == 'Someratheruniquevalue'
        provider1.can_intra_copy.assert_called_once_with(provider1, src_path)
        provider1.intra_copy.assert_called_once_with(provider1, src_path, dest_path)

    @async
    def test_calls_folder_op_on_dir(self, provider1):
        src_path = yield from provider1.validate_path('/source/path/')
        dest_path = yield from provider1.validate_path('/destination/path/')

        provider1._folder_file_op = utils.MockCoroutine(return_value='Someratheruniquevalue')

        data = yield from provider1.copy(provider1, src_path, dest_path)

        assert data == 'Someratheruniquevalue'

        provider1._folder_file_op.assert_called_once_with(
            provider1.copy,
            provider1,
            src_path,
            dest_path.child('path', folder=True),
        )

    @async
    def test_copy_pipes_download_to_upload(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.upload = utils.MockCoroutine(return_value='Upload return')
        provider1.download = utils.MockCoroutine(return_value='Download return')

        ret = yield from provider1.copy(provider1, src_path, dest_path)

        assert ret == 'Upload return'
        provider1.download.assert_called_once_with(src_path)
        provider1.upload.assert_called_once_with('Download return', dest_path)


class TestMove:
    @async
    def test_handles_naming_false(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.move(provider1, src_path, dest_path, handle_naming=False)

        assert provider1.handle_naming.called is False

    @async
    def test_handles_naming(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.move(provider1, src_path, dest_path)

        provider1.handle_naming.assert_called_once_with(
            src_path,
            dest_path,
            rename=None,
            conflict='replace',
        )

    @async
    def test_passes_on_conflict(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.move(provider1, src_path, dest_path, conflict='keep')

        provider1.handle_naming.assert_called_once_with(
            src_path,
            dest_path,
            rename=None,
            conflict='keep',
        )

    @async
    def test_passes_on_rename(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.handle_naming = utils.MockCoroutine()

        yield from provider1.move(provider1, src_path, dest_path, rename='Baz')

        provider1.handle_naming.assert_called_once_with(
            src_path,
            dest_path,
            rename='Baz',
            conflict='replace',
        )

    @async
    def test_checks_can_intra_move(self, provider1):
        provider1.can_intra_move = mock.Mock(return_value=False)
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        yield from provider1.move(provider1, src_path, dest_path)

        provider1.can_intra_move.assert_called_once_with(provider1, src_path)

    @async
    def test_calls_intra_move(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.can_intra_move = mock.Mock(return_value=True)
        provider1.intra_move = utils.MockCoroutine(return_value='Someratheruniquevalue')

        data = yield from provider1.move(provider1, src_path, dest_path)

        assert data == 'Someratheruniquevalue'
        provider1.can_intra_move.assert_called_once_with(provider1, src_path)
        provider1.intra_move.assert_called_once_with(provider1, src_path, dest_path)

    @async
    def test_calls_folder_op_on_dir_and_delete(self, provider1):
        src_path = yield from provider1.validate_path('/source/path/')
        dest_path = yield from provider1.validate_path('/destination/path/')

        provider1.delete = utils.MockCoroutine()
        provider1._folder_file_op = utils.MockCoroutine(return_value=('Someratheruniquevalue', 'AndThen'))

        data = yield from provider1.move(provider1, src_path, dest_path)

        assert data == ('Someratheruniquevalue', 'AndThen')

        provider1.delete.assert_called_once_with(src_path)
        provider1._folder_file_op.assert_called_once_with(
            provider1.move,
            provider1,
            src_path,
            dest_path.child('path', folder=True),
        )

    @async
    def test_calls_copy_and_delete(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.delete = utils.MockCoroutine()
        provider1.copy = utils.MockCoroutine(return_value=('Copy return', 'Value'))

        ret = yield from provider1.move(provider1, src_path, dest_path)

        assert ret == ('Copy return', 'Value')
        provider1.delete.assert_called_once_with(src_path)
        provider1.copy.assert_called_once_with(
            provider1,
            src_path,
            dest_path,
            handle_naming=False
        )

    @async
    def test_no_delete_on_copy_error(self, provider1):
        src_path = yield from provider1.validate_path('/source/path')
        dest_path = yield from provider1.validate_path('/destination/path')

        provider1.delete = utils.MockCoroutine()
        provider1.copy = utils.MockCoroutine(side_effect=Exception('WHAT'))

        with pytest.raises(Exception) as e:
            yield from provider1.move(provider1, src_path, dest_path)

        assert e.value.args == ('WHAT', )
        assert provider1.delete.called is False
        provider1.copy.assert_called_once_with(
            provider1,
            src_path,
            dest_path,
            handle_naming=False
        )

    def test_build_range_header(self, provider1):
        assert 'bytes=0-' == provider1._build_range_header((0, None))
        assert 'bytes=10-' == provider1._build_range_header((10, None))
        assert 'bytes=10-100' == provider1._build_range_header((10, 100))
        assert 'bytes=-255' == provider1._build_range_header((None, 255))
