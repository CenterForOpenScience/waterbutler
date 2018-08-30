import pytest

from tests.server.api.v1.fixtures import (
    celery_dest_copy_params,
    celery_dest_copy_params_root,
    celery_src_copy_params,
    handler,
    handler_auth,
    http_request,
    mock_file_metadata,
    mock_inter,
    mock_intra,
    move_copy_args,
    patch_auth_handler,
    patch_make_provider_move_copy,
    serialized_metadata,
    serialized_request
)
from waterbutler.core import exceptions


@pytest.mark.usefixtures('patch_auth_handler', 'patch_make_provider_move_copy')
class TestMoveOrCopy:

    def test_build_args(self, handler, move_copy_args):
        assert handler.build_args() == move_copy_args

    @pytest.mark.asyncio
    async def test_move_or_copy_invalid_action(self, handler):
        handler._json = {'action': 'invalid'}

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.move_or_copy()

        assert exc.value.message == 'Auth action must be "copy", "move", or "rename", not "invalid"'

    @pytest.mark.asyncio
    async def test_move_or_copy_invalid_path(self, handler):
        handler._json = {'action': 'copy'}

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.move_or_copy()

        assert exc.value.message == '"path" field is required for moves or copies'

    @pytest.mark.asyncio
    async def test_move_or_copy_invalid_path_slash(self, handler):
        handler._json = {'action': 'copy', 'path': '/file'}
        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.move_or_copy()

        assert exc.value.message == '"path" field requires a trailing' \
                                    ' slash to indicate it is a folder'

    @pytest.mark.asyncio
    @pytest.mark.parametrize('action', ['move', 'copy'])
    async def test_inter_move_copy(self, action, handler, mock_inter, mock_file_metadata,
                                   serialized_metadata, celery_src_copy_params,
                                   celery_dest_copy_params, serialized_request):
        mock_make_provider, mock_celery = mock_inter
        handler._json = {'action': action, 'path': '/test_path/'}

        await handler.move_or_copy()

        mock_make_provider.assert_called_with('MockProvider',
                                              handler.auth['auth'],
                                              handler.auth['credentials'],
                                              handler.auth['settings'])
        handler.write.assert_called_with(serialized_metadata)
        assert handler.dest_meta == mock_file_metadata
        mock_celery.assert_called_with(celery_src_copy_params,
                                       celery_dest_copy_params,
                                       conflict='warn',
                                       rename=None,
                                       request=serialized_request)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('action', ['move', 'copy'])
    async def test_intra_move_copy(self, action, handler, mock_intra, serialized_metadata,
                                   mock_file_metadata):
        handler._json = {'action': action, 'path': '/test_path/'}
        mock_make_provider, mock_celery = mock_intra

        await handler.move_or_copy()

        mock_make_provider.assert_called_with('MockProvider',
                                              handler.auth['auth'],
                                              handler.auth['credentials'],
                                              handler.auth['settings'])
        mock_celery.assert_called_with(getattr(handler.provider, action),
                                       handler.provider,
                                       handler.path,
                                       handler.dest_path,
                                       conflict='warn',
                                       rename=None)
        handler.write.assert_called_with(serialized_metadata)
        assert handler.dest_meta == mock_file_metadata

    @pytest.mark.asyncio
    async def test_invalid_rename(self, handler):
        handler._json = {'action': 'rename', 'path': '/test_path/'}

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.move_or_copy()

        assert exc.value.message == '"rename" field is required for renaming'

    @pytest.mark.asyncio
    async def test_move_or_copy_invalid_rename_root(self, handler):
        handler._json = {'action': 'copy', 'path': '/'}
        handler.path = '/'

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await handler.move_or_copy()

        assert exc.value.message == '"rename" field is required for copying root'

    @pytest.mark.asyncio
    async def test_rename(self, handler_auth, handler, mock_inter, mock_file_metadata,
                          serialized_metadata, celery_src_copy_params,
                          celery_dest_copy_params_root, serialized_request):

        mock_make_provider, mock_celery = mock_inter
        handler._json = {'action': 'rename', 'rename': 'renamed path', 'path': '/test_path/'}

        await handler.move_or_copy()

        assert handler.dest_auth == handler_auth
        assert handler.dest_path == handler.path.parent
        assert handler.dest_resource == handler.resource

        mock_make_provider.assert_called_with('test',
                                              handler.auth['auth'],
                                              handler.auth['credentials'],
                                              handler.auth['settings'])
        handler.write.assert_called_with(serialized_metadata)
        assert handler.dest_meta == mock_file_metadata
        mock_celery.assert_called_with(celery_src_copy_params,
                                       celery_dest_copy_params_root,
                                       conflict='warn',
                                       rename='renamed path',
                                       request=serialized_request)
