from unittest import mock

import pytest
import tornado

from tests import utils
from tests.server.api.v1.utils import ServerTestCase

from waterbutler.auth.osf import settings
from waterbutler.core.auth import AuthType
from waterbutler.auth.osf.handler import OsfAuthHandler
from waterbutler.core.exceptions import (UnsupportedHTTPMethodError,
                                            UnsupportedActionError)


class TestOsfAuthHandler(ServerTestCase):

    def setUp(self):
        super().setUp()

        self.handler = OsfAuthHandler()
        self.request = tornado.httputil.HTTPServerRequest(uri=settings.API_URL)

        mock_auth = utils.MockCoroutine(return_value={'auth': {}, 'callback_url': 'test.com'})
        self.mock_auth_patcher = mock.patch(
            'waterbutler.auth.osf.handler.OsfAuthHandler.make_request',
            mock_auth
        )
        self.mock_auth_patcher.start()

    def tearDown(self):
        self.mock_auth_patcher.stop()
        super().tearDown()

    @tornado.testing.gen_test
    async def test_supported_and_unsupported_methods(self):

        supported_methods = ['put', 'get', 'head', 'delete']
        post_actions = ['copy', 'rename', 'move']
        unsupported_actions = ['ma1f0rmed', 'put', 'get', 'head', 'delete']
        unsupported_methods = ['post', 'trace', 'connect', 'patch', 'ma1f0rmed']
        resource = 'test'
        provider = 'test'

        assert all(method in self.handler.ACTION_MAP.keys() for method in supported_methods)

        for auth_type in AuthType:
            for action in post_actions:
                self.request.method = 'post'
                await self.handler.get(resource, provider,
                                       self.request, action=action, auth_type=auth_type)

        for method in supported_methods:
            self.request.method = method
            await self.handler.get(resource, provider, self.request)

        for method in unsupported_methods:
            self.request.method = method
            with pytest.raises(UnsupportedHTTPMethodError):
                await self.handler.get(resource, provider, self.request)

        for action in unsupported_actions:
            self.request.method = 'post'
            with pytest.raises(UnsupportedActionError):
                await self.handler.get(resource, provider, self.request, action=action)

    @tornado.testing.gen_test
    async def test_permissions_post_copy_source_destination(self):

        resource = 'test'
        provider = 'test'
        action = 'copy'
        self.request.method = 'post'

        self.handler.build_payload = mock.Mock()

        for auth_type in AuthType:
            await self.handler.get(resource, provider, self.request, action=action, auth_type=auth_type)
            if auth_type is AuthType.SOURCE:
                self.handler.build_payload.assert_called_with({
                    'nid': 'test',
                    'provider': 'test',
                    'action': 'download',
                    'intent': 'copyfrom',
                    'path': '',
                    'version': None,
                    'metrics': {
                        'referrer': None,
                        'origin': None,
                        'uri': settings.API_URL,
                        'user_agent': None
                    }
                }, cookie=None, view_only=None)
            else:
                self.handler.build_payload.assert_called_with({
                    'nid': 'test',
                    'provider': 'test',
                    'action': 'upload',
                    'intent': 'copyto',
                    'path': '',
                    'version': None,
                    'metrics': {
                        'referrer': None,
                        'origin': None,
                        'uri': settings.API_URL,
                        'user_agent': None
                    }
                }, cookie=None, view_only=None)


class TestActionMapping:

    @pytest.mark.asyncio
    @pytest.mark.parametrize('method, action, path, auth_type, headers, query_args, exp_perm_action, exp_intent', [
        ['post', 'copy',   '/folder/', AuthType.SOURCE,      None, None, 'download', 'copyfrom'],
        ['post', 'copy',   '/folder/', AuthType.DESTINATION, None, None, 'upload',   'copyto'],
        ['post', 'move',   '/folder/', AuthType.SOURCE,      None, None, 'delete',   'movefrom'],
        ['post', 'move',   '/folder/', AuthType.DESTINATION, None, None, 'upload',   'moveto'],
        ['post', 'rename', '/folder/', AuthType.SOURCE,      None, None, 'upload',   'rename'],
        ['post', 'rename', '/folder/', AuthType.DESTINATION, None, None, 'upload',   'rename'],

        ['post', 'copy',   '/file', AuthType.SOURCE,      None, None, 'download', 'copyfrom'],
        ['post', 'copy',   '/file', AuthType.DESTINATION, None, None, 'upload',   'copyto'],
        ['post', 'move',   '/file', AuthType.SOURCE,      None, None, 'delete',   'movefrom'],
        ['post', 'move',   '/file', AuthType.DESTINATION, None, None, 'upload',   'moveto'],
        ['post', 'rename', '/file', AuthType.SOURCE,      None, None, 'upload',   'rename'],
        ['post', 'rename', '/file', AuthType.DESTINATION, None, None, 'upload',   'rename'],

        ['head', None, '/folder/', None, {settings.MFR_ACTION_HEADER: 'render'}, None, 'render', 'render'],
        ['head', None, '/folder/', None, {settings.MFR_ACTION_HEADER: 'export'}, None, 'export', 'export'],

        ['head', None, '/file', None, {settings.MFR_ACTION_HEADER: 'render'}, None, 'render', 'render'],
        ['head', None, '/file', None, {settings.MFR_ACTION_HEADER: 'export'}, None, 'export', 'export'],

        ['get',    None, '/folder/', None, None, None,               'metadata',  'metadata'],
        ['head',   None, '/folder/', None, None, None,               'metadata',  'metadata'],
        ['put',    None, '/folder/', None, None, {'kind': 'folder'}, 'upload',    'create_dir'],
        ['put',    None, '/folder/', None, None, {'kind': 'file'},   'upload',    'create_file'],
        ['delete', None, '/folder/', None, None, None,               'delete',    'delete'],
        ['get',    None, '/folder/', None, None, {'meta': 1},        'metadata',  'metadata'],
        ['get',    None, '/folder/', None, None, {'revisions': 1},   'metadata',  'metadata'],
        ['get',    None, '/folder/', None, None, {'zip': 1},         'download',  'daz'],

        ['get',    None, '/file', None, None, None,                        'download',  'download'],
        ['head',   None, '/file', None, None, None,                        'metadata',  'metadata'],
        ['put',    None, '/file', None, None, None,                        'upload',    'update_file'],
        ['delete', None, '/file', None, None, None,                        'delete',    'delete'],
        ['get',    None, '/file', None, None, {'meta': 1},                 'metadata',  'metadata'],
        ['get',    None, '/file', None, None, {'revisions': 1},            'revisions', 'revisions'],
        ['get',    None, '/file', None, None, {'zip': 1},                  'download',  'download'],
        ['get',    None, '/file', None, None, {'meta': 1, 'revisions': 1}, 'metadata',  'metadata'],
    ])
    async def test_action_type(self, method, action, path, auth_type, headers, query_args,
                               exp_perm_action, exp_intent):

        handler = OsfAuthHandler()

        request = mock.Mock()
        request.method = method
        request.headers = headers if headers is not None else {}
        request.query_arguments = query_args if query_args is not None else {}
        request.cookies = {}

        kwargs = {'path': path}
        if action is not None:
            kwargs['action'] = action
        if auth_type is not None:
            kwargs['auth_type'] = auth_type

        handler.build_payload = mock.Mock()
        handler.make_request = utils.MockCoroutine(
            return_value={'auth': {}, 'callback_url': 'dummy'}
        )

        await handler.get('test', 'test', request, **kwargs)

        handler.build_payload.asssert_called_once()
        args, _ = handler.build_payload.call_args
        assert args[0]['action'] == exp_perm_action
        assert args[0]['intent'] == exp_intent

    @pytest.mark.asyncio
    async def test_unhandled_mfr_action(self):
        handler = OsfAuthHandler()
        request = mock.Mock()
        request.method = 'head'
        request.headers = {settings.MFR_ACTION_HEADER: 'bad-action'}
        with pytest.raises(UnsupportedActionError):
            await handler.get('test', 'test', request)
