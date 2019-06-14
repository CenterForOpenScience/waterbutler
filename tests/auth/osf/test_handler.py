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
    @pytest.mark.parametrize('method, action, auth_type, headers, query_args, expected', [
        ['post', 'copy',   AuthType.SOURCE,      None, None, 'download'],
        ['post', 'copy',   AuthType.DESTINATION, None, None, 'upload'],
        ['post', 'move',   AuthType.SOURCE,      None, None, 'upload'],  # TODO: really?
        ['post', 'move',   AuthType.DESTINATION, None, None, 'upload'],
        ['post', 'rename', AuthType.SOURCE,      None, None, 'upload'],  # TODO: really?
        ['post', 'rename', AuthType.DESTINATION, None, None, 'upload'],

        ['head', None, None, {settings.MFR_ACTION_HEADER: 'render'}, None, 'render'],
        ['head', None, None, {settings.MFR_ACTION_HEADER: 'export'}, None, 'export'],

        ['get',    None, None, None, None,             'download'],
        ['put',    None, None, None, None,             'upload'],
        ['delete', None, None, None, None,             'delete'],
        ['get',    None, None, None, {'meta': 1},      'metadata'],
        ['get',    None, None, None, {'revisions': 1}, 'revisions'],
    ])
    async def test_action_type(self, method, action, auth_type, headers, query_args, expected):

        handler = OsfAuthHandler()

        request = mock.Mock()
        request.method = method
        request.headers = headers if headers is not None else {}
        request.query_arguments = query_args if query_args is not None else {}
        request.cookies = {}

        kwargs = {}
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
        assert args[0]['action'] == expected

    @pytest.mark.asyncio
    async def test_unhandled_mfr_action(self):
        handler = OsfAuthHandler()
        request = mock.Mock()
        request.method = 'head'
        request.headers = {settings.MFR_ACTION_HEADER: 'bad-action'}
        with pytest.raises(UnsupportedActionError):
            await handler.get('test', 'test', request)
