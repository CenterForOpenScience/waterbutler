import json
from http import HTTPStatus

from waterbutler import tasks
from waterbutler.sizes import MBs
from waterbutler.core import exceptions
from waterbutler.server import settings
from waterbutler.core.auth import AuthType
from waterbutler.core import remote_logging
from waterbutler.server.auth import AuthHandler
from waterbutler.core.utils import make_provider
from waterbutler.constants import DEFAULT_CONFLICT

auth_handler = AuthHandler(settings.AUTH_HANDLERS)


class MoveCopyMixin:

    @property
    def json(self):
        if not hasattr(self, '_json'):
            try:
                # self.body is defined by self.data_received
                self._json = json.loads(self.body.decode())
            except ValueError:
                raise exceptions.InvalidParameters('Invalid json body')
        return self._json

    def prevalidate_post(self):
        """Validate body and query parameters before spending API calls on validating path.  We
        don't trust path yet, so I don't wanna see it being used here.  Current validations:

        1. Max body size is 1Mb.
        2. Content-Length header must be provided.
        """
        try:
            if int(self.request.headers['Content-Length']) > 1 * MBs:
                # There should be no JSON body > 1 megs
                raise exceptions.InvalidParameters('Request body must be under 1Mb', code=413)
        except (KeyError, ValueError):
            raise exceptions.InvalidParameters('Content-Length is required', code=411)

    def build_args(self):
        return ({
            'nid': self.resource,  # TODO rename to anything but nid
            'path': self.path,
            'provider': self.provider.serialized()
        }, {
            'nid': self.dest_resource,
            'path': self.dest_path,
            'provider': self.dest_provider.serialized()
        })

    async def move_or_copy(self):
        """Copy, move, and rename files and folders.

        **Auth actions**: ``copy``, ``move``, or ``rename``

        **Provider actions**: ``copy`` or ``move``

        *Auth actions* come from the ``action`` body parameter in the request and are used by the
        auth handler.

        *Provider actions* are determined from the *auth action*.  A "rename" is a special case of
        the "move" provider action that implies that the destination resource, provider, and parent
        path will all be the same as the source.
        """

        auth_action = self.json.get('action', 'null')
        if auth_action not in ('copy', 'move', 'rename'):
            raise exceptions.InvalidParameters('Auth action must be "copy", "move", or "rename", '
                                               'not "{}"'.format(auth_action))

        # Provider setup is delayed so the provider action can be updated from the auth action.
        provider = self.path_kwargs.get('provider', '')
        provider_action = auth_action
        if auth_action == 'rename':
            if not self.json.get('rename', ''):
                raise exceptions.InvalidParameters('"rename" field is required for renaming')
            provider_action = 'move'

        self.auth = await auth_handler.get(
            self.resource,
            provider,
            self.request,
            action=auth_action,
            auth_type=AuthType.SOURCE,
            path=self.path,
            version=self.requested_version,
        )
        self.provider = make_provider(
            provider,
            self.auth['auth'],
            self.auth['credentials'],
            self.auth['settings']
        )
        self.path = await self.provider.validate_v1_path(self.path, **self.arguments)

        if auth_action == 'rename':  # 'rename' implies the file/folder does not change location
            self.dest_auth = self.auth
            self.dest_provider = self.provider
            self.dest_path = self.path.parent
            self.dest_resource = self.resource
        else:
            path = self.json.get('path', None)
            if path is None:
                raise exceptions.InvalidParameters('"path" field is required for moves or copies')
            if not path.endswith('/'):
                raise exceptions.InvalidParameters(
                    '"path" field requires a trailing slash to indicate it is a folder'
                )

            # TODO optimize for same provider and resource

            # for copy action, `auth_action` is the same as `provider_action`
            if auth_action == 'copy' and self.path.is_root and not self.json.get('rename'):
                raise exceptions.InvalidParameters('"rename" field is required for copying root')

            # Note: attached to self so that _send_hook has access to these
            self.dest_resource = self.json.get('resource', self.resource)
            self.dest_auth = await auth_handler.get(
                self.dest_resource,
                self.json.get('provider', self.provider.NAME),
                self.request,
                action=auth_action,
                path=path,
                auth_type=AuthType.DESTINATION,
            )
            self.dest_provider = make_provider(
                self.json.get('provider', self.provider.NAME),
                self.dest_auth['auth'],
                self.dest_auth['credentials'],
                self.dest_auth['settings']
            )
            self.dest_path = await self.dest_provider.validate_path(**self.json)

        if not getattr(self.provider, 'can_intra_' + provider_action)(self.dest_provider, self.path):
            # this weird signature syntax courtesy of py3.4 not liking trailing commas on kwargs
            conflict = self.json.get('conflict', DEFAULT_CONFLICT)
            result = await getattr(tasks, provider_action).adelay(
                rename=self.json.get('rename'),
                conflict=conflict,
                request=remote_logging._serialize_request(self.request),
                *self.build_args()
            )
            metadata, created = await tasks.wait_on_celery(result)
        else:
            metadata, created = (
                await tasks.backgrounded(
                    getattr(self.provider, provider_action),
                    self.dest_provider,
                    self.path,
                    self.dest_path,
                    rename=self.json.get('rename'),
                    conflict=self.json.get('conflict', DEFAULT_CONFLICT),
                )
            )

        self.dest_meta = metadata

        if created:
            self.set_status(int(HTTPStatus.CREATED))
        else:
            self.set_status(int(HTTPStatus.OK))

        self.write({'data': metadata.json_api_serialized(self.dest_resource)})
