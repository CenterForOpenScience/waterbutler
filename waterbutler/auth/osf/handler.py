import datetime

import jwe
import jwt
import json
from tornado.httpclient import (
    AsyncHTTPClient,
    HTTPRequest
)
from tornado.httputil import url_concat

from waterbutler.core import exceptions
from waterbutler.auth.osf import settings
from waterbutler.core.auth import (
    BaseAuthHandler,
    AuthType
)


JWE_KEY = jwe.kdf(settings.JWE_SECRET.encode(), settings.JWE_SALT.encode())


class OsfAuthHandler(BaseAuthHandler):
    """Identity lookup via the Open Science Framework"""
    ACTION_MAP = {
        'put': 'upload',
        'get': 'download',
        'head': 'metadata',
        'delete': 'delete',
    }

    @property
    def client(self):
        return AsyncHTTPClient()

    def build_payload(self, request, resource=None, provider=None, action=None, auth_type=None):
        bundle = {
            'nid': resource,
            'provider': provider,
            'action': self.get_action(
                request=request,
                action=action,
                auth_type=auth_type
            )
        }

        try:
            bundle['cookie'] = request.query_arguments.get('cookie')[0].decode()
        except:
            pass

        query_params = {
            'payload': jwe.encrypt(jwt.encode({
                'data': bundle,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)
            }, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM), JWE_KEY)
        }

        try:
            # View only must go outside of the jwt
            query_params['view_only'] = request.query_arguments.get('view_only')[0].decode()
        except:
            pass

        return query_params

    def parse_response(self, response):
        """Decode and extract auth, credentials and settings from the OSF's
        response
        """
        try:
            data = jwt.decode(
                jwe.decrypt(response['payload'].encode(), JWE_KEY),
                settings.JWT_SECRET,
                algorithm=settings.JWT_ALGORITHM,
                options={'require_exp': True}
            )
            payload = data['data']
        except (jwt.InvalidTokenError, KeyError):
            raise exceptions.AuthError(data, code=response.status)

        payload['auth']['callback_url'] = payload['callback_url']

        return payload

    async def fetch(self, request, bundle):
        """Used for v0"""
        headers = {'Content-Type': 'application/json'}

        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']

        cookie = request.query_arguments.get('cookie')
        if cookie:
            cookie = cookie[0].decode()

        view_only = request.query_arguments.get('view_only')
        if view_only:
            view_only = view_only[0].decode()

        payload = (await self.make_request(
            self.build_payload(bundle, cookie=cookie, view_only=view_only),
            headers,
            dict(request.cookies)
        ))

        payload['auth']['callback_url'] = payload['callback_url']
        return payload

    def get_action(self, request=None, action=None, auth_type=None):
        """Returns the action the request will perform. This is needed to
        determine the permission that wb need to check with the osf to see if
        the client posesses.
        """
        method = request.method.lower()
        if method is None:
            raise exceptions.AuthError('Unable to check permissions.', code=400)

        if method == 'post' and action:
            post_action_map = {
                'copy': 'download' if auth_type is AuthType.SOURCE else 'upload',
                'rename': 'upload',
                'move': 'upload',
            }

            try:
                return post_action_map[action.lower()]
            except KeyError:
                raise exceptions.UnsupportedActionError(method, supported=post_action_map.keys())

        elif method == 'head' and settings.MFR_ACTION_HEADER in request.headers:
            mfr_action_map = {
                'render': 'render',
                'export': 'export'
            }
            mfr_action = request.headers[settings.MFR_ACTION_HEADER].lower()
            try:
                return mfr_action_map[mfr_action]
            except KeyError:
                raise exceptions.UnsupportedActionError(mfr_action, supported=mfr_action_map.keys())

        else:
            try:
                return self.ACTION_MAP[method]
            except KeyError:
                raise exceptions.UnsupportedHTTPMethodError(method, supported=self.ACTION_MAP.keys())

    def build_auth_headers(self, request):
        """Build the headers to send to the OSF for the auth request
        """
        import pdb
        pdb.set_trace()
        headers = {
            'Content-Type': 'application/json',
            'Cookie': dict(request.cookies)
        }
        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']
        return headers

    async def get(self, resource, provider, request, action=None, auth_type=AuthType.SOURCE):
        """Used for v1"""

        try:
            import pdb
            pdb.set_trace()

            auth_request = HTTPRequest(
                url_concat(
                    settings.API_URL,
                    self.build_params(
                        request,
                        resource=resource,
                        provider=provider,
                        action=action,
                        auth_type=auth_type
                    )
                ),
                method='GET',
                headers=self.build_auth_headers(request),
            )

            response = await self.client.fetch(auth_request)
            # response = await aiohttp.request(
            #     'GET',
            #     settings.API_URL,
            #     params=params,
            #     headers=self.build_auth_headers(request),
            #     cookies=cookies
            # )

        except:
            pass
        # except aiohttp.errors.ClientError:
        #     raise exceptions.AuthError('Unable to connect to auth sever', code=503)

        if response.status != 200:
            try:
                data = json.loads(response.body)
            except ValueError:
                data = response.body
            raise exceptions.AuthError(data, code=response.status)

        return self.parse_response(json.loads(response.body))
