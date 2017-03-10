import datetime

import jwe
import jwt
import aiohttp

from waterbutler.core import auth
from waterbutler.core import exceptions

from waterbutler.auth.osf import settings


JWE_KEY = jwe.kdf(settings.JWE_SECRET.encode(), settings.JWE_SALT.encode())


class OsfAuthHandler(auth.BaseAuthHandler):
    """Identity lookup via the Open Science Framework"""
    ACTION_MAP = {
        'put': 'upload',
        'post': 'upload',  # TODO copyfrom
        'get': 'download',
        'head': 'metadata',
        'delete': 'delete',
    }

    def build_payload(self, bundle, view_only=None, cookie=None):
        query_params = {}

        if cookie:
            bundle['cookie'] = cookie

        if view_only:
            # View only must go outside of the jwt
            query_params['view_only'] = view_only

        query_params['payload'] = jwe.encrypt(jwt.encode({
            'data': bundle,
            'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)
        }, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM), JWE_KEY)

        return query_params

    async def make_request(self, params, headers, cookies):
        try:
            response = await aiohttp.request(
                'get',
                settings.API_URL,
                params=params,
                headers=headers,
                cookies=cookies,
            )
        except aiohttp.errors.ClientError:
            raise exceptions.AuthError('Unable to connect to auth sever', code=503)

        if response.status != 200:
            try:
                data = await response.json()
            except ValueError:
                data = await response.read()
            raise exceptions.AuthError(data, code=response.status)

        try:
            raw = await response.json()
            signed_jwt = jwe.decrypt(raw['payload'].encode(), JWE_KEY)
            data = jwt.decode(signed_jwt, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM, options={'require_exp': True})
            return data['data']
        except (jwt.InvalidTokenError, KeyError):
            raise exceptions.AuthError(data, code=response.status)

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

    async def get(self, resource, provider, request):
        """Used for v1"""

        try:
            action = self.ACTION_MAP[request.method.lower()]
        except KeyError:
            raise exceptions.UnsupportedHTTPMethodError(
                method_used=request.method.lower(),
                supported_methods=self.ACTION_MAP.keys()
            )

        headers = {'Content-Type': 'application/json'}

        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']

        cookie = request.query_arguments.get('cookie')
        if cookie:
            cookie = cookie[0].decode()

        view_only = request.query_arguments.get('view_only')
        if view_only:
            # View only must go outside of the jwt
            view_only = view_only[0].decode()

        payload = (await self.make_request(
            self.build_payload({
                'nid': resource,
                'provider': provider,
                'action': action
            }, cookie=cookie, view_only=view_only),
            headers,
            dict(request.cookies)
        ))

        payload['auth']['callback_url'] = payload['callback_url']
        return payload
