import asyncio
import datetime

import jwt
import aiohttp

from waterbutler.core import auth
from waterbutler.core import exceptions

from waterbutler.auth.osf import settings


class OsfAuthHandler(auth.BaseAuthHandler):
    """Identity lookup via the Open Science Framework"""
    ACTION_MAP = {
        'put': 'upload',
        'post': 'upload',  # TODO copyfrom
        'get': 'download',
        'head': 'metadata',
        'delete': 'delete',
    }

    @asyncio.coroutine
    def fetch(self, request, bundle):
        """Used for v0"""
        headers = {'Content-Type': 'application/json'}

        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']

        cookie = request.query_arguments.get('cookie')
        if cookie:
            bundle['cookie'] = cookie[0].decode()

        view_only = request.query_arguments.get('view_only')
        if view_only:
            bundle['view_only'] = view_only[0].decode()

        bundle['exp'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)

        response = yield from aiohttp.request(
            'get',
            settings.API_URL,
            params={'payload': jwt.encode(bundle, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)},
            headers=headers,
            cookies=dict(request.cookies),
        )

        if response.status != 200:
            try:
                data = yield from response.json()
            except ValueError:
                data = yield from response.read()
            raise exceptions.AuthError(data, code=response.status)

        return jwt.decode((yield from response.json()), settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

    @asyncio.coroutine
    def get(self, resource, provider, request):
        """Used for v1"""
        headers = {'Content-Type': 'application/json'}

        if 'Authorization' in request.headers:
            headers['Authorization'] = request.headers['Authorization']

        params = {
            'nid': resource,
            'provider': provider,
            'action': self.ACTION_MAP[request.method.lower()]
        }

        cookie = request.query_arguments.get('cookie')
        if cookie:
            params['cookie'] = cookie[0].decode()

        view_only = request.query_arguments.get('view_only')
        if view_only:
            params['view_only'] = view_only[0].decode()

        params['exp'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)

        try:
            response = yield from aiohttp.request(
                'get',
                settings.API_URL,
                params={'payload': jwt.encode(params, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)},
                headers=headers,
                cookies=dict(request.cookies),
            )
        except aiohttp.errors.ClientError:
            raise exceptions.AuthError('Unable to connect to auth sever', code=503)

        if response.status != 200:
            try:
                data = yield from response.json()
            except ValueError:
                data = yield from response.read()
            raise exceptions.AuthError(data, code=response.status)

        return jwt.decode((yield from response.json()), settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
