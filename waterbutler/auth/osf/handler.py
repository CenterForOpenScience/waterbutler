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

        query_params = {
            'payload': jwt.encode({
                'data': bundle,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)
            }, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
        }

        view_only = request.query_arguments.get('view_only')
        if view_only:
            # View only must go outside of the jwt
            query_params['view_only'] = view_only[0].decode()

        response = yield from aiohttp.request(
            'get',
            settings.API_URL,
            params=query_params,
            headers=headers,
            cookies=dict(request.cookies),
        )

        if response.status != 200:
            try:
                data = yield from response.json()
            except ValueError:
                data = yield from response.read()
            raise exceptions.AuthError(data, code=response.status)

        try:
            return jwt.decode((yield from response.json()), settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM, options={'require_exp': True})['data']
        except KeyError:
            raise exceptions.AuthError(data, code=response.status)

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

        query_params = {
            'payload': jwt.encode({
                'data': params,
                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=settings.JWT_EXPIRATION)
            }, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
        }

        view_only = request.query_arguments.get('view_only')
        if view_only:
            # View only must go outside of the jwt
            query_params['view_only'] = view_only[0].decode()

        try:
            response = yield from aiohttp.request(
                'get',
                settings.API_URL,
                params=query_params,
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

        try:
            return jwt.decode((yield from response.json()), settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM, option={'require_exp': True})['data']
        except KeyError:
            raise exceptions.AuthError(data, code=response.status)
