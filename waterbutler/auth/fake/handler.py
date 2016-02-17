
from waterbutler.core import auth
from waterbutler.core import exceptions

from waterbutler.auth.fake import settings

class FakeAuthHandler(auth.BaseAuthHandler):

    async def fetch(self, resource, provider, request):
        pass

    async def get(self, resource, provider, request):
        # part of osf website.addons.base.views.get_auth
        #auth = {
        #    'id': user._id,
        #    'email': '{}@osf.io'.format(user._id),
        #    'name': user.fullname,
        #}
        # just github for now

        try:
            credentials = settings.PROVIDERS[provider]['credentials']
            provider_settings = settings.PROVIDERS[provider]['settings']
        except KeyError:
            raise exceptions.AuthError('Configure your fakes!', code=503)

        return {
            'auth': {},
            'credentials': credentials,
            'settings': provider_settings,
            'callback_url': ''
        }

