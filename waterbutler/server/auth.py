from stevedore import driver

from waterbutler.core.auth import AuthType


class AuthHandler:

    def __init__(self, names):
        self.manager = driver.NamedExtensionManager(
            namespace='waterbutler.auth',
            names=names,
            invoke_on_load=True,
            invoke_args=(),
            name_order=True,
        )

    async def fetch(self, request, bundle):
        for extension in self.manager.extensions:
            credential = await extension.obj.fetch(request, bundle)
            if credential:
                return credential
        raise AuthHandler('no valid credential found')

    async def get(self, resource, provider, request, action=None, auth_type=AuthType.SOURCE,
                  path='', version=None):
        for extension in self.manager.extensions:
            credential = await extension.obj.get(resource, provider, request,
                                                 action=action, auth_type=auth_type,
                                                 path=path, version=version)
            if credential:
                return credential
        raise AuthHandler('no valid credential found')
