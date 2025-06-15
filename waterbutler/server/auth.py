import logging

from stevedore import driver

from waterbutler.core.auth import AuthType

logger = logging.getLogger(__name__)


class AuthHandler:

    gorbums = None

    def __init__(self, names):
        logger.error(f'@@@@  __init__ names are: {names}')
        self.gorbums = names
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
        raise Exception('no valid credential found')

    async def get(self, resource, provider, request, action=None, auth_type=AuthType.SOURCE,
                  path='', version=None):
        logger.error('@@@@ in get befor extenstions')
        logger.error(f'@@@@    gorbums are {self.gorbums}')
        logger.error(f'@@@@    extensions are {self.manager.extensions}')
        for extension in self.manager.extensions:
            logger.error(f'@@@ checking extension {extension} to see if we can get a cred')
            credential = await extension.obj.get(resource, provider, request,
                                                 action=action, auth_type=auth_type,
                                                 path=path, version=version)
            if credential:
                return credential
            logger.error('@@@     no cred found')

        raise Exception('no valid credential found')
