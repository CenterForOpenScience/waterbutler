import abc
from enum import Enum


class AuthType(Enum):
    SOURCE = 0
    DESTINATION = 1


class BaseAuthHandler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def fetch(self, request, bundle):
        pass

    @abc.abstractmethod
    async def get(self, resource, provider, request, action=None, auth_type=AuthType.SOURCE,
                  path='', version=None):
        pass
