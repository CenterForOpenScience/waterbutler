import abc


class BaseAuthHandler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def fetch(self, request, bundle):
        pass

    @abc.abstractmethod
    async def get(self, resource, provider, request, action=None, is_source=True):
        pass
