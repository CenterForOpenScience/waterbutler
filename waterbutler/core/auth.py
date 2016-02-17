import abc


class BaseAuthHandler(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def fetch(self, request_handler):
        # used by v0
        pass

    @abc.abstractmethod
    def get(self, resource, provider, request):
        # used by v1
        pass

