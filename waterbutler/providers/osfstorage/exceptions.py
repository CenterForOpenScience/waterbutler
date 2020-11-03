from http import HTTPStatus

from waterbutler.core.exceptions import ProviderError


class OsfStorageQuotaExceededError(ProviderError):

    def __init__(self, dummy) -> None:
        """``dummy`` argument is because children of ``WaterButlerError`` must be instantiable with
        a single integer argument.  See :class:`waterbutler.core.exceptions.WaterButlerError` for
        details.
        """

        super().__init__('The quota on this osfstorage project has been exceeded',
                         code=HTTPStatus.INSUFFICIENT_STORAGE)
