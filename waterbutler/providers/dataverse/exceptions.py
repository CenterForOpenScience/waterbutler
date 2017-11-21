from http import HTTPStatus

from waterbutler.core.exceptions import UploadError


class DataverseIngestionLockError(UploadError):
    def __init__(self, message, code=HTTPStatus.BAD_REQUEST):
        """``dummy`` argument is because children of ``WaterButlerError`` must be instantiable with
        a single integer argument.  See :class:`waterbutler.core.exceptions.WaterButlerError`
        for details.
        """
        super().__init__(
            'Some uploads to Dataverse will lock uploading for a time. Please wait'
            ' a few seconds and try again.',
            code=code)
