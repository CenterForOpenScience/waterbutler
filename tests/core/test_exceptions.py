import pytest

from waterbutler.core import exceptions


class TestExceptionSerialization:

    @pytest.mark.parametrize('exception_class', [
        exceptions.WaterButlerError,
        exceptions.InvalidParameters,
        exceptions.UnsupportedHTTPMethodError,
        exceptions.UnsupportedActionError,
        exceptions.PluginError,
        exceptions.AuthError,
        exceptions.ProviderError,
        exceptions.UnhandledProviderError,
        exceptions.CopyError,
        exceptions.CreateFolderError,
        exceptions.DeleteError,
        exceptions.DownloadError,
        exceptions.IntraCopyError,
        exceptions.IntraMoveError,
        exceptions.MoveError,
        exceptions.MetadataError,
        exceptions.RevisionsError,
        exceptions.UploadError,
        exceptions.RetryChunkedUploadCommit,
        exceptions.FolderNamingConflict,
        exceptions.NamingConflict,
        exceptions.ProviderNotFound,
        exceptions.UploadChecksumMismatchError,
        exceptions.UploadFailedError,
        exceptions.NotFoundError,
        exceptions.InvalidPathError,
        exceptions.OverwriteSelfError,
        exceptions.UnsupportedOperationError,
        exceptions.ReadOnlyProviderError,
        exceptions.UninitializedRepositoryError,
        exceptions.UnexportableFileTypeError,
        exceptions.InvalidProviderConfigError,
        exceptions.WaterButlerRedisError,
        exceptions.TooManyRequests,
    ])
    def test_tolerate_dumb_signature(self, exception_class):
        """In order for WaterButlerError-inheriting exception classes to survive
        pickling/unpickling, it is necessary for them to be able to be instatiated with
         a single integer arg.  The reasons for this are described in the docstring for
        `waterbutler.core.exceptions.WaterButlerError`.
        """
        try:
            i_live_but_why = exception_class(616)
        except Exception as exc:
            pytest.fail(str(exc))

        assert isinstance(i_live_but_why, exception_class)
