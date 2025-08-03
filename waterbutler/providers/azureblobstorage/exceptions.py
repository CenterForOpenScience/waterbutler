from waterbutler.core import exceptions


class AzureBlobStorageError(exceptions.ProviderError):
    """Base exception for Azure Blob Storage provider."""
    pass


class AzureBlobStorageNotFoundError(AzureBlobStorageError, exceptions.NotFoundError):
    """Exception for when a blob is not found."""
    pass


class AzureBlobStorageUploadError(AzureBlobStorageError, exceptions.UploadError):
    """Exception for upload errors."""
    pass


class AzureBlobStorageDownloadError(AzureBlobStorageError, exceptions.DownloadError):
    """Exception for download errors."""
    pass


class AzureBlobStorageDeleteError(AzureBlobStorageError, exceptions.DeleteError):
    """Exception for delete errors."""
    pass


class AzureBlobStorageMetadataError(AzureBlobStorageError, exceptions.MetadataError):
    """Exception for metadata errors."""
    pass
