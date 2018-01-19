import logging

import pytest

from waterbutler.providers.googlecloudstorage import GoogleCloudStorageProvider

from tests.providers.googlecloudstorage.fixtures import mock_auth, mock_credentials, mock_settings

logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_provider(mock_auth, mock_credentials, mock_settings):
    return GoogleCloudStorageProvider(mock_auth, mock_credentials, mock_settings)


@pytest.fixture()
def mock_dest_provider(mock_provider):
    mock_provider.bucket = 'gcloud-test-2.longzechen.com'
    return mock_provider


class TestProviderInit:
    """Test that provider initialization set properties correctly.
    """

    async def test_provider_init(self, mock_provider: GoogleCloudStorageProvider):

        assert mock_provider is not None
        assert mock_provider.bucket is not None
        assert mock_provider.access_token is not None
        assert mock_provider.region is not None
        assert mock_provider.can_duplicate_names() is True
