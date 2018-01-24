import logging

import pytest

from waterbutler.providers.googlecloud import GoogleCloudProvider

from tests.providers.googlecloud.fixtures import mock_auth, mock_creds, mock_settings

logger = logging.getLogger(__name__)


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture()
def mock_dest_provider(mock_provider):
    mock_provider.bucket = 'gcloud-test-2.longzechen.com'
    return mock_provider


class TestProviderInit:
    """Test that provider initialization set properties correctly.
    """

    async def test_provider_init(self, mock_provider: GoogleCloudProvider):

        assert mock_provider is not None
        assert mock_provider.bucket is not None
        assert mock_provider.access_token is not None
        assert mock_provider.region is not None
        assert mock_provider.can_duplicate_names() is True
