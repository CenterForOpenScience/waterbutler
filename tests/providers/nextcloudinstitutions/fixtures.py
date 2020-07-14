import pytest

from waterbutler.providers.nextcloudinstitutions import (
    NextcloudInstitutionsProvider
)

@pytest.fixture
def provider(auth, credentials, settings):
    return NextcloudInstitutionsProvider(auth, credentials, settings)


@pytest.fixture
def provider_different_credentials(auth, credentials_2, settings):
    return NextcloudInstitutionsProvider(auth, credentials_2, settings)
