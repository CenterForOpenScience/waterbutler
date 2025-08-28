"""
Azure Blob Storage test fixtures that load from JSON and XML files
"""

import pytest
import io
import json
import os
from pathlib import Path
from waterbutler.core import streams
from waterbutler.providers.azureblobstorage.provider import AzureBlobStorageProvider


# Get the fixtures directory path
FIXTURES_DIR = Path(__file__).parent / 'fixtures'


def load_fixture(filename):
    """Load a fixture file from the fixtures directory"""
    filepath = FIXTURES_DIR / filename
    if filename.endswith('.xml'):
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)


# ============== Authentication Fixtures (JSON) ==============

@pytest.fixture
def auth():
    """Auth information from the user"""
    return load_fixture('auth.json')


@pytest.fixture
def credentials():
    """OAuth credentials from Azure Entra ID"""
    return load_fixture('credentials.json')


@pytest.fixture
def settings():
    """Provider settings for Azure Blob Storage"""
    return load_fixture('settings.json')


# ============== Response Fixtures (Mixed JSON/XML) ==============

@pytest.fixture
def blob_properties_headers():
    """Standard blob properties headers (JSON format for headers)"""
    return load_fixture('blob_properties_headers.json')


# XML Response Fixtures - Direct from Azure API format
@pytest.fixture
def blob_list_xml():
    """Standard blob list XML response"""
    return load_fixture('blob_list_response.xml')


@pytest.fixture
def blob_list_with_versions_response():
    """Blob list with version information XML response"""
    return load_fixture('blob_list_with_versions.xml')


@pytest.fixture
def empty_list_xml():
    """Empty folder list XML response"""
    return load_fixture('empty_list_response.xml')


@pytest.fixture
def root_list_xml():
    """Root container list XML response"""
    return load_fixture('root_list_response.xml')


@pytest.fixture
def special_characters_blobs():
    """Blobs with special characters in names XML response"""
    return load_fixture('special_characters_blobs.xml')


# Error Response Fixtures
@pytest.fixture
def error_authentication_failed_xml():
    """Authentication failed error XML response"""
    return load_fixture('error_authentication_failed.xml')


@pytest.fixture
def error_authorization_failure_xml():
    """Authorization failure error XML response"""
    return load_fixture('error_authorization_failure.xml')


@pytest.fixture
def error_not_found_xml():
    """Blob not found error XML response"""
    return load_fixture('error_not_found.xml')


@pytest.fixture
def error_internal_error_xml():
    """Internal server error XML response"""
    return load_fixture('error_internal_error.xml')


@pytest.fixture
def error_response_xml():
    """Error response XML generator"""
    def _error_xml(error_type):
        try:
            return load_fixture(f'error_{error_type}.xml')
        except FileNotFoundError:
            # Default error XML
            return '''<?xml version="1.0" encoding="utf-8"?>
            <Error>
                <Code>UnknownError</Code>
                <Message>Unknown error occurred</Message>
            </Error>'''
    
    return _error_xml


# ============== Provider Fixture ==============

@pytest.fixture
def provider(auth, credentials, settings):
    return AzureBlobStorageProvider(auth, credentials, settings)


# ============== Stream Fixtures ==============

@pytest.fixture
def file_content():
    """Basic file content for testing"""
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def large_file_content():
    """Large file content for multipart upload testing"""
    # 10 MB of data
    return b'x' * (10 * 1024 * 1024)


@pytest.fixture
def file_like(file_content):
    """File-like object"""
    return io.BytesIO(file_content)


@pytest.fixture
def large_file_like(large_file_content):
    """Large file-like object"""
    return io.BytesIO(large_file_content)


@pytest.fixture
def file_stream(file_like):
    """File stream for upload testing"""
    return streams.FileStreamReader(file_like)


@pytest.fixture
def large_file_stream(large_file_like):
    """Large file stream for multipart upload testing"""
    return streams.FileStreamReader(large_file_like)


# ============== Folder Creation Fixtures ==============

@pytest.fixture
def empty_folder_list_xml():
    """Empty folder list response for checking if folder exists"""
    return load_fixture('empty_folder_check.xml')


@pytest.fixture
def folder_validation_response_xml():
    """XML response for folder validation (folder exists)"""
    return load_fixture('folder_validation_response.xml')


@pytest.fixture
def folder_not_found_response_xml():
    """XML response for folder validation (folder does not exist)"""
    return load_fixture('folder_not_found_response.xml')


@pytest.fixture
def folder_exists_xml():
    """XML response indicating folder already has content"""
    return load_fixture('folder_exists.xml')


@pytest.fixture
def folder_placeholder_headers():
    """Standard headers for folder placeholder creation"""
    return load_fixture('folder_placeholder_headers.json')


@pytest.fixture
def create_folder_test_data():
    """Test data for various folder creation scenarios"""
    return {
        'simple_folder': {
            'path': '/newfolder/',
            'name': 'newfolder',
            'placeholder': 'newfolder/.osfkeep'
        },
        'nested_folder': {
            'path': '/parent/child/',
            'name': 'child', 
            'placeholder': 'parent/child/.osfkeep'
        },
        'special_chars_folder': {
            'path': '/folder with spaces/',
            'name': 'folder with spaces',
            'placeholder': 'folder%20with%20spaces/.osfkeep'
        }
    }


# ============== Helper Functions ==============

@pytest.fixture
def build_error_response():
    """Build a custom error response for testing"""
    def _builder(code, message, status=400, auth_detail=None):
        xml_body = f'''<?xml version="1.0" encoding="utf-8"?>
        <Error>
            <Code>{code}</Code>
            <Message>{message}</Message>
            {f"<AuthenticationErrorDetail>{auth_detail}</AuthenticationErrorDetail>" if auth_detail else ""}
        </Error>'''

        return {
            'status': status,
            'body': xml_body,
            'headers': {'Content-Type': 'application/xml'}
        }

    return _builder