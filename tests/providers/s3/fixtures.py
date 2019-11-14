import os
from collections import OrderedDict

import pytest

from waterbutler.providers.s3.metadata import (S3Revision,
                                               S3FileMetadata,
                                               S3FolderMetadata,
                                               S3FolderKeyMetadata,
                                               S3FileMetadataHeaders,
                                               )


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'access_key': 'Dont dead',
        'secret_key': 'open inside',
    }


@pytest.fixture
def settings():
    return {
        'bucket': 'that kerning',
        'encrypt_uploads': False
    }


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def folder_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/folder_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_single_item_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/folder_single_item_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_item_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/folder_item_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_and_contents():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/folder_and_contents.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def version_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/version_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def single_version_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/single_version_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_empty_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/folder_empty_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_header_metadata():
    return {
        'Content-Length': '9001',
        'Last-Modified': 'SomeTime',
        'Content-Type': 'binary/octet-stream',
        'Etag': '"fba9dede5f27731c9771645a39863328"',
        'x-amz-server-side-encryption': 'AES256'
    }


@pytest.fixture
def file_metadata_headers_object(file_header_metadata):
    return S3FileMetadataHeaders('test-path', file_header_metadata)


@pytest.fixture
def file_metadata_object():
    content = OrderedDict(Key='my-image.jpg',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag="fba9dede5f27731c9771645a39863328",
                          Size='434234',
                          StorageClass='STANDARD')

    return S3FileMetadata(content)


@pytest.fixture
def folder_key_metadata_object():
    content = OrderedDict(Key='naptime/',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag='"fba9dede5f27731c9771645a39863328"',
                          Size='0',
                          StorageClass='STANDARD')

    return S3FolderKeyMetadata(content)


@pytest.fixture
def folder_metadata_object():
    content = OrderedDict(Prefix='photos/')
    return S3FolderMetadata(content)


@pytest.fixture
def revision_metadata_object():
    content = OrderedDict(
        Key='single-version.file',
        VersionId='3/L4kqtJl40Nr8X8gdRQBpUMLUo',
        IsLatest='true',
        LastModified='2009-10-12T17:50:30.000Z',
        ETag='"fba9dede5f27731c9771645a39863328"',
        Size=434234,
        StorageClass='STANDARD',
        Owner=OrderedDict(
            ID='75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a',
            DisplayName='mtd@amazon.com'
        )
    )

    return S3Revision(content)


@pytest.fixture
def create_session_resp():
    file_path = 'fixtures/chunked_uploads/create_session_resp.xml'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()


@pytest.fixture
def generic_http_404_resp():
    file_path = 'fixtures/chunked_uploads/generic_http_404_resp.xml'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()


@pytest.fixture
def generic_http_403_resp():
    file_path = 'fixtures/chunked_uploads/generic_http_403_resp.xml'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()


@pytest.fixture
def list_parts_resp_empty():
    file_path = 'fixtures/chunked_uploads/list_parts_resp_empty.xml'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()


@pytest.fixture
def list_parts_resp_not_empty():
    file_path = 'fixtures/chunked_uploads/list_parts_resp_not_empty.xml'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()


@pytest.fixture
def complete_upload_resp():
    file_path = 'fixtures/chunked_uploads/complete_upload_resp.xml'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()


@pytest.fixture
def upload_parts_headers_list():
    file_path = 'fixtures/chunked_uploads/upload_parts_headers_list.json'
    with open(os.path.join(os.path.dirname(__file__), file_path), 'r') as fp:
        return fp.read()
