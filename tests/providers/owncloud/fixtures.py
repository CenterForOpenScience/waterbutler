import os
import pytest

from waterbutler.providers.owncloud import OwnCloudProvider

from waterbutler.providers.owncloud.metadata import (
    OwnCloudFileMetadata,
    OwnCloudFolderMetadata,
    OwnCloudFileRevisionMetadata
)


@pytest.fixture
def file_metadata_object():
    return OwnCloudFileMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
            '{DAV:}getcontentlength':3011,
            '{DAV:}getcontenttype': 'test-type'})


@pytest.fixture
def file_metadata_object_less_info():
    return OwnCloudFileMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT'})


@pytest.fixture
def folder_metadata_object():
    return OwnCloudFolderMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
            '{DAV:}getcontentlength':3011,
            '{DAV:}getcontenttype': 'test-type'})


@pytest.fixture
def folder_metadata_object_less_info():
    return OwnCloudFolderMetadata('dissertation.aux','/owncloud/remote.php/webdav/Documents/phile',
            {'{DAV:}getetag':'&quot;a3c411808d58977a9ecd7485b5b7958e&quot;',
            '{DAV:}getlastmodified':'Sun, 10 Jul 2016 23:28:31 GMT',
            '{DAV:}getcontentlength':3011})


@pytest.fixture
def revision_metadata_object(file_metadata_object):
    return OwnCloudFileRevisionMetadata(file_metadata_object.modified)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'username':'cat',
            'password':'cat',
            'host':'https://cat/owncloud'}


@pytest.fixture
def credentials_2():
    return {'username':'dog',
            'password':'dog',
            'host':'https://dog/owncloud'}


@pytest.fixture
def credentials_host_with_trailing_slash():
    return {'username':'cat',
            'password':'cat',
            'host':'https://cat/owncloud/'}


@pytest.fixture
def settings():
    return {'folder': '/my_folder', 'verify_ssl':False}


@pytest.fixture
def provider(auth, credentials, settings):
    return OwnCloudProvider(auth, credentials, settings)


@pytest.fixture
def provider_different_credentials(auth, credentials_2, settings):
    return OwnCloudProvider(auth, credentials_2, settings)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def moved_parent_folder_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/moved_parent_folder_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def moved_folder_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/moved_folder_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_list():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/folder_list.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/folder_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def folder_contents_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/folder_contents_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_metadata_unparsable_response():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/file_metadata_unparsable_response.xml'), 'r') as fp:
        return fp.read()

