import os

import pytest

from waterbutler.providers.nextcloud import NextcloudProvider
from waterbutler.providers.nextcloud.metadata import (NextcloudFileMetadata,
                                                     NextcloudFolderMetadata,
                                                     NextcloudFileRevisionMetadata)


@pytest.fixture
def file_metadata_object(provider):
    file_attr = {'{DAV:}getcontentlength': '3011',
     '{DAV:}getcontenttype': 'application/octet-stream',
     '{DAV:}getetag': '"a3c411808d58977a9ecd7485b5b7958e"',
     '{DAV:}getlastmodified': 'Sun, 10 Jul 2016 23:28:31 GMT',
     '{DAV:}resourcetype': None,
     '{http://owncloud.org/ns}fileid': '7923'}
    metadata = NextcloudFileMetadata('/Documents/dissertation.aux', '/', provider.NAME, file_attr)
    metadata.extra = {
        'hashes': {
            'md5': 'ee0558f500468642243e29dc914832e9',
            'sha256': 'c9b2543ae9c0a94579fa899dde770af9538d93ce6c58948c86c0a6d8f5d1b014',
            'sha512': '45e0920b6d7850fbaf028a1ee1241154a7641f3ee325efb3fe483d86dba5c170a4b1075d7e7fd2ae0c321def6022f3aa2b59e0c1dc5213bf1c50690f5cf0b688'
        }
    }

    return metadata


@pytest.fixture
def file_metadata_object_less_info(provider):
    file_attr = {'{DAV:}getetag': '"a3c411808d58977a9ecd7485b5b7958e"',
     '{DAV:}getlastmodified': 'Sun, 10 Jul 2016 23:28:31 GMT',}
    metadata = NextcloudFileMetadata('/Documents/dissertation.aux', '/', provider.NAME, file_attr)
    metadata.extra = {
        'hashes': {
            'md5': 'ee0558f500468642243e29dc914832e9',
            'sha256': 'c9b2543ae9c0a94579fa899dde770af9538d93ce6c58948c86c0a6d8f5d1b014',
            'sha512': '45e0920b6d7850fbaf028a1ee1241154a7641f3ee325efb3fe483d86dba5c170a4b1075d7e7fd2ae0c321def6022f3aa2b59e0c1dc5213bf1c50690f5cf0b688'
        }
    }

    return metadata


@pytest.fixture
def file_metadata_object_2(provider):
    file_attr = {'{DAV:}getcontentlength': '1820',
     '{DAV:}getcontenttype': 'text/plain',
     '{DAV:}getetag': '"8acd67d989953d6a02c9e496bb2fe9ff"',
     '{DAV:}getlastmodified': 'Thu, 11 Jun 2020 08:41:29 GMT',
     '{DAV:}resourcetype': None,
     '{http://owncloud.org/ns}fileid': '8512'}

    return NextcloudFileMetadata('/Documents/meeting_memo.txt', '/', provider.NAME, file_attr)


@pytest.fixture
def folder_metadata_object(provider):
    file_attr = {'{DAV:}getetag': '"57688dd3584b0"',
         '{DAV:}getlastmodified': 'Tue, 21 Jun 2016 00:44:03 GMT',
         '{DAV:}quota-available-bytes': '-3',
         '{DAV:}quota-used-bytes': '36227',
         '{DAV:}resourcetype': '\n                    '}

    return NextcloudFolderMetadata('/Documents/', '/my_folder/', provider.NAME, file_attr)


@pytest.fixture
def folder_metadata_object_less_info(provider):
    file_attr = {'{DAV:}getetag': '"a3c411808d58977a9ecd7485b5b7958e"',
     '{DAV:}getlastmodified': 'Sun, 10 Jul 2016 23:28:31 GMT',}

    return NextcloudFolderMetadata('/Documents/', '/my_folder/', provider.NAME, file_attr)


@pytest.fixture
def revision_metadata_object(file_metadata_object):
    return NextcloudFileRevisionMetadata(file_metadata_object.provider,
                                         'a3c411808d58977a9ecd7485b5b7958e',
                                         file_metadata_object)


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
            'host':'https://cat/nextcloud'}


@pytest.fixture
def credentials_2():
    return {'username':'dog',
            'password':'dog',
            'host':'https://dog/nextcloud'}


@pytest.fixture
def credentials_host_with_trailing_slash():
    return {'username':'cat',
            'password':'cat',
            'host':'https://cat/nextcloud/'}


@pytest.fixture
def settings():
    return {'folder': '/my_folder', 'verify_ssl':False}


@pytest.fixture
def provider(auth, credentials, settings):
    return NextcloudProvider(auth, credentials, settings)


@pytest.fixture
def provider_different_credentials(auth, credentials_2, settings):
    return NextcloudProvider(auth, credentials_2, settings)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def moved_parent_folder_metadata():
    with open(os.path.join(os.path.dirname(__file__),
                           'fixtures/moved_parent_folder_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_checksum():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_checksum.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_checksum_2():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_checksum_2.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_checksum_3():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_checksum_3.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_checksum_4():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_checksum_4.xml'), 'r') as fp:
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


@pytest.fixture
def file_metadata_2():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_metadata_2.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_revision_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_revision_metadata.xml'), 'r') as fp:
        return fp.read()


@pytest.fixture
def file_revision_metadata_error_response():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_revision_metadata_error_response.xml'), 'r') as fp:
        return fp.read()
