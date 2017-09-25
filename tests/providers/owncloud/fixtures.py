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
def moved_parent_folder_metadata():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
    <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
       <d:response>
          <d:href>/owncloud/remote.php/webdav/parent_folder/</d:href>
          <d:propstat>
             <d:prop>
                <d:getlastmodified>Thu, 14 Sep 2017 19:13:44 GMT</d:getlastmodified>
                <d:resourcetype>
                   <d:collection />
                </d:resourcetype>
                <d:quota-used-bytes>1</d:quota-used-bytes>
                <d:quota-available-bytes>-3</d:quota-available-bytes>
                <d:getetag>&amp;quot;59bad4e9c26e7&amp;quot;</d:getetag>
             </d:prop>
             <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
       </d:response>
       <d:response>
          <d:href>/owncloud/remote.php/webdav/parent_folder/moved_folder/</d:href>
          <d:propstat>
             <d:prop>
                <d:getlastmodified>Thu, 14 Sep 2017 19:12:45 GMT</d:getlastmodified>
                <d:resourcetype>
                   <d:collection />
                </d:resourcetype>
                <d:quota-used-bytes>1</d:quota-used-bytes>
                <d:quota-available-bytes>-3</d:quota-available-bytes>
                <d:getetag>&amp;quot;59bad4ad640eb&amp;quot;</d:getetag>
             </d:prop>
             <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
       </d:response>
    </d:multistatus>'''


@pytest.fixture
def moved_folder_metadata():
    return b'''<?xml version="1.0" ?>
    <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
       <d:response>
          <d:href>/owncloud/remote.php/webdav/moved_folder/</d:href>
          <d:propstat>
             <d:prop>
                <d:getlastmodified>Thu, 14 Sep 2017 19:12:45 GMT</d:getlastmodified>
                <d:resourcetype>
                   <d:collection />
                </d:resourcetype>
                <d:quota-used-bytes>1</d:quota-used-bytes>
                <d:quota-available-bytes>-3</d:quota-available-bytes>
                <d:getetag>&amp;quot;59bad4ad640eb&amp;quot;</d:getetag>
             </d:prop>
             <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
       </d:response>
       <d:response>
          <d:href>/owncloud/remote.php/webdav/moved_folder/child_file</d:href>
          <d:propstat>
             <d:prop>
                <d:getlastmodified>Thu, 14 Sep 2017 19:12:45 GMT</d:getlastmodified>
                <d:getcontentlength>1</d:getcontentlength>
                <d:resourcetype />
                <d:getetag>&amp;quot;5cedcd73801ef88766bc32ce712bb1f5&amp;quot;</d:getetag>
                <d:getcontenttype>application/octet-stream</d:getcontenttype>
             </d:prop>
             <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
       </d:response>
    </d:multistatus>
'''


@pytest.fixture
def folder_list():
    return b'''<?xml version="1.0" ?>
    <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
        <d:response>
            <d:href>/owncloud/remote.php/webdav/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Tue, 21 Jun 2016 00:44:03 GMT</d:getlastmodified>
                    <d:resourcetype>
                         <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>714783</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;57688dd358fb7&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/owncloud/remote.php/webdav/Documents/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Tue, 21 Jun 2016 00:44:03 GMT</d:getlastmodified>
                    <d:resourcetype>
                        <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>36227</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;57688dd3584b0&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
        <d:response>
            <d:href>/owncloud/remote.php/webdav/Photos/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Wed, 15 Jun 2016 22:49:40 GMT</d:getlastmodified>
                    <d:resourcetype>
                        <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>678556</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;5761db8485325&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
     </d:multistatus>'''


@pytest.fixture
def folder_metadata():
    return b'''<?xml version="1.0" ?>
    <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
        <d:response>
            <d:href>/owncloud/remote.php/webdav/Documents/</d:href>
            <d:propstat>
                <d:prop>
                    <d:getlastmodified>Tue, 21 Jun 2016 00:44:03 GMT</d:getlastmodified>
                    <d:resourcetype>
                        <d:collection/>
                    </d:resourcetype>
                    <d:quota-used-bytes>36227</d:quota-used-bytes>
                    <d:quota-available-bytes>-3</d:quota-available-bytes>
                    <d:getetag>&quot;57688dd3584b0&quot;</d:getetag>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
     </d:multistatus>'''


@pytest.fixture
def folder_contents_metadata():
    return b'''<?xml version="1.0"?>
      <d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:s="http://sabredav.org/ns">
        <d:response>
          <d:href>/remote.php/webdav/Documents/</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Thu, 01 Jun 2017 15:53:13 GMT</d:getlastmodified>
              <d:resourcetype>
                <d:collection/>
              </d:resourcetype>
              <d:quota-used-bytes>36227</d:quota-used-bytes>
              <d:quota-available-bytes>-3</d:quota-available-bytes>
              <d:getetag>&quot;5930386978ae6&quot;</d:getetag>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
        <d:response>
          <d:href>/remote.php/webdav/Documents/Example.odt</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Fri, 12 May 2017 20:37:35 GMT</d:getlastmodified>
              <d:getcontentlength>36227</d:getcontentlength>
              <d:resourcetype/>
              <d:getetag>&quot;95db455e6e33d57d521c0d4e93496747&quot;</d:getetag>
              <d:getcontenttype>application/vnd.oasis.opendocument.text</d:getcontenttype>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
        <d:response>
          <d:href>/remote.php/webdav/Documents/pumpkin/</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Thu, 01 Jun 2017 15:28:10 GMT</d:getlastmodified>
              <d:resourcetype>
                <d:collection/>
              </d:resourcetype>
              <d:quota-used-bytes>0</d:quota-used-bytes>
              <d:quota-available-bytes>-3</d:quota-available-bytes>
              <d:getetag>&quot;5930328a6f9da&quot;</d:getetag>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
        <d:response>
          <d:href>/remote.php/webdav/Documents/squash/</d:href>
          <d:propstat>
            <d:prop>
              <d:getlastmodified>Thu, 01 Jun 2017 15:53:13 GMT</d:getlastmodified>
              <d:resourcetype>
                <d:collection/>
              </d:resourcetype>
              <d:quota-used-bytes>0</d:quota-used-bytes>
              <d:quota-available-bytes>-3</d:quota-available-bytes>
              <d:getetag>&quot;59303869582b4&quot;</d:getetag>
            </d:prop>
            <d:status>HTTP/1.1 200 OK</d:status>
          </d:propstat>
        </d:response>
      </d:multistatus>'''


@pytest.fixture
def file_metadata():
    return b'''<?xml version="1.0"?>
            <d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns">
                <d:response>
                    <d:href>/owncloud/remote.php/webdav/Documents/dissertation.aux</d:href>
                    <d:propstat>
                        <d:prop>
                            <d:getlastmodified>Sun, 10 Jul 2016 23:28:31 GMT</d:getlastmodified>
                            <d:getcontentlength>3011</d:getcontentlength>
                            <d:resourcetype/>
                            <d:getetag>&quot;a3c411808d58977a9ecd7485b5b7958e&quot;</d:getetag>
                            <d:getcontenttype>application/octet-stream</d:getcontenttype>
                        </d:prop>
                        <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                </d:response>
            </d:multistatus>'''


@pytest.fixture
def file_metadata_unparsable_response():
    return b'''<?xml version="1.0"?>
            <d:multistatus xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns">
                <d:response>
                    <d:propstat>
                        <d:prop>
                            <d:getlastmodified>Sun, 10 Jul 2016 23:28:31 GMT</d:getlastmodified>
                            <d:getcontentlength>3011</d:getcontentlength>
                            <d:resourcetype/>
                            <d:getetag>&quot;a3c411808d58977a9ecd7485b5b7958e&quot;</d:getetag>
                            <d:getcontenttype>application/octet-stream</d:getcontenttype>
                        </d:prop>
                        <d:status>HTTP/1.1 200 OK</d:status>
                    </d:propstat>
                </d:response>
            </d:multistatus>'''


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'

