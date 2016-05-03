import pytest

from tests.utils import async

import io
from http import client

import json
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core import metadata
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.fedora import FedoraProvider
from waterbutler.providers.fedora.metadata import FedoraFileMetadata
from waterbutler.providers.fedora.metadata import FedoraFolderMetadata

@pytest.fixture
def auth():
    return {
        'name': 'Calvin',
        'email': 'bill.watterson@example.com',
    }


@pytest.fixture
def credentials():
    return {
        'repo': 'http://localhost:8080/rest/',
        'user': 'calvin',
        'password': 'hobbes'
    }


@pytest.fixture
def settings():
    return {}


@pytest.fixture
def provider(auth, credentials, settings):
    return FedoraProvider(auth, credentials, settings)

test_file_path = '/farm/gorilla'
test_file_content =  b'banana eater'
test_folder_path = '/farm/barn/'
test_subfile_path = '/farm/barn/moo'
test_subfolder_path = '/farm/barn/stalls/'

# JSON-LD returned by fedora 4 by http://localhost:8080/rest/farm/gorilla/fcr:metadata
test_file_json_ld = '''
[ {
  "@id" : "http://localhost:8080/rest/farm/gorilla",
  "@type" : [ "http://www.w3.org/ns/ldp#NonRDFSource", "http://www.jcp.org/jcr/nt/1.0resource", "http://www.jcp.org/jcr/mix/1.0mimeType", "http://fedora.info/definitions/v4/repository#Binary", "http://fedora.info/definitions/v4/repository#Resource" ],
  "http://fedora.info/definitions/v4/repository#created" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:44:59.35Z"
  } ],
  "http://fedora.info/definitions/v4/repository#createdBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://fedora.info/definitions/v4/repository#hasFixityService" : [ {
    "@id" : "http://localhost:8080/rest/farm/gorilla/fcr:fixity"
  } ],
  "http://fedora.info/definitions/v4/repository#hasParent" : [ {
    "@id" : "http://localhost:8080/rest/farm"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModified" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:44:59.35Z"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModifiedBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://fedora.info/definitions/v4/repository#numberOfChildren" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#int",
    "@value" : "0"
  } ],
  "http://fedora.info/definitions/v4/repository#writable" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#boolean",
    "@value" : "true"
  } ],
  "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename" : [ {
    "@value" : "gorilla.jpg"
  } ],
  "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType" : [ {
    "@value" : "image/jpeg"
  } ],
  "http://www.iana.org/assignments/relation/describedby" : [ {
    "@id" : "http://localhost:8080/rest/farm/gorilla/fcr:metadata"
  } ],
  "http://www.loc.gov/premis/rdf/v1#hasMessageDigest" : [ {
    "@id" : "urn:sha1:16135783c4e06692974557a77aa4618d340f63a8"
  } ],
  "http://www.loc.gov/premis/rdf/v1#hasSize" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#long",
    "@value" : "373453"
  } ]
} ]
'''

# JSON-LD returned by fedora 4 by http://localhost:8080/rest/farm/barn/
test_folder_json_ld = '''
[ {
  "@id" : "http://localhost:8080/rest/farm/barn",
  "@type" : [ "http://www.w3.org/ns/ldp#RDFSource", "http://www.w3.org/ns/ldp#Container", "http://fedora.info/definitions/v4/repository#Container", "http://fedora.info/definitions/v4/repository#Resource" ],
  "http://fedora.info/definitions/v4/repository#created" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:43:50.994Z"
  } ],
  "http://fedora.info/definitions/v4/repository#createdBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://fedora.info/definitions/v4/repository#exportsAs" : [ {
    "@id" : "http://localhost:8080/rest/farm/barn/fcr:export?format=jcr/xml"
  } ],
  "http://fedora.info/definitions/v4/repository#hasParent" : [ {
    "@id" : "http://localhost:8080/rest/farm"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModified" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:44:29.561Z"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModifiedBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://fedora.info/definitions/v4/repository#numberOfChildren" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#int",
    "@value" : "2"
  } ],
  "http://fedora.info/definitions/v4/repository#writable" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#boolean",
    "@value" : "true"
  } ],
  "http://www.w3.org/ns/ldp#contains" : [ {
    "@id" : "http://localhost:8080/rest/farm/barn/stalls"
  }, {
    "@id" : "http://localhost:8080/rest/farm/barn/moo"
  } ]
}, {
  "@id" : "http://localhost:8080/rest/farm/barn/fcr:export?format=jcr/xml",
  "http://purl.org/dc/elements/1.1/format" : [ {
    "@id" : "http://fedora.info/definitions/v4/repository#jcr/xml"
  } ]
}, {
  "@id" : "http://localhost:8080/rest/farm/barn/moo",
  "@type" : [ "http://www.jcp.org/jcr/nt/1.0resource", "http://www.jcp.org/jcr/mix/1.0mimeType", "http://fedora.info/definitions/v4/repository#Binary", "http://fedora.info/definitions/v4/repository#Resource" ],
  "http://fedora.info/definitions/v4/repository#created" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:44:29.561Z"
  } ],
  "http://fedora.info/definitions/v4/repository#createdBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModified" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:44:29.561Z"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModifiedBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename" : [ {
    "@value" : "moo.ogg"
  } ],
  "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#hasMimeType" : [ {
    "@value" : "video/ogg"
  } ],
  "http://www.loc.gov/premis/rdf/v1#hasMessageDigest" : [ {
    "@id" : "urn:sha1:a269ae751ee365f2924764fd5846319ebd9abfa2"
  } ],
  "http://www.loc.gov/premis/rdf/v1#hasSize" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#long",
    "@value" : "22571"
  } ]
}, {
  "@id" : "http://localhost:8080/rest/farm/barn/stalls",
  "@type" : [ "http://fedora.info/definitions/v4/repository#Container", "http://fedora.info/definitions/v4/repository#Resource" ],
  "http://fedora.info/definitions/v4/repository#created" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:43:56.344Z"
  } ],
  "http://fedora.info/definitions/v4/repository#createdBy" : [ {
    "@value" : "bypassAdmin"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModified" : [ {
    "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
    "@value" : "2016-04-20T16:43:56.344Z"
  } ],
  "http://fedora.info/definitions/v4/repository#lastModifiedBy" : [ {
    "@value" : "bypassAdmin"
  } ]
} ]

'''

class TestProvider:
    # Test building a URL to a resource in the respository
    def test_build_repo_url(self, provider, credentials):
        repo = credentials['repo']
        path_str = '/path/to/file'
        path = WaterButlerPath(path_str)
        url = provider.build_repo_url(path)

        expected = repo.rstrip('/') + path_str
        assert url == expected


    # Ensure that a missing folder results in a NotFoundError exception
    @async
    @pytest.mark.aiohttpretty
    def test_metadata_missing_folder(self, provider):
        path = WaterButlerPath('/missing/')

        url = provider.build_repo_url(path)
        aiohttpretty.register_uri('GET', url, status=404)
        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.NotFoundError):
            yield from provider.metadata(path)

    # Ensure that a missing file results in a NotFoundError exception
    @async
    @pytest.mark.aiohttpretty
    def test_metadata_missing_file(self, provider):
        path = WaterButlerPath('/missing/file')

        url = provider.build_repo_url(path)
        aiohttpretty.register_uri('GET', url + '/fcr:metadata', status=404)
        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.NotFoundError):
            yield from provider.metadata(path)

    # Ensure that metadata can be returned for a file
    @async
    @pytest.mark.aiohttpretty
    def test_metadata_file(self, provider):
        path = WaterButlerPath(test_file_path)

        url = provider.build_repo_url(path)
        aiohttpretty.register_uri('HEAD', url, status=200)
        aiohttpretty.register_uri('GET', url + '/fcr:metadata', status=200, body=test_file_json_ld, headers={'Content-Type': 'application/json'})

        result = yield from provider.metadata(path)

        expected = FedoraFileMetadata(json.loads(test_file_json_ld), url, path)

        assert expected == result

    # Ensure that metadata can be returned for a folder
    @async
    @pytest.mark.aiohttpretty
    def test_metadata_folder(self, provider):
        path = WaterButlerPath(test_folder_path)

        url = provider.build_repo_url(path)
        aiohttpretty.register_uri('HEAD', url, status=200, headers={'Link': '<http://www.w3.org/ns/ldp#Container>;rel="type"'})
        aiohttpretty.register_uri('GET', url, status=200, body=test_folder_json_ld, headers={'Content-Type': 'application/json'})

        result = yield from provider.metadata(path)

        assert 2 == len(result)
        folder_md = None
        subfile_md = None

        for md in result:
            assert isinstance(md, metadata.BaseMetadata)
            print(md.name)
            print(md.wb_path)
            print(md.raw)
            print('ASDFASDFASDF')
            if md.name == 'moo.ogg':
                subfile_md = md
            else:
                subfolder_md = md

        assert subfile_md is not None
        assert subfile_md.name == 'moo.ogg'
        assert subfile_md.kind == 'file'
        assert subfile_md.path == test_subfile_path
        assert subfile_md.size == '22571'
        assert subfile_md.modified == '2016-04-20T16:44:29.561Z'
        assert subfile_md.content_type == 'video/ogg'

        assert subfolder_md is not None
        assert subfolder_md.name == 'stalls'
        assert subfolder_md.kind == 'folder'
        assert subfolder_md.modified == '2016-04-20T16:43:56.344Z'
        assert subfolder_md.path == test_subfolder_path


    # Test downloading a file
    @async
    @pytest.mark.aiohttpretty
    def test_download_file(self, provider):
        path = WaterButlerPath(test_file_path)
        url = provider.build_repo_url(path)

        aiohttpretty.register_uri('GET', url, body=test_file_content, auto_length=True, status=200)

        result = yield from provider.download(path)
        content = yield from result.response.read()

        assert test_file_content == content

    # Test downloading a file that does not exist
    @async
    @pytest.mark.aiohttpretty
    def test_download_non_existent_file(self, provider):
        path = WaterButlerPath(test_file_path)

        url = provider.build_repo_url(path)
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.DownloadError) as e:
            yield from provider.download(path)

        assert e.value.code == 404


    # Test uploading a file
    @async
    @pytest.mark.aiohttpretty
    def test_upload_file(self, provider):
        path = WaterButlerPath(test_file_path)
        url = provider.build_repo_url(path)
        file_stream = streams.FileStreamReader(io.BytesIO(test_file_content))
        fedora_id = provider.build_repo_url(path)
        file_metadata = json.loads(test_file_json_ld)

        aiohttpretty.register_uri('HEAD', url, status=200)
        aiohttpretty.register_uri('GET', url + '/fcr:metadata', status=200, body=test_file_json_ld, headers={'Content-Type': 'application/json'})
        aiohttpretty.register_json_uri('PUT', url, status=201, headers={'Location': url})

        metadata, created = yield from provider.upload(file_stream, path)

        expected = FedoraFileMetadata(file_metadata, fedora_id, path)

        assert created is True
        assert metadata == expected


    # Test deleting a folder
    @async
    @pytest.mark.aiohttpretty
    def test_delete_file(self, provider):
        path = WaterButlerPath(test_file_path)
        url = provider.build_repo_url(path)

        aiohttpretty.register_uri('DELETE', url, status=204)
        aiohttpretty.register_uri('DELETE', url + '/fcr:tombstone', status=204)

        yield from provider.delete(path)

    # Test creating a folder and returning metadata for it
    @async
    @pytest.mark.aiohttpretty
    def test_create_folder(self, provider):
        path = WaterButlerPath(test_folder_path)
        url = provider.build_repo_url(path)

        aiohttpretty.register_uri('PUT', url, status=201)
        aiohttpretty.register_uri('GET', url, status=200, body=test_folder_json_ld, headers={'Content-Type': 'application/json'})
        aiohttpretty.register_uri('HEAD', url, status=200, headers={'Link': '<http://www.w3.org/ns/ldp#Container>;rel="type"'})

        result = yield from provider.create_folder(path)

        assert result.name == 'barn'
        assert result.kind == 'folder'
        assert result.path == '/farm/barn/'

    # Test copying a file, /farm/gorilla, to a folder /farm/barn
    @async
    @pytest.mark.aiohttpretty
    def test_intra_copy(self, provider):
        test_file = WaterButlerPath(test_file_path)
        dest_file = WaterButlerPath('/farm/barn/gorilla')
        src_file_url = provider.build_repo_url(test_file)
        test_folder = WaterButlerPath(test_folder_path)
        dest_folder_url = provider.build_repo_url(test_folder)
        dest_file_url = dest_folder_url + '/gorilla'

        # Put new location in file json-ld
        dest_file_json_ld = test_file_json_ld.replace(src_file_url, dest_file_url)

        aiohttpretty.register_uri('GET', dest_file_url + '/fcr:metadata', status=200, body=dest_file_json_ld, headers={'Content-Type': 'application/json'})
        aiohttpretty.register_uri('HEAD', dest_file_url, status=200)
        aiohttpretty.register_json_uri('COPY', src_file_url, status=201, headers={'Location': dest_file_url})

        metadata, created = yield from provider.intra_copy(provider, test_file, test_folder)

        expected = FedoraFileMetadata(json.loads(dest_file_json_ld), dest_file_url, dest_file)

        assert created is True
        assert metadata == expected


    # Test moving a file, /farm/gorilla, to a folder /farm/barn
    @async
    @pytest.mark.aiohttpretty
    def test_intra_move(self, provider):
        test_file = WaterButlerPath(test_file_path)
        dest_file = WaterButlerPath('/farm/barn/gorilla')
        src_file_url = provider.build_repo_url(test_file)
        test_folder = WaterButlerPath(test_folder_path)
        dest_folder_url = provider.build_repo_url(test_folder)
        dest_file_url = dest_folder_url + '/gorilla'

        # Put new location in file json-ld
        dest_file_json_ld = test_file_json_ld.replace(src_file_url, dest_file_url)

        aiohttpretty.register_uri('GET', dest_file_url + '/fcr:metadata', status=200, body=dest_file_json_ld, headers={'Content-Type': 'application/json'})
        aiohttpretty.register_uri('HEAD', dest_file_url, status=200)
        aiohttpretty.register_json_uri('MOVE', src_file_url, status=201, headers={'Location': dest_file_url})
        aiohttpretty.register_json_uri('DELETE', src_file_url + '/fcr:tombstone', status=204)

        metadata, created = yield from provider.intra_move(provider, test_file, test_folder)

        expected = FedoraFileMetadata(json.loads(dest_file_json_ld), dest_file_url, dest_file)

        assert created is True
        assert metadata == expected


    # Ensure that validate_path works.
    @async
    @pytest.mark.aiohttpretty
    def test_validate_path(self, provider):
        result = yield from provider.validate_path(test_file_path)
        assert result == WaterButlerPath(test_file_path)

        result = yield from provider.validate_path(test_folder_path)
        assert result == WaterButlerPath(test_folder_path)

    # Ensure that validate_v1_path works on existing file
    @async
    @pytest.mark.aiohttpretty
    def test_validate_v1_path_existing_file(self, provider):
        expected = WaterButlerPath(test_file_path)
        url = provider.build_repo_url(expected)

        aiohttpretty.register_uri('HEAD', url, status=200)

        result = yield from provider.validate_v1_path(test_file_path)

        assert result == expected

    # Ensure that validate_v1_path works on missing file
    @async
    @pytest.mark.aiohttpretty
    def test_validate_v1_path_missing_file(self, provider):
        url = provider.build_repo_url(WaterButlerPath(test_file_path))

        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.NotFoundError):
            yield from provider.validate_v1_path(test_file_path)


    # Ensure that validate_v1_path throws NotFoundError when types do not match
    @async
    @pytest.mark.aiohttpretty
    def test_validate_v1_path_fails_treating_file_as_folder(self, provider):
        url = provider.build_repo_url(WaterButlerPath(test_file_path))

        aiohttpretty.register_uri('HEAD', url, status=200, headers={'Link': '<http://www.w3.org/ns/ldp#Container>;rel="type"'})

        with pytest.raises(exceptions.NotFoundError):
            result = yield from provider.validate_v1_path(test_file_path)
