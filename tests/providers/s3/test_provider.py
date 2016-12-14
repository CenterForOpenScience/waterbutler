import pytest

from tests.utils import MockCoroutine

import io
import time
import base64
import hashlib
from http import client
from unittest import mock

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import metadata
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.s3 import S3Provider
from waterbutler.providers.s3.metadata import S3FileMetadata
from waterbutler.providers.s3.metadata import S3FolderMetadata


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
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1454684930.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def provider(auth, credentials, settings):
    provider = S3Provider(auth, credentials, settings)
    provider._check_region = MockCoroutine()
    return provider


@pytest.fixture
def file_content():
    return b'sleepy'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def folder_metadata():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>my-image.jpg</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>434234</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <Contents>
                <Key>my-third-image.jpg</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;1b2cf535f27731c974343645a3985328&quot;</ETag>
                <Size>64994</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <CommonPrefixes>
                <Prefix>   photos/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>'''


@pytest.fixture
def just_a_folder_metadata():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>naptime/</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
        </ListBucketResult>'''


@pytest.fixture
def contents_and_self():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>thisfolder/</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <Contents>
                <Key>thisfolder/item1</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
            <Contents>
                <Key>thisfolder/item2</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
        </ListBucketResult>'''


@pytest.fixture
def folder_empty_metadata():
    return b'''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
        </ListBucketResult>'''


@pytest.fixture
def file_metadata():
    return {
        'Content-Length': 9001,
        'Last-Modified': 'SomeTime',
        'Content-Type': 'binary/octet-stream',
        'ETag': '"fba9dede5f27731c9771645a39863328"',
        'X-AMZ-SERVER-SIDE-ENCRYPTION': 'AES256'
    }


@pytest.fixture
def version_metadata():
    return b'''<?xml version="1.0" encoding="UTF-8"?>

    <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
        <Name>bucket</Name>
        <Prefix>my</Prefix>
        <KeyMarker/>
        <VersionIdMarker/>
        <MaxKeys>5</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Version>
            <Key>my-image.jpg</Key>
            <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
            <IsLatest>true</IsLatest>
            <LastModified>2009-10-12T17:50:30.000Z</LastModified>
            <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
            <Size>434234</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
        <Version>
            <Key>my-image.jpg</Key>
            <VersionId>QUpfdndhfd8438MNFDN93jdnJFkdmqnh893</VersionId>
            <IsLatest>false</IsLatest>
            <LastModified>2009-10-10T17:50:30.000Z</LastModified>
            <ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>
            <Size>166434</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
        <Version>
            <Key>my-image.jpg</Key>
            <VersionId>UIORUnfndfhnw89493jJFJ</VersionId>
            <IsLatest>false</IsLatest>
            <LastModified>2009-10-11T12:50:30.000Z</LastModified>
            <ETag>&quot;772cf535f27731c974343645a3985328&quot;</ETag>
            <Size>64</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Version>
    </ListVersionsResult>'''

def location_response(location):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        '{}</LocationConstraint>'
    ).format(location)

def list_objects_response(keys, truncated=False):
    response = '''<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>bucket</Name>
        <Prefix/>
        <Marker/>
        <MaxKeys>1000</MaxKeys>'''

    response += '<IsTruncated>' + str(truncated).lower() + '</IsTruncated>'
    response += ''.join(map(
        lambda x: '<Contents><Key>{}</Key></Contents>'.format(x),
        keys
    ))

    response += '</ListBucketResult>'

    return response.encode('utf-8')

def bulk_delete_body(keys):
    payload = '<?xml version="1.0" encoding="UTF-8"?>'
    payload += '<Delete>'
    payload += ''.join(map(
        lambda x: '<Object><Key>{}</Key></Object>'.format(x),
        keys
    ))
    payload += '</Delete>'
    payload = payload.encode('utf-8')

    md5 = base64.b64encode(hashlib.md5(payload).digest())
    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5.decode('ascii'),
        'Content-Type': 'text/xml',
    }

    return (payload, headers)

def build_folder_params(path):
    return {'prefix': path.path, 'delimiter': '/'}


class TestRegionDetection:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("region_name,host", [
        ('',               's3.amazonaws.com'),
        ('EU',             's3-eu-west-1.amazonaws.com'),
        ('us-east-2',      's3-us-east-2.amazonaws.com'),
        ('us-west-1',      's3-us-west-1.amazonaws.com'),
        ('us-west-2',      's3-us-west-2.amazonaws.com'),
        ('eu-central-1',   's3-eu-central-1.amazonaws.com'),
        ('ap-northeast-1', 's3-ap-northeast-1.amazonaws.com'),
        ('ap-northeast-2', 's3-ap-northeast-2.amazonaws.com'),
        ('ap-south-1',     's3-ap-south-1.amazonaws.com'),
        ('ap-southeast-1', 's3-ap-southeast-1.amazonaws.com'),
        ('ap-southeast-2', 's3-ap-southeast-2.amazonaws.com'),
        ('sa-east-1',      's3-sa-east-1.amazonaws.com'),
    ])
    async def test_region_host(self, auth, credentials, settings, region_name, host, mock_time):
        provider = S3Provider(auth, credentials, settings)
        orig_host = provider.connection.host

        region_url = provider.bucket.generate_url(
            100,
            'GET',
            query_parameters={'location': ''},
        )
        aiohttpretty.register_uri('GET', region_url, status=200, body=location_response(region_name))

        await provider._check_region()
        assert provider.connection.host == host


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_metadata, mock_time):
        file_path = 'foobah'

        params = {'prefix': '/' + file_path + '/', 'delimiter': '/'}
        good_metadata_url = provider.bucket.new_key('/' + file_path).generate_url(100, 'HEAD')
        bad_metadata_url = provider.bucket.generate_url(100)
        aiohttpretty.register_uri('HEAD', good_metadata_url, headers=file_metadata)
        aiohttpretty.register_uri('GET', bad_metadata_url, params=params, status=404)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, folder_metadata, mock_time):
        folder_path = 'Photos'

        params = {'prefix': '/' + folder_path + '/', 'delimiter': '/'}
        good_metadata_url = provider.bucket.generate_url(100)
        bad_metadata_url = provider.bucket.new_key('/' + folder_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'GET', good_metadata_url, params=params,
            body=folder_metadata, headers={'Content-Type': 'application/xml'}
        )
        aiohttpretty.register_uri('HEAD', bad_metadata_url, status=404)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_path + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_path)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_path + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_normal_name(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/path.txt')
        assert path.name == 'path.txt'
        assert path.parent.name == 'a'
        assert path.is_file
        assert not path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_folder(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root

    @pytest.mark.asyncio
    async def test_root(self, provider, mock_time):
        path = await provider.validate_path('/this/is/a/folder/')
        assert path.name == 'folder'
        assert path.parent.name == 'a'
        assert not path.is_file
        assert path.is_dir
        assert not path.is_root


class TestCRUD:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = provider.bucket.new_key(path.path).generate_url(100, response_headers={'response-content-disposition': 'attachment'})
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_version(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = provider.bucket.new_key(path.path).generate_url(
            100,
            query_parameters={'versionId': 'someversion'},
            response_headers={'response-content-disposition': 'attachment'},
        )
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True)

        result = await provider.download(path, version='someversion')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_display_name(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = provider.bucket.new_key(path.path).generate_url(100, response_headers={'response-content-disposition': "attachment; filename*=UTF-8''tuna"})
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True)

        result = await provider.download(path, displayName='tuna')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        url = provider.bucket.new_key(path.path).generate_url(100, response_headers={'response-content-disposition': 'attachment'})
        aiohttpretty.register_uri('GET', url, status=404)

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder_400s(self, provider, mock_time):
        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(WaterButlerPath('/cool/folder/mom/'))
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, file_content, file_stream, file_metadata, mock_time):
        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_metadata)
        aiohttpretty.register_uri('PUT', url, status=201, headers={'ETag': '"{}"'.format(content_md5)})

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_encrypted(self, provider, file_content, file_stream, file_metadata, mock_time):
        # Set trigger for encrypt_key=True in s3.provider.upload
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.path).generate_url(100, 'PUT', encrypt_key=True)
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_metadata},
            ],
        )
        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"{}"'.format(content_md5)})

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.extra['encryption'] == 'AES256'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-file')
        url = provider.bucket.new_key(path.path).generate_url(100, 'DELETE')
        aiohttpretty.register_uri('DELETE', url, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, contents_and_self, mock_time):
        path = WaterButlerPath('/some-folder/')

        params = {'prefix': 'some-folder/'}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=contents_and_self,
            status=200,
        )

        query_params = {'delete': ''}
        (payload, headers) = bulk_delete_body(
            ['thisfolder/', 'thisfolder/item1', 'thisfolder/item2']
        )

        delete_url = provider.bucket.generate_url(
            100,
            'POST',
            query_parameters=query_params,
            headers=headers,
        )
        aiohttpretty.register_uri('POST', delete_url, status=204)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params)
        assert aiohttpretty.has_call(method='POST', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_large_folder_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-folder/')

        query_url = provider.bucket.generate_url(100, 'GET')

        keys_one = [str(x) for x in range(2500, 3500)]
        response_one = list_objects_response(keys_one, truncated=True)
        params_one = {'prefix': 'some-folder/'}

        keys_two = [str(x) for x in range(3500, 3601)]
        response_two = list_objects_response(keys_two)
        params_two = {'prefix': 'some-folder/', 'marker': '3499'}

        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params_one,
            body=response_one,
            status=200,
        )
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params_two,
            body=response_two,
            status=200,
        )

        query_params = {'delete': None}

        (payload_one, headers_one) = bulk_delete_body(keys_one)
        delete_url_one = provider.bucket.generate_url(
            100,
            'POST',
            query_parameters=query_params,
            headers=headers_one,
        )
        aiohttpretty.register_uri('POST', delete_url_one, status=204)

        (payload_two, headers_two) = bulk_delete_body(keys_two)
        delete_url_two = provider.bucket.generate_url(
            100,
            'POST',
            query_parameters=query_params,
            headers=headers_two,
        )
        aiohttpretty.register_uri('POST', delete_url_two, status=204)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params_one)
        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params_two)
        assert aiohttpretty.has_call(method='POST', uri=delete_url_one)
        assert aiohttpretty.has_call(method='POST', uri=delete_url_two)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_accepts_url(self, provider, mock_time):
        path = WaterButlerPath('/my-image')
        url = provider.bucket.new_key(path.path).generate_url(100, 'GET', response_headers={'response-content-disposition': 'attachment'})

        ret_url = await provider.download(path, accept_url=True)

        assert ret_url == url


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/darp/')
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0].name == '   photos'
        assert result[1].name == 'my-image.jpg'
        assert result[2].extra['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_self_listing(self, provider, contents_and_self, mock_time):
        path = WaterButlerPath('/thisfolder/')
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=contents_and_self)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result:
            assert fobj.name != path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_just_a_folder_metadata_folder(self, provider, just_a_folder_metadata, mock_time):
        path = WaterButlerPath('/')
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=just_a_folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_must_have_slash(self, provider, just_a_folder_metadata, mock_time):
    #     with pytest.raises(exceptions.InvalidPathError):
    #         await provider.metadata('')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, headers=file_metadata)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        path = WaterButlerPath('/notfound.txt')
        url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_content, file_stream, file_metadata, mock_time):
        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_metadata},
            ],
        )
        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"{}"'.format(content_md5)}),

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raise_409(self, provider, just_a_folder_metadata, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=just_a_folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot create folder "alreadyexists" because a file or folder already exists at path "/alreadyexists/"'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_start_with_slash(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists')

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400
        assert e.value.message == 'Path must be a directory'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)
        create_url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')

        aiohttpretty.register_uri('GET', url, params=params, status=404)
        aiohttpretty.register_uri('PUT', create_url, status=403)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out_metadata(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET', url, params=params, status=403)

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/')
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)
        create_url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')

        aiohttpretty.register_uri('GET', url, params=params, status=404)
        aiohttpretty.register_uri('PUT', create_url, status=200)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/doesntalreadyexists/'


class TestOperations:

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_copy(self, provider, file_metadata, mock_time):
    #     dest_path = WaterButlerPath('/dest')
    #     source_path = WaterButlerPath('/source')
    #     headers = {'x-amz-copy-source': '/{}/{}'.format(provider.settings['bucket'], source_path.path)}

    #     metadata_url = provider.bucket.new_key(dest_path.path).generate_url(100, 'HEAD')
    #     url = provider.bucket.new_key(dest_path.path).generate_url(100, 'PUT', headers=headers)

    #     aiohttpretty.register_uri('PUT', url, status=200)
    #     aiohttpretty.register_uri('HEAD', metadata_url, headers=file_metadata)

    #     resp = await provider.copy(provider, source_path, dest_path)

    #     # TODO: matching url content for request
    #     assert resp['kind'] == 'file'
    #     assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)
    #     assert aiohttpretty.has_call(method='PUT', uri=url, headers=headers)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_metadata(self, provider, version_metadata, mock_time):
        path = WaterButlerPath('/my-image.jpg')
        url = provider.bucket.generate_url(100, 'GET', query_parameters={'versions': ''})
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, status=200, body=version_metadata)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 3

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    async def test_equality(self, provider, mock_time):
        assert provider.can_intra_copy(provider)
        assert provider.can_intra_move(provider)
