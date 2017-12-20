import os
import io
import time
import base64
import hashlib
import aiohttpretty
from http import client
from urllib import parse
from unittest import mock

import pytest

from waterbutler.core import (streams,
                              metadata,
                              exceptions)
from waterbutler.providers.s3 import S3Provider
from waterbutler.core.path import WaterButlerPath

from tests.utils import MockCoroutine
from tests.providers.s3.fixtures import (
    auth,
    credentials,
    settings,
    file_content,
    folder_metadata,
    folder_single_item_metadata,
    folder_item_metadata,
    version_metadata,
    single_version_metadata,
    folder_metadata,
    folder_and_contents,
    folder_empty_metadata,
    file_header_metadata,
    file_metadata_headers_object,
    file_metadata_object,
    folder_key_metadata_object,
    revision_metadata_object
)


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
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


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
        ('ca-central-1',   's3-ca-central-1.amazonaws.com'),
        ('eu-central-1',   's3-eu-central-1.amazonaws.com'),
        ('eu-west-2',      's3-eu-west-2.amazonaws.com'),
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
        aiohttpretty.register_uri('GET',
                                  region_url,
                                  status=200,
                                  body=location_response(region_name))

        await provider._check_region()
        assert provider.connection.host == host


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_header_metadata, mock_time):
        file_path = 'foobah'

        params = {'prefix': '/' + file_path + '/', 'delimiter': '/'}
        good_metadata_url = provider.bucket.new_key('/' + file_path).generate_url(100, 'HEAD')
        bad_metadata_url = provider.bucket.generate_url(100)
        aiohttpretty.register_uri('HEAD', good_metadata_url, headers=file_header_metadata)
        aiohttpretty.register_uri('GET', bad_metadata_url, params=params, status=404)

        assert WaterButlerPath('/') == await provider.validate_v1_path('/')

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
        response_headers = {'response-content-disposition': 'attachment'}
        url = provider.bucket.new_key(path.path).generate_url(100,
                                                              response_headers=response_headers)
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

        result = await provider.download(path, revision='someversion')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_display_name(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        response_headers = {'response-content-disposition': "attachment; filename*=UTF-8''tuna"}
        url = provider.bucket.new_key(path.path).generate_url(100,
                                                              response_headers=response_headers)
        aiohttpretty.register_uri('GET', url, body=b'delicious', auto_length=True)

        result = await provider.download(path, displayName='tuna')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle')
        response_headers = {'response-content-disposition': 'attachment'}
        url = provider.bucket.new_key(path.path).generate_url(100,
                                                              response_headers=response_headers)
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
    async def test_upload_update(self,
                                 provider,
                                 file_content,
                                 file_stream,
                                 file_header_metadata,
                                 mock_time):

        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata)
        header = {'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=201, headers=header)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert not created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_encrypted(self,
                                    provider,
                                    file_content,
                                    file_stream,
                                    file_header_metadata,
                                    mock_time):

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
                {'headers': file_header_metadata},
            ],
        )
        headers={'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers)

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
    async def test_delete_comfirm_delete(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/')

        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params={'prefix': ''},
            body=folder_and_contents,
            status=200,
        )

        (payload, headers) = bulk_delete_body(
            ['thisfolder/', 'thisfolder/item1', 'thisfolder/item2']
        )
        delete_url = provider.bucket.generate_url(
            100,
            'POST',
            query_parameters={'delete': ''},
            headers=headers,
        )
        aiohttpretty.register_uri('POST', delete_url, status=204)

        with pytest.raises(exceptions.DeleteError):
            await provider.delete(path)

        await provider.delete(path, confirm_delete=1)

        assert aiohttpretty.has_call(method='POST', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/some-folder/')

        params = {'prefix': 'some-folder/'}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=folder_and_contents,
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
    async def test_single_item_folder_delete(self,
                                             provider,
                                             folder_single_item_metadata,
                                             mock_time):
        path = WaterButlerPath('/single-thing-folder/')

        params = {'prefix': 'single-thing-folder/'}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=folder_single_item_metadata,
            status=200,
        )

        (payload, headers) = bulk_delete_body(
            ['my-image.jpg']
        )
        delete_url = provider.bucket.generate_url(
            100,
            'POST',
            query_parameters={'delete': ''},
            headers=headers,
        )
        aiohttpretty.register_uri('POST', delete_url, status=204)


        await provider.delete(path)
        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params)
        aiohttpretty.register_uri('POST', delete_url, status=204)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_folder_delete(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/empty-folder/')

        params = {'prefix': 'empty-folder/'}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=folder_empty_metadata,
            status=200,
        )

        with pytest.raises(exceptions.NotFoundError):
            await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params)

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
        response_headers = {'response-content-disposition': 'attachment'}
        url = provider.bucket.new_key(path.path).generate_url(100,
                                                              'GET',
                                                              response_headers=response_headers)

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
        assert result[2].extra['hashes']['md5'] == '1b2cf535f27731c974343645a3985328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_self_listing(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/thisfolder/')
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_and_contents)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result:
            assert fobj.name != path.path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata_folder_item(self, provider, folder_item_metadata, mock_time):
        path = WaterButlerPath('/')
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_item_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_metadata_folder(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/this-is-not-the-root/')
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')

        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_empty_metadata,
                                  headers={'Content-Type': 'application/xml'})


        aiohttpretty.register_uri('HEAD', metadata_url, header=folder_empty_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 0


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_lastest_revision(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg')
        url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata)

        result = await provider.metadata(path, revision='Latest')

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == str(path)
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'
        assert result.extra['hashes']['md5'] == 'fba9dede5f27731c9771645a39863328'

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
    async def test_upload(self,
                          provider,
                          file_content,
                          file_stream,
                          file_header_metadata,
                          mock_time):

        path = WaterButlerPath('/foobah')
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )
        headers = {'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers),

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_checksum_mismatch(self,
                                            provider,
                                            file_stream,
                                            file_header_metadata,
                                            mock_time):
        path = WaterButlerPath('/foobah')
        url = provider.bucket.new_key(path.path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )
        aiohttpretty.register_uri('PUT', url, status=200, headers={'ETag': '"bad hash"'})

        with pytest.raises(exceptions.UploadChecksumMismatchError):
            await provider.upload(file_stream, path)

        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raise_409(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/alreadyexists/')
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "alreadyexists", because a file or '
                                   'folder already exists with that name')

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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy(self, provider, file_header_metadata, mock_time):

        source_path = WaterButlerPath('/source')
        dest_path = WaterButlerPath('/dest')
        metadata_url = provider.bucket.new_key(dest_path.path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata)

        header_path = '/' + os.path.join(provider.settings['bucket'], source_path.path)
        headers = {'x-amz-copy-source': parse.quote(header_path)}

        url = provider.bucket.new_key(dest_path.path).generate_url(100, 'PUT', headers=headers)
        aiohttpretty.register_uri('PUT', url, status=200)

        metadata, exists = await provider.intra_copy(provider, source_path, dest_path)


        assert provider._check_region.called

        assert metadata.kind == 'file'
        assert not exists
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=url, headers=headers)

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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_version_metadata(self, provider, single_version_metadata, mock_time):
        path = WaterButlerPath('/single-version.file')
        url = provider.bucket.generate_url(100, 'GET', query_parameters={'versions': ''})
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET',
                                  url,
                                  params=params,
                                  status=200,
                                  body=single_version_metadata)

        data = await provider.revisions(path)

        assert isinstance(data, list)
        assert len(data) == 1

        for item in data:
            assert hasattr(item, 'extra')
            assert hasattr(item, 'version')
            assert hasattr(item, 'version_identifier')

        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    def test_can_intra_move(self, provider):

        file_path = WaterButlerPath('/my-image.jpg')
        folder_path = WaterButlerPath('/folder/', folder=True)

        assert provider.can_intra_move(provider)
        assert provider.can_intra_move(provider, file_path)
        assert not provider.can_intra_move(provider, folder_path)

    def test_can_intra_copy(self, provider):

        file_path = WaterButlerPath('/my-image.jpg')
        folder_path = WaterButlerPath('/folder/', folder=True)

        assert provider.can_intra_copy(provider)
        assert provider.can_intra_copy(provider, file_path)
        assert not provider.can_intra_copy(provider, folder_path)

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()
