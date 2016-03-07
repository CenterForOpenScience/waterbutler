import pytest

import io
from http import client

import aiohttpretty

import logging

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.onedrive import OneDriveProvider
from waterbutler.providers.onedrive.settings import settings
from waterbutler.providers.onedrive.metadata import OneDriveRevision
from waterbutler.providers.onedrive.metadata import OneDriveFileMetadata
from waterbutler.providers.onedrive.metadata import OneDriveFolderMetadata

logger = logging.getLogger(__name__)

@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'wrote harry potter'}


@pytest.fixture
def settings():
    return {'folder': '11446498'}


@pytest.fixture
def provider(auth, credentials, settings):
    return OneDriveProvider(auth, credentials, settings)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR OSX GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)

@pytest.fixture
def folder_object_metadata():
    return {
       "size": 119410,
       "name": "sub1-b",
       "folder": {
          "childCount": 4
       },
       "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
       "id": "75BFE374EBEB1211!118",
       "createdDateTime": "2015-11-29T17:21:09.997Z",
       "lastModifiedDateTime": "2015-12-07T16:45:28.46Z",
       "parentReference": {
          "driveId": "75bfe374ebeb1211",
          "path": "/drive/root:/ryan-test1",
          "id": "75BFE374EBEB1211!107"
       },
       "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!118",
       "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITExOC42MzU4NTEwMzUyODQ2MDAwMDA",
       "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMTguMw",
       "children": [
              {
                 "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!118",
                 "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMTguMw",
                 "fileSystemInfo": {
                    "lastModifiedDateTime": "2015-12-07T16:45:28.46Z",
                    "createdDateTime": "2015-11-29T17:21:09.997Z"
                 },
                 "id": "75BFE374EBEB1211!118",
                 "lastModifiedDateTime": "2015-12-09T01:48:52.31Z",
                 "size": 119410,
                 "createdDateTime": "2015-11-29T17:21:09.997Z",
                 "folder": {
                    "childCount": 4
                 },
                 "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITExOC42MzU4NTIyMjUzMjMxMDAwMDA",
                 "name": "sub1-b",
                 "parentReference": {
                    "driveId": "75bfe374ebeb1211",
                    "path": "/drive/root:/ryan-test1/sub1",
                    "id": "75BFE374EBEB1211!107"
                 }
              },
              {
                 "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!143",
                 "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExNDMuMTI",
                 "fileSystemInfo": {
                    "lastModifiedDateTime": "2015-12-07T17:26:09.48Z",
                    "createdDateTime": "2015-12-01T16:52:33.07Z"
                 },
                 "id": "75BFE374EBEB1211!143",
                 "lastModifiedDateTime": "2015-12-07T17:26:09.48Z",
                 "size": 0,
                 "createdDateTime": "2015-12-01T16:52:33.07Z",
                 "folder": {
                    "childCount": 1
                 },
                 "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITE0My42MzU4NTEwNTk2OTQ4MDAwMDA",
                 "name": "sub1-z",
                 "parentReference": {
                    "driveId": "75bfe374ebeb1211",
                    "path": "/drive/root:/ryan-test1/sub1",
                    "id": "75BFE374EBEB1211!107"
                 }
              },
              {
                 "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!150",
                 "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExNTAuMTE",
                 "fileSystemInfo": {
                    "lastModifiedDateTime": "2015-12-08T21:51:15.593Z",
                    "createdDateTime": "2015-12-02T20:25:26.51Z"
                 },
                 "id": "75BFE374EBEB1211!150",
                 "lastModifiedDateTime": "2015-12-08T21:51:15.593Z",
                 "size": 83736,
                 "@content.downloadUrl": "https://public-ch3302.files.1drv.com/y3mgyZqUob4fS1RGIHa8w3tl0ozOlXXiKPMmz3hxZ0KbMqyZmIOnzXL8G9fWREL01mog9XRQn2g2qExRSSFce9ixl7fOlq_yjwOxX-6F2CNzgp3-wE9oThZSrvTix8h7cMD32RHd-__uwGK6Db0ErsGuxorWJKfRlmkpJFn7b8F9ZVvsIsLOmJWVKMyxrQMfves",
                 "cTag": "aYzo3NUJGRTM3NEVCRUIxMjExITE1MC4yNTc",
                 "file": {
                    "mimeType": "image/jpeg",
                    "hashes": {
                       "crc32Hash": "6D98C9D5",
                       "sha1Hash": "68A4192BF9DEAD103D7E4EA481074745932989F4"
                    }
                 },
                 "name": "elect-a.jpg",
                 "parentReference": {
                    "driveId": "75bfe374ebeb1211",
                    "path": "/drive/root:/ryan-test1/sub1",
                    "id": "75BFE374EBEB1211!107"
                 },
              }
           ],
    }

@pytest.fixture
def folder_list_metadata():
    return {

           "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!107",
           "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMDcuMA",
           "fileSystemInfo": {
              "lastModifiedDateTime": "2015-11-22T14:33:33.57Z",
              "createdDateTime": "2015-11-22T14:33:33.57Z"
           },
           "children@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items('75BFE374EBEB1211%21107')/children",
           "id": "75BFE374EBEB1211!107",
           "lastModifiedDateTime": "2015-12-11T14:45:36.6Z",
           "size": 203146,
           "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
           "createdDateTime": "2015-11-22T14:33:33.57Z",
           "folder": {
              "childCount": 3
           },
           "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITEwNy42MzU4NTQ0MTkzNjYwMDAwMDA",
           "children": [
              {
                 "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!118",
                 "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMTguMw",
                 "fileSystemInfo": {
                    "lastModifiedDateTime": "2015-12-07T16:45:28.46Z",
                    "createdDateTime": "2015-11-29T17:21:09.997Z"
                 },
                 "id": "75BFE374EBEB1211!118",
                 "lastModifiedDateTime": "2015-12-09T01:48:52.31Z",
                 "size": 119410,
                 "createdDateTime": "2015-11-29T17:21:09.997Z",
                 "folder": {
                    "childCount": 4
                 },
                 "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITExOC42MzU4NTIyMjUzMjMxMDAwMDA",
                 "name": "sub1-b",
                 "parentReference": {
                    "driveId": "75bfe374ebeb1211",
                    "path": "/drive/root:/ryan-test1",
                    "id": "75BFE374EBEB1211!107"
                 }
              },
              {
                 "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!143",
                 "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExNDMuMTI",
                 "fileSystemInfo": {
                    "lastModifiedDateTime": "2015-12-07T17:26:09.48Z",
                    "createdDateTime": "2015-12-01T16:52:33.07Z"
                 },
                 "id": "75BFE374EBEB1211!143",
                 "lastModifiedDateTime": "2015-12-07T17:26:09.48Z",
                 "size": 0,
                 "createdDateTime": "2015-12-01T16:52:33.07Z",
                 "folder": {
                    "childCount": 1
                 },
                 "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITE0My42MzU4NTEwNTk2OTQ4MDAwMDA",
                 "name": "sub1-z",
                 "parentReference": {
                    "driveId": "75bfe374ebeb1211",
                    "path": "/drive/root:/ryan-test1",
                    "id": "75BFE374EBEB1211!107"
                 }
              },
              {
                 "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!150",
                 "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExNTAuMTE",
                 "fileSystemInfo": {
                    "lastModifiedDateTime": "2015-12-08T21:51:15.593Z",
                    "createdDateTime": "2015-12-02T20:25:26.51Z"
                 },
                 "id": "75BFE374EBEB1211!150",
                 "lastModifiedDateTime": "2015-12-08T21:51:15.593Z",
                 "photo": {
                    "takenDateTime": "2013-04-17T14:32:26Z"
                 },
                 "size": 83736,
                 "@content.downloadUrl": "https://public-ch3302.files.1drv.com/y3mgyZqUob4fS1RGIHa8w3tl0ozOlXXiKPMmz3hxZ0KbMqyZmIOnzXL8G9fWREL01mog9XRQn2g2qExRSSFce9ixl7fOlq_yjwOxX-6F2CNzgp3-wE9oThZSrvTix8h7cMD32RHd-__uwGK6Db0ErsGuxorWJKfRlmkpJFn7b8F9ZVvsIsLOmJWVKMyxrQMfves",
                 "cTag": "aYzo3NUJGRTM3NEVCRUIxMjExITE1MC4yNTc",
                 "image": {
                    "width": 883,
                    "height": 431
                 },
                 "file": {
                    "mimeType": "image/jpeg",
                    "hashes": {
                       "crc32Hash": "6D98C9D5",
                       "sha1Hash": "68A4192BF9DEAD103D7E4EA481074745932989F4"
                    }
                 },
                 "name": "elect-a.jpg",
                 "parentReference": {
                    "driveId": "75bfe374ebeb1211",
                    "path": "/drive/root:/ryan-test1",
                    "id": "75BFE374EBEB1211!107"
                 },
                 "createdDateTime": "2015-12-02T20:25:26.51Z"
              }
           ],
           "name": "ryan-test1",
           "parentReference": {
              "driveId": "75bfe374ebeb1211",
              "path": "/drive/root:",
              "id": "75BFE374EBEB1211!103"
           }
    }

@pytest.fixture
def file_root_folder_metadata():
    return {
           "id": "75BFE374EBEB1211!128",
           "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITEyOC42MzU4NTYxODI2MDA5MzAwMDA",
           "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMjguMA",
           "size": 998322,
           "name": "hello.jpg",
           "parentReference": {
              "id": "75BFE374EBEB1211!103",
              "path": "/drive/root:/sam",
              "driveId": "75bfe374ebeb1211"
           },
           "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
           "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!128",
           "file": {
                "hashes": {
                   "crc32Hash": "6D98C9D5",
                   "sha1Hash": "68A4192BF9DEAD103D7E4EA481074745932989F4"
                },
                "mimeType": "image/jpeg"
            },
    }


@pytest.fixture
def folder_sub_folder_metadata():
    return {
           "id": "75BFE374EBEB1211!128",
           "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITEyOC42MzU4NTYxODI2MDA5MzAwMDA",
           "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMjguMA",
           "size": 998322,
           "name": "hello",
           "parentReference": {
              "id": "75BFE374EBEB1211!103",
              "path": "/drive/root:/sam/i/am",
              "driveId": "75bfe374ebeb1211"
           },
           "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
           "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!128",
           "folder": {
              "childCount": 3
            },
    }

@pytest.fixture
def file_sub_folder_metadata():
    return {
           "id": "75BFE374EBEB1211!128",
           "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITEyOC42MzU4NTYxODI2MDA5MzAwMDA",
           "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMjguMA",
           "size": 998322,
           "name": "hello.jpg",
           "parentReference": {
              "id": "75BFE374EBEB1211!103",
              "path": "/drive/root:/sam/i/am",
              "driveId": "75bfe374ebeb1211"
           },
           "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
           "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!128",
           "file": {
                "hashes": {
                   "crc32Hash": "6D98C9D5",
                   "sha1Hash": "68A4192BF9DEAD103D7E4EA481074745932989F4"
                },
                "mimeType": "image/jpeg"
            },
    }

@pytest.fixture
def file_root_parent_metadata():
    return {
           "id": "75BFE374EBEB1211!150",
           "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!150",
           "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
           "cTag": "aYzo3NUJGRTM3NEVCRUIxMjExITE1MC4yNTc",
           "children": [],
           "file": {
              "hashes": {
                 "sha1Hash": "68A4192BF9DEAD103D7E4EA481074745932989F4",
                 "crc32Hash": "6D98C9D5"
              },
              "mimeType": "image/jpeg"
           },
           "fileSystemInfo": {
              "createdDateTime": "2015-12-02T20:25:26.51Z",
              "lastModifiedDateTime": "2015-12-08T21:51:15.593Z"
           },
           "createdDateTime": "2015-12-02T20:25:26.51Z",
           "size": 83736,
           "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExNTAuMTE",
           "name": "elect-a.jpg",
           "@content.downloadUrl": "https://public-ch3302.files.1drv.com/y3mnrbLFOgJJ8JQA7Ots0pzvL0xHYJx9NQJylS6IoQqp5G2CIIG5IWCKT_ADdp035kbr3qEmz6Va5j8-NCplk4ZMG_cYipxUfhP-NNl-SjlKocwc7yDplc1qWEynHGm_lME_o98pKSxNg6sKbEphRPufHea_h7LU1XH2qkFEGOIZGHQlw_JmH9fvygq8_XY2iE-",
           "children@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items('75BFE374EBEB1211%21150')/children",
           "parentReference": {
              "id": "75BFE374EBEB1211!107",
              "driveId": "75bfe374ebeb1211",
              "path": "/drive/root:"
           },
           "lastModifiedDateTime": "2015-12-08T21:51:15.593Z"
    }


@pytest.fixture
def file_metadata():
    return {
           "id": "75BFE374EBEB1211!150",
           "webUrl": "https://onedrive.live.com/redir?resid=75BFE374EBEB1211!150",
           "@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity",
           "cTag": "aYzo3NUJGRTM3NEVCRUIxMjExITE1MC4yNTc",
           "children": [],
           "image": {
              "width": 883,
              "height": 431
           },
           "file": {
              "hashes": {
                 "sha1Hash": "68A4192BF9DEAD103D7E4EA481074745932989F4",
                 "crc32Hash": "6D98C9D5"
              },
              "mimeType": "image/jpeg"
           },
           "fileSystemInfo": {
              "createdDateTime": "2015-12-02T20:25:26.51Z",
              "lastModifiedDateTime": "2015-12-08T21:51:15.593Z"
           },
           "createdDateTime": "2015-12-02T20:25:26.51Z",
           "size": 83736,
           "photo": {
              "takenDateTime": "2013-04-17T14:32:26Z"
           },
           "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExNTAuMTE",
           "name": "elect-a.jpg",
           "@content.downloadUrl": "https://public-ch3302.files.1drv.com/y3mnrbLFOgJJ8JQA7Ots0pzvL0xHYJx9NQJylS6IoQqp5G2CIIG5IWCKT_ADdp035kbr3qEmz6Va5j8-NCplk4ZMG_cYipxUfhP-NNl-SjlKocwc7yDplc1qWEynHGm_lME_o98pKSxNg6sKbEphRPufHea_h7LU1XH2qkFEGOIZGHQlw_JmH9fvygq8_XY2iE-",
           "children@odata.context": "https://api.onedrive.com/v1.0/$metadata#drives('me')/items('75BFE374EBEB1211%21150')/children",
           "parentReference": {
              "id": "75BFE374EBEB1211!107",
              "driveId": "75bfe374ebeb1211",
              "path": "/drive/root:/ryan-test1"
           },
           "lastModifiedDateTime": "2015-12-08T21:51:15.593Z"
    }


@pytest.fixture
def revisions_list_metadata():
    return {
    }


class TestValidatePath:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_validate_v1_path_file(self, provider, file_root_parent_metadata):
        file_id = '75BFE374EBEB1211!150'
        file_id = '1234'

        good_url = provider.build_url(file_id)

        aiohttpretty.register_json_uri('GET', good_url, body=file_root_parent_metadata, status=200)

        wb_path_v1 = await provider.validate_v1_path('/' + file_id)

        assert str(wb_path_v1) == '/{}'.format(file_root_parent_metadata['name'])

        wb_path_v0 = await provider.validate_path('/' + file_id)

        assert str(wb_path_v0) == '/{}'.format(file_root_parent_metadata['name'])

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_base_has_id(self, provider, file_root_parent_metadata):
        file_id = '1234'
        file_name = 'elect-a.jpg'
        parent_id = '75BFE374EBEB1211!107'
        expected_path = WaterButlerPath('/' + file_name, [None, file_id])
        base_path = WaterButlerPath('/', [file_id])

        good_url = "https://api.onedrive.com/v1.0/drive/root%3A/{}/{}".format(file_name, file_name)
        aiohttpretty.register_json_uri('GET', good_url, body=file_root_parent_metadata, status=200)

        good_url = "https://api.onedrive.com/v1.0/drive/items/{}".format(parent_id)
        aiohttpretty.register_json_uri('GET', good_url, body=file_root_parent_metadata, status=200)

        good_url = "https://api.onedrive.com/v1.0/drive/items/{}".format(file_id)
        aiohttpretty.register_json_uri('GET', good_url, body=file_root_parent_metadata, status=200)

        actual_path = await provider.revalidate_path(base_path, file_name, False)

        assert actual_path == expected_path


    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_no_child_folders_sub_folder(self, provider, file_root_parent_metadata):
        file_id = '1234'
        file_name = 'elect-a.jpg'
        parent_id = '75BFE374EBEB1211!107'
        expected_path = WaterButlerPath('/' + file_name, [None, file_id])
        base_path = WaterButlerPath('/', prepend=parent_id)

        good_url = "https://api.onedrive.com/v1.0/drive/root%3A/{}/{}".format(file_name, file_name)
        aiohttpretty.register_json_uri('GET', good_url, body=file_root_parent_metadata, status=200)

        good_url = "https://api.onedrive.com/v1.0/drive/items/{}".format(parent_id)
        aiohttpretty.register_json_uri('GET', good_url, body=file_root_parent_metadata, status=200)

        actual_path = await provider.revalidate_path(base_path, file_name, False)

        assert actual_path == expected_path


    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_revalidate_path_has_child_folders(self, provider, folder_object_metadata):
        file_id = '1234'
        file_name = 'elect-a.jpg'
        parent_id = '75BFE374EBEB1211!107'
        base_path = WaterButlerPath('/sub1-b', prepend=parent_id)
        expected_path = WaterButlerPath('/sub1-b/' + file_name, [None, None, file_id])

        good_url = provider._build_root_url('drive/root:', 'ryan-test1', 'sub1-b', file_name)
        aiohttpretty.register_json_uri('GET', good_url, body=folder_object_metadata, status=200)

        good_url = "https://api.onedrive.com/v1.0/drive/items/{}".format(parent_id)
        aiohttpretty.register_json_uri('GET', good_url, body=folder_object_metadata, status=200)

        assert '/sub1-b/' + file_name == str(expected_path)


class TestMoveOperations:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_rename_file(self, provider, folder_object_metadata, folder_list_metadata):
#         dest_path::WaterButlerPath('/elect-b.jpg', prepend='75BFE374EBEB1211!128') srcpath:WaterButlerPath('/75BFE374EBEB1211!132', prepend='75BFE374EBEB1211!128')
        dest_path = WaterButlerPath('/elect-b.jpg', [None, '1234!1'])
        src_path = WaterButlerPath('/elect-c.jpg', [None, '1234!1'])

#         logger.info('test_metadata path:{} provider.folder:{} provider:'.format(repr(path), repr(provider.folder), repr(provider)))

        list_url = provider.build_url('1234!1')

        aiohttpretty.register_json_uri('PATCH', list_url, body=folder_object_metadata)

        result = await provider.intra_move(provider, src_path, dest_path)

        assert result is not None

class TestMetadata:

    @pytest.mark.aiohttpretty
    @pytest.mark.asyncio
    async def test_metadata_root(self, provider, folder_object_metadata, folder_list_metadata):
        path = WaterButlerPath('/0/', _ids=(0, ))
        logger.info('test_metadata path:{} provider.folder:{} provider:'.format(repr(path), repr(provider.folder), repr(provider)))

        list_url = provider.build_url('root', expand='children')

        aiohttpretty.register_json_uri('GET', list_url, body=folder_list_metadata)

        result = await provider.metadata(path)

        assert len(result) == 3

    @pytest.mark.aiohttpretty
    def test_metadata_file_root_parent_names(self, provider, folder_object_metadata, file_root_parent_metadata):
        result = provider._get_names(file_root_parent_metadata)

        assert result == '/elect-a.jpg'

    @pytest.mark.aiohttpretty
    def test_metadata_ids_padding(self, provider, folder_object_metadata, file_sub_folder_metadata):
        result = provider._get_ids(file_sub_folder_metadata)
        assert result == [None, None, None, file_sub_folder_metadata['parentReference']['id'], file_sub_folder_metadata['id']]

    @pytest.mark.aiohttpretty
    def test_metadata_ids_no_padding(self, provider, folder_object_metadata, file_root_folder_metadata):
        result = provider._get_ids(file_root_folder_metadata)
        assert result == [None, file_root_folder_metadata['parentReference']['id'], file_root_folder_metadata['id']]

    @pytest.mark.aiohttpretty
    def test_metadata_folder_ids_padding(self, provider, folder_sub_folder_metadata):
        result = provider._get_ids(folder_sub_folder_metadata)
        assert result == [None, None, None, folder_sub_folder_metadata['parentReference']['id'], folder_sub_folder_metadata['id']]

