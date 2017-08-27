import io
import pytest

from waterbutler.core import streams
from waterbutler.providers.onedrive import OneDriveProvider


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
    return {'folder': '75BFE374EBEB1211!118'}


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
def folder_root_response():
    return {
        'size': 5264812,
        'folder': {'childCount': 3},
        'eTag': 'aOUI4M0ZGNDVGMzM4NkExMCExMDMuMA',
        'root': {},
        'lastModifiedDateTime': '2016-11-22T12:40:13.797Z',
        'id': '9B83FF45F3386A10!103',
        'createdDateTime': '2016-11-16T09:48:07.853Z',
        'webUrl': 'https://onedrive.live.com/?cid=9b83ff45f3386a10',
        'cTag': 'adDo5QjgzRkY0NUYzMzg2QTEwITEwMy42MzYxNTQxNTIxMzc5NzAwMDA',
        'name': 'root',
        'children': [{
            'size': 2632455,
            'folder': {'childCount': 2},
            'eTag': 'aOUI4M0ZGNDVGMzM4NkExMCExODMuMA',
            'lastModifiedDateTime': '2016-11-22T10:35:48.33Z',
            'id': '9B83FF45F3386A10!183',
            'createdDateTime': '2016-11-21T17:57:32.947Z',
            'webUrl': 'https://1drv.ms/f/s!ABBqOPNF_4ObgTc',
            'cTag': 'adDo5QjgzRkY0NUYzMzg2QTEwITE4My42MzYxNTQwNzc0ODMzMDAwMDA',
            'name': 'test6',
            'parentReference': {
                'path': '/drive/root:',
                'driveId': '9b83ff45f3386a10',
                'id': '9B83FF45F3386A10!103'}
        }, {
            'size': 98,
            'eTag': 'aOUI4M0ZGNDVGMzM4NkExMCExMTMuMA',
            'lastModifiedDateTime': '2016-11-16T11:17:20.957Z',
            'id': '9B83FF45F3386A10!113',
            'createdDateTime': '2016-11-16T11:17:20.957Z',
            'file': {
                'mimeType': 'application/zip',
                'hashes': {'crc32Hash': 'A2BF6316',
                           'sha1Hash': 'C8D9E56F12E09FD2630F0D1F315B7AB196C43A05'}
            },
            'webUrl': 'https://1drv.ms/u/s!ABBqOPNF_4ObcQ',
            'cTag': 'aYzo5QjgzRkY0NUYzMzg2QTEwITExMy4yNTc',
            'name': 'googledrive-archive.zip',
            '@content.downloadUrl': 'https://public-dm2306.files.1drv.com/y3mxEQECWSCZVCfSRkI0ZcHNeD9VUVLWwJbC1qDHr-WR9eD_Vwi2Cwc-IYx06YSVBNjroUwZYTQmXd6mXRI-Vt-zIQwFsp7EvPi9edvuRisa-p9RMpedZ22V6HjS8qJi88nGebzOC6WG--nweas9HpC4iAxqFQKcgMIPHMmfmC1aVuPxGYiHcbc1Nseg7yAC7Dvh2HxMQ8-Bm4lLVZC92fZs3idbXkk9S_eBitB5RaIQHY',
            'parentReference': {
                'path': '/drive/root:',
                'driveId': '9b83ff45f3386a10',
                'id': '9B83FF45F3386A10!103'
            }
        }
        ],
        'children@odata.context': "https://api.onedrive.com/v1.0/$metadata#drives('me')/items('9B83FF45F3386A10%21103')/children",
        '@odata.context': "https://api.onedrive.com/v1.0/$metadata#drives('me')/items/$entity"
    }


@pytest.fixture
def folder_sub_sub_response():
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
def folder_sub_response():
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
def file_sub_folder_response():
    return {
        "id": "75BFE374EBEB1211!128",
        "cTag": "adDo3NUJGRTM3NEVCRUIxMjExITEyOC42MzU4NTYxODI2MDA5MzAwMDA",
        "eTag": "aNzVCRkUzNzRFQkVCMTIxMSExMjguMA",
        "size": 998322,
        "name": "hello.jpg",
        "parentReference": {
            "id": "75BFE374EBEB1211!103",
            "path": "/drive/root:/ryan-test1",
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
def not_found_error_response():
    return {
        "error": {
            "code": "itemNotFound",
            "message": "The resource could not be found."
        }
    }


@pytest.fixture
def file_root_response():
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
def file_sub_response():
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
def file_rename_sub_response(file_sub_response):
    new = file_sub_response.copy()
    new['name'] = 'elect-a (1).jpg'
    new['id'] = '66BFE321EBEB1211!150'
    return new


@pytest.fixture
def create_upload_session_response():
    return {
        "uploadUrl": "https://any.url/up/fe688",
        "expirationDateTime": "2015-01-29T09:21:55.523Z",
        "nextExpectedRanges": ["0-"]
    }


@pytest.fixture
def revisions_list_metadata():
    return {
    }
