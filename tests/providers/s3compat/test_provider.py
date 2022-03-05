import os
import io
import xml
import json
import time
import base64
import hashlib
import aiohttpretty
from http import client
from urllib import parse
from unittest import mock

import pytest
from boto.compat import BytesIO
from boto.utils import compute_md5

from waterbutler.core import streams, metadata, exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.providers.s3compat import S3CompatProvider
from waterbutler.providers.s3compat import settings as pd_settings

from tests.utils import MockCoroutine
from collections import OrderedDict
from waterbutler.providers.s3compat.metadata import (S3CompatRevision,
                                                     S3CompatFileMetadata,
                                                     S3CompatFolderMetadata,
                                                     S3CompatFolderKeyMetadata,
                                                     S3CompatFileMetadataHeaders,
                                                     )


@pytest.fixture
def base_prefix():
    return ''


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {
        'host': 'Target Host',
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
    return S3CompatProvider(auth, credentials, settings)


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
    return S3CompatFileMetadataHeaders('test-path', file_header_metadata)


@pytest.fixture
def file_metadata_object():
    content = OrderedDict(Key='my-image.jpg',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag="fba9dede5f27731c9771645a39863328",
                          Size='434234',
                          StorageClass='STANDARD')

    return S3CompatFileMetadata(content)


@pytest.fixture
def folder_key_metadata_object():
    content = OrderedDict(Key='naptime/',
                          LastModified='2009-10-12T17:50:30.000Z',
                          ETag='"fba9dede5f27731c9771645a39863328"',
                          Size='0',
                          StorageClass='STANDARD')

    return S3CompatFolderKeyMetadata(content)


@pytest.fixture
def folder_metadata_object():
    content = OrderedDict(Prefix='photos/')
    return S3CompatFolderMetadata(content)


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

    return S3CompatRevision(content)


@pytest.fixture
def single_version_metadata():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
        <Name>bucket</Name>
        <Prefix>my</Prefix>
        <KeyMarker/>
        <VersionIdMarker/>
        <MaxKeys>5</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Version>
            <Key>single-version.file</Key>
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
    </ListVersionsResult>'''


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


@pytest.fixture
def folder_and_contents(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>{prefix}thisfolder/</Key>
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
                <Key>{prefix}thisfolder/item1</Key>
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
                <Key>{prefix}thisfolder/item2</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_empty_metadata():
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
        </ListBucketResult>'''


@pytest.fixture
def folder_item_metadata(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>{prefix}naptime/</Key>
                <LastModified>2009-10-12T17:50:30.000Z</LastModified>
                <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                <Size>0</Size>
                <StorageClass>STANDARD</StorageClass>
                <Owner>
                    <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                    <DisplayName>mtd@amazon.com</DisplayName>
                </Owner>
            </Contents>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_metadata(base_prefix):
    return '''<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>bucket</Name>
            <Prefix/>
            <Marker/>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>{prefix}my-image.jpg</Key>
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
                <Key>{prefix}my-third-image.jpg</Key>
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
                <Prefix>{prefix}   photos/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def folder_single_item_metadata(base_prefix):
    return'''<?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Name>bucket</Name>
        <Prefix/>
        <Marker/>
        <MaxKeys>1000</MaxKeys>
        <IsTruncated>false</IsTruncated>
        <Contents>
            <Key>{prefix}my-image.jpg</Key>
            <LastModified>2009-10-12T17:50:30.000Z</LastModified>
            <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
            <Size>434234</Size>
            <StorageClass>STANDARD</StorageClass>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>mtd@amazon.com</DisplayName>
            </Owner>
        </Contents>
        <CommonPrefixes>
            <Prefix>{prefix}   photos/</Prefix>
        </CommonPrefixes>
    </ListBucketResult>'''.format(prefix=base_prefix)


@pytest.fixture
def complete_upload_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
        <Bucket>Example-Bucket</Bucket>
        <Key>Example-Object</Key>
        <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
    </CompleteMultipartUploadResult>'''


@pytest.fixture
def create_session_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
       <Bucket>example-bucket</Bucket>
       <Key>example-object</Key>
       <UploadId>EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-</UploadId>
    </InitiateMultipartUploadResult>'''


@pytest.fixture
def generic_http_403_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
        <Code>AccessDenied</Code>
        <Message>Access Denied</Message>
        <RequestId>656c76696e6727732072657175657374</RequestId>
        <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
    </Error>'''


@pytest.fixture
def generic_http_404_resp():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <Error>
        <Code>NotFound</Code>
        <Message>Not Found</Message>
        <RequestId>656c76696e6727732072657175657374</RequestId>
        <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
    </Error>'''


@pytest.fixture
def list_parts_resp_empty():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Bucket>example-bucket</Bucket>
        <Key>example-object</Key>
        <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
        <Initiator>
            <ID>arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx</ID>
            <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-b288870f11xx</DisplayName>
        </Initiator>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>someName</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
    </ListPartsResult>'''


@pytest.fixture
def list_parts_resp_not_empty():
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Bucket>example-bucket</Bucket>
        <Key>example-object</Key>
        <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
        <Initiator>
            <ID>arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx</ID>
            <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-b288870f11xx</DisplayName>
        </Initiator>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>someName</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
        <PartNumberMarker>1</PartNumberMarker>
        <NextPartNumberMarker>3</NextPartNumberMarker>
        <MaxParts>2</MaxParts>
        <IsTruncated>true</IsTruncated>
        <Part>
            <PartNumber>2</PartNumber>
            <LastModified>2010-11-10T20:48:34.000Z</LastModified>
            <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
            <Size>10485760</Size>
        </Part>
        <Part>
            <PartNumber>3</PartNumber>
            <LastModified>2010-11-10T20:48:33.000Z</LastModified>
            <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
            <Size>10485760</Size>
        </Part>
    </ListPartsResult>'''


@pytest.fixture
def upload_parts_headers_list():
    return '''{
        "headers_list": [
            {
                "x-amz-id-2": "Vvag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==",
                "x-amz-request-id": "656c76696e6727732072657175657374",
                "Date": "Mon, 1 Nov 2010 20:34:54 GMT",
                "ETag": "b54357faf0632cce46e942fa68356b38",
                "Content-Length": "0",
                "Connection": "keep-alive",
                "Server": "AmazonS3"
            },
            {
                "x-amz-id-2": "imru9pO4ZVKnJ2Qz7Vvag1LuByRx9e6j5On/CAtRPfTaOFg1NPcfTW==",
                "x-amz-request-id": "732072657175657374656c76696e6727",
                "Date": "Mon, 1 Nov 2010 20:35:55 GMT",
                "ETag": "46e942fa68356b38b54357faf0632cce",
                "Content-Length": "0",
                "Connection": "keep-alive",
                "Server": "AmazonS3"
            },
            {
                "x-amz-id-2": "yRx9e6j5Onimru9pOVvag1LuB4ZVKnJ2Qz7/cfTWAtRPf1NPTaOFg==",
                "x-amz-request-id": "67277320726571656c76696e75657374",
                "Date": "Mon, 1 Nov 2010 20:36:56 GMT",
                "ETag": "af0632cce46e942fab54357f68356b38",
                "Content-Length": "0",
                "Connection": "keep-alive",
                "Server": "AmazonS3"
            }
        ]
    }'''


def location_response(location):
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{location}</LocationConstraint>
    '''.format(location=location)


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
    prefix = path.full_path.lstrip('/')
    return {'prefix': prefix, 'delimiter': '/'}


def list_upload_chunks_body(parts_metadata):
    payload = '''<?xml version="1.0" encoding="UTF-8"?>
        <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Bucket>example-bucket</Bucket>
            <Key>example-object</Key>
            <UploadId>XXBsb2FkIElEIGZvciBlbHZpbmcncyVcdS1tb3ZpZS5tMnRzEEEwbG9hZA</UploadId>
            <Initiator>
                <ID>arn:aws:iam::111122223333:user/some-user-11116a31-17b5-4fb7-9df5-b288870f11xx</ID>
                <DisplayName>umat-user-11116a31-17b5-4fb7-9df5-b288870f11xx</DisplayName>
            </Initiator>
            <Owner>
                <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
                <DisplayName>someName</DisplayName>
            </Owner>
            <StorageClass>STANDARD</StorageClass>
            <PartNumberMarker>1</PartNumberMarker>
            <NextPartNumberMarker>3</NextPartNumberMarker>
            <MaxParts>2</MaxParts>
            <IsTruncated>false</IsTruncated>
            <Part>
                <PartNumber>2</PartNumber>
                <LastModified>2010-11-10T20:48:34.000Z</LastModified>
                <ETag>"7778aef83f66abc1fa1e8477f296d394"</ETag>
                <Size>10485760</Size>
            </Part>
            <Part>
                <PartNumber>3</PartNumber>
                <LastModified>2010-11-10T20:48:33.000Z</LastModified>
                <ETag>"aaaa18db4cc2f85cedef654fccc4a4x8"</ETag>
                <Size>10485760</Size>
            </Part>
        </ListPartsResult>
    '''.encode('utf-8')

    md5 = compute_md5(BytesIO(payload))

    headers = {
        'Content-Length': str(len(payload)),
        'Content-MD5': md5[1],
        'Content-Type': 'text/xml',
    }

    return payload, headers


class TestProviderConstruction:

    def test_https(self, auth, credentials, settings):
        provider = S3CompatProvider(auth, {'host': 'securehost',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert provider.connection.is_secure
        assert provider.connection.host == 'securehost'
        assert provider.connection.port == 443

        provider = S3CompatProvider(auth, {'host': 'securehost:443',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert provider.connection.is_secure
        assert provider.connection.host == 'securehost'
        assert provider.connection.port == 443

    def test_http(self, auth, credentials, settings):
        provider = S3CompatProvider(auth, {'host': 'normalhost:80',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert not provider.connection.is_secure
        assert provider.connection.host == 'normalhost'
        assert provider.connection.port == 80

        provider = S3CompatProvider(auth, {'host': 'normalhost:8080',
                                           'access_key': 'a',
                                           'secret_key': 's'}, settings)
        assert not provider.connection.is_secure
        assert provider.connection.host == 'normalhost'
        assert provider.connection.port == 8080


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, file_header_metadata, mock_time):
        file_path = 'foobah'
        full_path = file_path
        prefix = provider.prefix
        if prefix:
            full_path = prefix + full_path
        params_for_dir = {'prefix': full_path + '/', 'delimiter': '/'}
        good_metadata_url = provider.bucket.new_key(full_path).generate_url(100, 'HEAD')
        bad_metadata_url = provider.bucket.generate_url(100)
        aiohttpretty.register_uri('HEAD', good_metadata_url, headers=file_header_metadata)
        aiohttpretty.register_uri('GET', bad_metadata_url, params=params_for_dir, status=404)

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
        full_path = folder_path
        prefix = provider.prefix
        if prefix:
            full_path = prefix + full_path

        params_for_dir = {'prefix': full_path + '/', 'delimiter': '/'}
        good_metadata_url = provider.bucket.generate_url(100)
        bad_metadata_url = provider.bucket.new_key(full_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'GET', good_metadata_url, params=params_for_dir,
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
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)
        response_headers = {'response-content-disposition': 'attachment'}
        url = provider.bucket.new_key(path.full_path).generate_url(100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', url[:url.index('?')], body=b'delicious', auto_length=True)

        result = await provider.download(path)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)
        response_headers = {'response-content-disposition': 'attachment;'}
        url = provider.bucket.new_key(path.full_path).generate_url(100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', url[:url.index('?')], body=b'de', auto_length=True, status=206)

        result = await provider.download(path, range=(0, 1))
        assert result.partial
        content = await result.read()
        assert content == b'de'
        assert aiohttpretty.has_call(method='GET', uri=url[:url.index('?')])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_version(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)
        url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            query_parameters={'versionId': 'someversion'},
            response_headers={'response-content-disposition': 'attachment'},
        )
        aiohttpretty.register_uri('GET', url[:url.index('?')], body=b'delicious', auto_length=True)

        result = await provider.download(path, version='someversion')
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    @pytest.mark.parametrize("display_name_arg,expected_name", [
        ('meow.txt', 'meow.txt'),
        ('',         'muhtriangle'),
        (None,       'muhtriangle'),
    ])
    async def test_download_with_display_name(self, provider, mock_time, display_name_arg, expected_name):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)
        response_headers = {
            'response-content-disposition':
            'attachment; filename="{}"; filename*=UTF-8''{}'.format(expected_name, expected_name)
        }
        url = provider.bucket.new_key(path.full_path).generate_url(100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', url[:url.index('?')], body=b'delicious', auto_length=True)

        result = await provider.download(path, display_name=display_name_arg)
        content = await result.read()

        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_not_found(self, provider, mock_time):
        path = WaterButlerPath('/muhtriangle', prepend=provider.prefix)
        response_headers = {'response-content-disposition': 'attachment'}
        url = provider.bucket.new_key(path.full_path).generate_url(100, response_headers=response_headers)
        aiohttpretty.register_uri('GET', url[:url.index('?')], status=404)

        with pytest.raises(exceptions.DownloadError):
            await provider.download(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_folder_400s(self, provider, mock_time):
        with pytest.raises(exceptions.DownloadError) as e:
            await provider.download(WaterButlerPath('/cool/folder/mom/', prepend=provider.prefix))
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, file_content, file_stream, file_header_metadata, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
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
    async def test_upload_encrypted(self, provider, file_content, file_stream, file_header_metadata, mock_time):
        # Set trigger for encrypt_key=True in s3compat.provider.upload
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'PUT', encrypt_key=True)
        metadata_url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri(
            'HEAD',
            metadata_url,
            responses=[
                {'status': 404},
                {'headers': file_header_metadata},
            ],
        )
        headers = {'ETag': '"{}"'.format(content_md5)}
        aiohttpretty.register_uri('PUT', url, status=200, headers=headers)

        metadata, created = await provider.upload(file_stream, path)

        assert metadata.kind == 'file'
        assert metadata.extra['encryption'] == 'AES256'
        assert created
        assert aiohttpretty.has_call(method='PUT', uri=url)
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_chunked(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._chunked_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._chunked_upload.assert_called_with(file_stream, path)

        # Fixtures are shared between tests. Need to revert the settings back.
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete(self, provider, upload_parts_headers_list, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]

        provider.metadata = MockCoroutine()
        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.return_value = headers_list
        provider._complete_multipart_upload = MockCoroutine()

        await provider._chunked_upload(file_stream, path)

        provider._create_upload_session.assert_called_with(path)
        provider._upload_parts.assert_called_with(file_stream, path, upload_id)
        provider._complete_multipart_upload.assert_called_with(path, upload_id, headers_list)


    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_aborted_success(self, provider, upload_parts_headers_list, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 5
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]

        provider.metadata = MockCoroutine()
        provider._create_upload_session = MockCoroutine()
        provider._create_upload_session.return_value = upload_id
        provider._upload_parts = MockCoroutine()
        provider._upload_parts.return_value = headers_list
        provider._upload_part = MockCoroutine()
        provider._upload_part.side_effect = Exception('error')
        provider._abort_chunked_upload = MockCoroutine()
        provider._abort_chunked_upload.return_value = True

        with pytest.raises(exceptions.UploadError) as exc:
            await provider._chunked_upload(file_stream, path)
        msg = 'An unexpected error has occurred during the multi-part upload.'
        msg += '  The abort action failed to clean up the temporary file parts generated ' \
               'during the upload process.  Please manually remove them.'
        assert str(exc.value) == ', '.join(['500', msg])

        provider._create_upload_session.assert_called_with(path)
        provider._upload_parts.assert_called_with(file_stream, path, upload_id)
        provider._abort_chunked_upload.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_limit_contiguous(self, provider, file_stream, mock_time):
        assert file_stream.size == 6
        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = 10
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        provider._contiguous_upload = MockCoroutine()
        provider.metadata = MockCoroutine()

        await provider.upload(file_stream, path)

        provider._contiguous_upload.assert_called_with(file_stream, path)

        provider.CONTIGUOUS_UPLOAD_SIZE_LIMIT = pd_settings.CONTIGUOUS_UPLOAD_SIZE_LIMIT
        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_no_encryption(self, provider, create_session_resp, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        init_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'POST',
            query_parameters={'uploads': ''},
        )

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_create_upload_session_with_encryption(self, provider,
                                                                        create_session_resp,
                                                                        mock_time):
        provider.encrypt_uploads = True
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        init_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'POST',
            query_parameters={'uploads': ''},
            encrypt_key=True
        )

        aiohttpretty.register_uri('POST', init_url, body=create_session_resp, status=200)

        session_id = await provider._create_upload_session(path)
        expected_session_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                              '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        assert aiohttpretty.has_call(method='POST', uri=init_url)
        assert session_id is not None
        assert session_id == expected_session_id

        provider.encrypt_uploads = False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts(self, provider, file_stream,
                                               upload_parts_headers_list):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        parts_metadata = await provider._upload_parts(file_stream, path, upload_id)

        assert provider._upload_part.call_count == 3
        assert len(parts_metadata) == 3
        assert parts_metadata == side_effect

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_parts_remainder(self, provider,
                                                         upload_parts_headers_list):

        file_stream = streams.StringStream('abcdefghijklmnopqrst')
        assert file_stream.size == 20
        provider.CHUNK_SIZE = 9

        side_effect = json.loads(upload_parts_headers_list).get('headers_list')
        assert len(side_effect) == 3

        provider._upload_part = MockCoroutine(side_effect=side_effect)
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'

        parts_metadata = await provider._upload_parts(file_stream, path, upload_id)

        assert provider._upload_part.call_count == 3
        provider._upload_part.assert_has_calls([
            mock.call(file_stream, path, upload_id, 1, 9),
            mock.call(file_stream, path, upload_id, 2, 9),
            mock.call(file_stream, path, upload_id, 3, 2),
        ])
        assert len(parts_metadata) == 3
        assert parts_metadata == side_effect

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_upload_part(self, provider, file_stream,
                                              upload_parts_headers_list,
                                              mock_time):
        assert file_stream.size == 6
        provider.CHUNK_SIZE = 2

        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        chunk_number = 1
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {
            'partNumber': str(chunk_number),
            'uploadId': upload_id,
        }
        headers = {'Content-Length': str(provider.CHUNK_SIZE)}
        upload_part_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'PUT',
            query_parameters=params,
            headers=headers
        )
        # aiohttp resp headers use upper case
        part_headers = json.loads(upload_parts_headers_list).get('headers_list')[0]
        part_headers = {k.upper(): v for k, v in part_headers.items()}
        aiohttpretty.register_uri('PUT', upload_part_url, status=200, headers=part_headers)

        part_metadata = await provider._upload_part(file_stream, path, upload_id, chunk_number,
                                                    provider.CHUNK_SIZE)

        assert aiohttpretty.has_call(method='PUT', uri=upload_part_url)
        assert part_headers == part_metadata

        provider.CHUNK_SIZE = pd_settings.CHUNK_SIZE

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_chunked_upload_complete_multipart_upload(self, provider,
                                                            upload_parts_headers_list,
                                                            complete_upload_resp, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        params = {'uploadId': upload_id}
        payload = '<?xml version="1.0" encoding="UTF-8"?>'
        payload += '<CompleteMultipartUpload>'
        # aiohttp resp headers are upper case
        headers_list = json.loads(upload_parts_headers_list).get('headers_list')
        headers_list = [{k.upper(): v for k, v in headers.items()} for headers in headers_list]
        for i, part in enumerate(headers_list):
            payload += '<Part>'
            payload += '<PartNumber>{}</PartNumber>'.format(i+1)  # part number must be >= 1
            payload += '<ETag>{}</ETag>'.format(xml.sax.saxutils.escape(part['ETAG']))
            payload += '</Part>'
        payload += '</CompleteMultipartUpload>'
        payload = payload.encode('utf-8')

        headers = {
            'Content-Length': str(len(payload)),
            'Content-MD5': compute_md5(BytesIO(payload))[1],
            'Content-Type': 'text/xml',
        }

        complete_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'POST',
            headers=headers,
            query_parameters=params
        )

        aiohttpretty.register_uri(
            'POST',
            complete_url,
            status=200,
            body=complete_upload_resp
        )

        await provider._complete_multipart_upload(path, upload_id, headers_list)

        assert aiohttpretty.has_call(method='POST', uri=complete_url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_session_deleted(self, provider, generic_http_404_resp,
                                                        mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'DELETE',
            query_parameters={'uploadId': upload_id}
        )
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_empty(self, provider, list_parts_resp_empty,
                                                   mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'DELETE',
            query_parameters={'uploadId': upload_id}
        )
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert aborted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_list_not_empty(self, provider, list_parts_resp_not_empty, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'DELETE',
            query_parameters={'uploadId': upload_id}
        )
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200)

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_abort_chunked_upload_exception(self, provider, upload_parts_headers_list, file_stream, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        abort_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'DELETE',
            query_parameters={'uploadId': upload_id}
        )
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('DELETE', abort_url, status=204)
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200)
        provider._list_uploaded_chunks = MockCoroutine()
        provider._list_uploaded_chunks.side_effect = Exception('error')

        aborted = await provider._abort_chunked_upload(path, upload_id)

        assert aiohttpretty.has_call(method='DELETE', uri=abort_url)
        assert aborted is False
        provider._list_uploaded_chunks.assert_called_with(path, upload_id)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_session_not_found(self,
                                                          provider,
                                                          generic_http_404_resp,
                                                          mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('GET', list_url, body=generic_http_404_resp, status=404)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_empty_list(self,
                                                   provider,
                                                   list_parts_resp_empty,
                                                   mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_empty, status=200)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_list_uploaded_chunks_list_not_empty(self,
                                                       provider,
                                                       list_parts_resp_not_empty,
                                                       mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        upload_id = 'EXAMPLEJZ6e0YupT2h66iePQCc9IEbYbDUy4RTpMeoSMLPRp8Z5o1u' \
                    '8feSRonpvnWsKKG35tI2LB9VDPiCgTy.Gq2VxQLYjrue4Nq.NBdqI-'
        list_url = provider.bucket.new_key(path.full_path).generate_url(
            100,
            'GET',
            query_parameters={'uploadId': upload_id}
        )
        aiohttpretty.register_uri('GET', list_url, body=list_parts_resp_not_empty, status=200)

        resp_xml, session_deleted = await provider._list_uploaded_chunks(path, upload_id)

        assert aiohttpretty.has_call(method='GET', uri=list_url)
        assert resp_xml is not None
        assert session_deleted is False

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, mock_time):
        path = WaterButlerPath('/some-file', prepend=provider.prefix)
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'DELETE')
        aiohttpretty.register_uri('DELETE', url, status=200)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_confirm_delete(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/', prepend=provider.prefix)

        params = {'prefix': path.full_path.lstrip('/')}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=folder_and_contents,
            status=200,
        )

        target_items = ['thisfolder/', 'thisfolder/item1', 'thisfolder/item2']
        delete_urls = []
        prefix = provider.prefix
        if prefix is None:
            prefix = ''
        for i in target_items:
            delete_url = provider.bucket.new_key(prefix + i).generate_url(
                100,
                'DELETE',
            )
            delete_urls.append(delete_url)
            aiohttpretty.register_uri('DELETE', delete_url, status=204)

        with pytest.raises(exceptions.DeleteError):
            await provider.delete(path)

        await provider.delete(path, confirm_delete=1)

        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params)
        for delete_url in delete_urls:
            assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_delete(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/some-folder/', prepend=provider.prefix)

        params = {'prefix': path.full_path.lstrip('/')}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=folder_and_contents,
            status=200,
        )

        target_items = ['thisfolder/', 'thisfolder/item1', 'thisfolder/item2']
        delete_urls = []
        prefix = provider.prefix
        if prefix is None:
            prefix = ''
        for i in target_items:
            delete_url = provider.bucket.new_key(prefix + i).generate_url(
                100,
                'DELETE',
            )
            delete_urls.append(delete_url)
            aiohttpretty.register_uri('DELETE', delete_url, status=204)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params)
        for delete_url in delete_urls:
            assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_item_folder_delete(self, provider, folder_single_item_metadata, mock_time):
        path = WaterButlerPath('/single-thing-folder/', prepend=provider.prefix)

        params = {'prefix': path.full_path.lstrip('/')}
        query_url = provider.bucket.generate_url(100, 'GET')
        aiohttpretty.register_uri(
            'GET',
            query_url,
            params=params,
            body=folder_single_item_metadata,
            status=200,
        )

        prefix = 'my-image.jpg'
        delete_url = provider.bucket.new_key(prefix).generate_url(
            100,
            'DELETE',
        )
        aiohttpretty.register_uri('DELETE', delete_url, status=204)

        await provider.delete(path)

        assert aiohttpretty.has_call(method='GET', uri=query_url, params=params)
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)
        (payload, headers) = bulk_delete_body(
            ['my-image.jpg']
        )


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder(self, provider, folder_metadata, mock_time):
        path = WaterButlerPath('/darp/', prepend=provider.prefix)
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
    async def test_metadata_folder_self_listing(self, provider, folder_and_contents, mock_time):
        path = WaterButlerPath('/thisfolder/', prepend=provider.prefix)
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_and_contents)

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 2
        for fobj in result:
            assert fobj.name != path.full_path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_just_a_folder_metadata_folder(self, provider, folder_item_metadata, mock_time):
        path = WaterButlerPath('/', prepend=provider.prefix)
        url = provider.bucket.generate_url(100)
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_item_metadata,
                                  headers={'Content-Type': 'application/xml'})

        result = await provider.metadata(path)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].kind == 'folder'

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_must_have_slash(self, provider, folder_item_metadata, mock_time):
    #     with pytest.raises(exceptions.InvalidPathError):
    #         await provider.metadata('')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_empty_metadata_folder(self, provider, folder_empty_metadata, mock_time):
        path = WaterButlerPath('/this-is-not-the-root/', prepend=provider.prefix)
        metadata_url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')

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
        path = WaterButlerPath('/Foo/Bar/my-image.jpg', prepend=provider.prefix)
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata)

        result = await provider.metadata(path)

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == '/' + path.path
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_lastest_revision(self, provider, file_header_metadata, mock_time):
        path = WaterButlerPath('/Foo/Bar/my-image.jpg', prepend=provider.prefix)
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, headers=file_header_metadata)

        result = await provider.metadata(path, revision='Latest')

        assert isinstance(result, metadata.BaseFileMetadata)
        assert result.path == '/' + path.path
        assert result.name == 'my-image.jpg'
        assert result.extra['md5'] == 'fba9dede5f27731c9771645a39863328'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_missing(self, provider, mock_time):
        path = WaterButlerPath('/notfound.txt', prepend=provider.prefix)
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', url, status=404)

        with pytest.raises(exceptions.MetadataError):
            await provider.metadata(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload(self, provider, file_content, file_stream, file_header_metadata, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        content_md5 = hashlib.md5(file_content).hexdigest()
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
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
    async def test_upload_checksum_mismatch(self, provider, file_stream, file_header_metadata, mock_time):
        path = WaterButlerPath('/foobah', prepend=provider.prefix)
        url = provider.bucket.new_key(path.full_path).generate_url(100, 'PUT')
        metadata_url = provider.bucket.new_key(path.full_path).generate_url(100, 'HEAD')
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
        path = WaterButlerPath('/alreadyexists/', prepend=provider.prefix)
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)
        aiohttpretty.register_uri('GET', url, params=params, body=folder_metadata,
                                  headers={'Content-Type': 'application/xml'})

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot create folder "alreadyexists", because a file or folder already exists with that name'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_start_with_slash(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists', prepend=provider.prefix)

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
        create_url = provider.bucket.new_key(path.full_path).generate_url(100, 'PUT')

        aiohttpretty.register_uri('GET', url, params=params, status=404)
        aiohttpretty.register_uri('PUT', create_url, status=403)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out_metadata(self, provider, mock_time):
        path = WaterButlerPath('/alreadyexists/', prepend=provider.prefix)
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)

        aiohttpretty.register_uri('GET', url, params=params, status=403)

        with pytest.raises(exceptions.MetadataError) as e:
            await provider.create_folder(path)

        assert e.value.code == 403

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_creates(self, provider, mock_time):
        path = WaterButlerPath('/doesntalreadyexists/', prepend=provider.prefix)
        url = provider.bucket.generate_url(100, 'GET')
        params = build_folder_params(path)
        create_url = provider.bucket.new_key(path.full_path).generate_url(100, 'PUT')

        aiohttpretty.register_uri('GET', url, params=params, status=404)
        aiohttpretty.register_uri('PUT', create_url, status=200)

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'doesntalreadyexists'
        assert resp.path == '/' + path.path


class TestOperations:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy(self, provider, file_header_metadata, mock_time):
        dest_path = WaterButlerPath('/dest', prepend=provider.prefix)
        source_path = WaterButlerPath('/source', prepend=provider.prefix)

        metadata_url = provider.bucket.new_key(dest_path.full_path).generate_url(100, 'HEAD')
        aiohttpretty.register_uri('HEAD', metadata_url, headers=file_header_metadata)

        header_path = '/' + os.path.join(provider.settings['bucket'], source_path.full_path)
        headers = {'x-amz-copy-source': parse.quote(header_path)}
        url = provider.bucket.new_key(dest_path.full_path).generate_url(100, 'PUT', headers=headers)
        aiohttpretty.register_uri('PUT', url, status=200)

        metadata, exists = await provider.intra_copy(provider, source_path, dest_path)

        assert metadata.kind == 'file'
        assert not exists
        assert aiohttpretty.has_call(method='HEAD', uri=metadata_url)
        assert aiohttpretty.has_call(method='PUT', uri=url, headers=headers)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_version_metadata(self, provider, version_metadata, mock_time):
        path = WaterButlerPath('/my-image.jpg', prepend=provider.prefix)
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

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_single_version_metadata(self, provider, single_version_metadata, mock_time):
        path = WaterButlerPath('/single-version.file', prepend=provider.prefix)
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

        file_path = WaterButlerPath('/my-image.jpg', prepend=provider.prefix)
        folder_path = WaterButlerPath('/folder/', folder=True, prepend=provider.prefix)

        assert provider.can_intra_move(provider)
        assert provider.can_intra_move(provider, file_path)
        assert not provider.can_intra_move(provider, folder_path)

    def test_can_intra_copy(self, provider):

        file_path = WaterButlerPath('/my-image.jpg', prepend=provider.prefix)
        folder_path = WaterButlerPath('/folder/', folder=True, prepend=provider.prefix)

        assert provider.can_intra_copy(provider)
        assert provider.can_intra_copy(provider, file_path)
        assert not provider.can_intra_copy(provider, folder_path)

    def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names()
