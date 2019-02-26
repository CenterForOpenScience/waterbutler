import json
import time
from unittest import mock
from urllib.parse import quote, unquote

import furl
import pytest
from multidict import MultiDictProxy

from tests.providers.googlecloud.fixtures.files import (file_wb_path,
                                                        meta_file_raw,
                                                        file_obj_name,
                                                        file_2_obj_name,
                                                        file_2_copy_obj_name)

from tests.providers.googlecloud.fixtures.providers import (mock_auth,
                                                            mock_auth_2,
                                                            mock_creds,
                                                            mock_creds_2,
                                                            mock_settings,
                                                            mock_settings_2)

from tests.providers.googlecloud.fixtures.folders import folder_obj_name, folder_wb_path

from waterbutler.providers.googlecloud import utils
from waterbutler.providers.googlecloud import settings
from waterbutler.providers.googlecloud import GoogleCloudProvider


@pytest.fixture()
def mock_provider(mock_auth, mock_creds, mock_settings):
    return GoogleCloudProvider(mock_auth, mock_creds, mock_settings)


@pytest.fixture
def mock_time(monkeypatch):
    mock_time = mock.Mock(return_value=1234567890.0)
    monkeypatch.setattr(time, 'time', mock_time)


@pytest.fixture
def expires():
    return 1234567890 + settings.SIGNATURE_EXPIRATION


class TestPathAndNameForObjects:
    """Test that the object name and Waterbutler Path can be converted to each other correctly.

    Google Cloud uses "Object Name" as an identifier to refer to objects (files and folders) in URL
    path and request headers. Make sure that:
    1. For both files and folders, it never starts with a '/'
    2. For files, it does not end with a '/'
    3. For folders, it does end with a '/'

    For ``WaterButlerPath``, ``generic_path_validation()`` expects one and only one leading `/`.
    """

    def test_path_and_obj_name_for_file(self, file_obj_name, file_wb_path):

        object_name = utils.get_obj_name(file_wb_path)
        assert object_name == file_obj_name

        path = utils.build_path(file_obj_name)
        assert path == '/' + file_wb_path.path

    def test_path_and_obj_name_for_folder(self, folder_obj_name, folder_wb_path):

        object_name = utils.get_obj_name(folder_wb_path, is_folder=True)
        assert object_name == folder_obj_name

        path = utils.build_path(folder_obj_name, is_folder=True)
        assert path == '/' + folder_wb_path.path


class TestBuildAndSignURL:
    """Test that URL are correctly built and signed.
    """

    def test_build_canonical_ext_headers_str(self, mock_provider, file_2_obj_name):

        object_name_with_bucket = '{}/{}'.format(mock_provider.bucket, file_2_obj_name)
        canonical_ext_headers = {'x-goog-copy-source': object_name_with_bucket}
        canonical_ext_headers_str = utils.build_canonical_ext_headers_str(canonical_ext_headers)
        expected = 'x-goog-copy-source:{}\n'.format(object_name_with_bucket)
        assert canonical_ext_headers_str == expected

    def test_build_and_sign_metadata_url(self, mock_time, expires, mock_provider, file_2_obj_name):
        signed_url = mock_provider._build_and_sign_url('HEAD', file_2_obj_name, **{})
        url = furl.furl(signed_url)

        assert '{}://{}'.format(url.scheme, url.host) == mock_provider.BASE_URL
        assert url.path == '/{}/{}'.format(mock_provider.bucket, quote(file_2_obj_name, safe=''))
        assert int(url.args.get('Expires')) == expires
        assert url.args.get('GoogleAccessId') == mock_provider.creds.service_account_email
        assert url.args.get('Signature') == unquote(
            'ndhn7AZQQvy1cyriphHgA3DEQeShTbCTnqTMyRK2X5eOsmCQ2YQBnAIC7dfvLgA%2Bx0h57UAXY88gNdXo1Qlm'
            'DNMUg9uxWE%2BGPP9eYau%2BQbZImnvYeFEzdSze9I2bgwvYjk1uE8cZYymj74stt58FX30uD1SUnNa4gyJiCY'
            '1FpAfaMV0VrWjrjNu2z0PB6mQiGrpbZ4xMLvbMuImaVWDYbYhUXocRzSXdp%2BqOe1f9UnqYxYnBQB1JhNiKxR'
            'WiQJ8RlZvXmwDYkfzpT4OUCf6oarDh14x05OSd2LP4QmJ1LP466fLJgvOqNTBx9KFkZQx%2FLZS%2F22JY0T2d'
            'JbQ8at8IuQ%3D%3D'
        )

    def test_build_and_sign_upload_url(self, mock_time, expires, mock_provider, file_2_obj_name):
        signed_url = mock_provider._build_and_sign_url('PUT', file_2_obj_name, **{})
        url = furl.furl(signed_url)

        assert '{}://{}'.format(url.scheme, url.host) == mock_provider.BASE_URL
        assert url.path == '/{}/{}'.format(mock_provider.bucket, quote(file_2_obj_name, safe=''))
        assert int(url.args.get('Expires')) == expires
        assert url.args.get('GoogleAccessId') == mock_provider.creds.service_account_email
        assert url.args.get('Signature') == unquote(
            'adVO2pVvFmUdS824inIBBzly63m5gbBYKP%2FAs910n%2FyX7zPTncyCQBKP9lq6nB%2BDnEvd5Pv5l9rjtMuX'
            'uITvv7nWdCEKMn0Kl%2FR3FWr2a%2Bs5Zdqv9TW2o4WV4EyMHOAzTWSVFJ8icoxQ4LyEb584aSEZ5I2CtA1IvJ'
            'RZhdSJYdpkBAagyHIG80PYLEzS%2BxrEFgSdE%2B88DgQSf5g8PJUo3rRuW9Kusmm2%2Bdf%2B3mvqHNyCAh%2'
            'FpRTzxObTroD4hTEbJMsy60uyQIefBriEoI8Ha2LUfoc9fBa2YyuAfq9ti3f477JlXKYBPex8ij5OLswGkjrST'
            'y3vlYjTDRwkUiAFKAw%3D%3D'
        )

    def test_build_and_sign_download_url(self, mock_time, expires, mock_provider, file_2_obj_name):
        signed_url = mock_provider._build_and_sign_url('GET', file_2_obj_name, **{})
        url = furl.furl(signed_url)

        assert '{}://{}'.format(url.scheme, url.host) == mock_provider.BASE_URL
        assert url.path == '/{}/{}'.format(mock_provider.bucket, quote(file_2_obj_name, safe=''))
        assert int(url.args.get('Expires')) == expires
        assert url.args.get('GoogleAccessId') == mock_provider.creds.service_account_email
        assert url.args.get('Signature') == unquote(
            'lC0Wc5VDE65cVk%2F3RSvO5YA9%2Fw8KMu201oR2cOmWkoJR%2FJcvV3lknK3VVh%2F4gQnbteM1RByBpKwZez'
            'EAT0cU4NKMNDkpj677GYSEe31KcwiFCDLKmFh6BtllDgeY3jstKwcWfolY09uSQo4t5zpSwYqlP8N267NmAknU'
            'slcbrAVwCuiPD%2Bv0ecocILVXtaPMDK360g5utAhISiHIUs8SlEY8abuQsxHQ96xOrNCBgtQ6tG1NqigNIRSM'
            'CSp%2F%2BzA6lC3OYaGFNYUGNvGMHJQiZSXV3lGVXP0BhJrycOp0%2FGmVU7k9mwer69t5imC9soHEuPqAF1jw'
            'txbML11F4KRj%2Bw%3D%3D'
        )

    def test_build_and_sign_delete_url(self, mock_time, expires, mock_provider, file_2_obj_name):
        signed_url = mock_provider._build_and_sign_url('DELETE', file_2_obj_name, **{})
        url = furl.furl(signed_url)

        assert '{}://{}'.format(url.scheme, url.host) == mock_provider.BASE_URL
        assert url.path == '/{}/{}'.format(mock_provider.bucket, quote(file_2_obj_name, safe=''))
        assert int(url.args.get('Expires')) == expires
        assert url.args.get('GoogleAccessId') == mock_provider.creds.service_account_email
        assert url.args.get('Signature') == unquote(
            'euS%2FNjjQDP%2FYJtFa99WnEjlyi0MDZjruI9bnsqvrvl1ngSDDdpm99SNltETfJCpy7eE6hU6WKntXJj6Zfo'
            'qb0w%2B1J3Fn3h1VSZJGwMuHrLGcxLWfZ17Iix2DnqK%2BT%2BBxCgMSgckxPxupXwGhlftupn4hbRQe2ZUYpQ'
            'FodW6bAFebF%2FdbIXEkweKSgyM09yA5U5jUVmXqmK7FpvlfWGQ%2FBN4v16euj59NMpLGFazoRJ0OQv1hyGSw'
            'W%2BLZxPLSWw9QgxFvIl%2FME0kojTJ0pf1dKh0X7wHHdL%2BiImqsxqy%2FR82K0jHJoZjiUoPX73nWPE0X6M'
            '%2FK0VtKoc%2FAoAa9%2BdLgvA%3D%3D'
        )

    def test_build_and_sign_copy_url(self, mock_time, expires, mock_provider, file_2_obj_name,
                                     file_2_copy_obj_name):
        object_name_with_bucket = '{}/{}'.format(mock_provider.bucket, file_2_obj_name)
        canonical_ext_headers = {'x-goog-copy-source': object_name_with_bucket}
        signed_url = mock_provider._build_and_sign_url(
            'PUT',
            file_2_copy_obj_name,
            canonical_ext_headers=canonical_ext_headers,
            **{}
        )
        url = furl.furl(signed_url)

        assert '{}://{}'.format(url.scheme, url.host) == mock_provider.BASE_URL
        assert url.path == '/{}/{}'.format(mock_provider.bucket, quote(file_2_copy_obj_name, safe=''))
        assert int(url.args.get('Expires')) == expires
        assert url.args.get('GoogleAccessId') == mock_provider.creds.service_account_email
        assert url.args.get('Signature') == unquote(
            'wC2tSJtDlhfv1gKOFdPk9L3PmyRIHId2ehqKEBDuiZ0XG2bGH9duno6PmzDqc9yAmC8OHKCHTVt6QOvAcF4%2F'
            'TQH3lThazlWdEwvfgRTk9s8zYw4k2QsScWyeo2g4wryukDi0ulfipaw12uiGKPpdVyj6HH1ooCrTCkM9m9XzEG'
            'spNfVmOreHbIN6mFjhHdD4r%2F13P0zd77CIdCWA5Tyq8UaFrgejva%2B36%2BU%2F6w4ywRnr0J5Aw5wGxg7u'
            'woqBWitPTHXkUfFWq1jidCWBTyI7Hhbk4Sm4W39TPIvCiDCPGq9UEMPt0DzYSKAXEcmpFvWL96FYZ6bjm2PqFV'
            'JDXM0j8Q%3D%3D'

        )


class TestHash:

    def test_get_multi_dict_from_json(self, meta_file_raw):

        resp_headers_json = json.loads(meta_file_raw)
        resp_headers_dict = utils.get_multi_dict_from_python_dict(dict(resp_headers_json))
        assert resp_headers_dict and isinstance(resp_headers_dict, MultiDictProxy)

        google_hashes = resp_headers_dict.getall('x-goog-hash')
        assert len(google_hashes) == 2

        for google_hash in google_hashes:
            assert google_hash.startswith('crc32c=') or google_hash.startswith('md5=')

    def test_verify_raw_google_hash_header(self):

        google_hash = 'crc32c=Tf8tmw==,md5=mkaUfJxiLXeSEl2OpExGOA=='
        assert utils.verify_raw_google_hash_header(google_hash)

    def test_verify_bad_raw_google_hash_header(self):

        google_hash = 'this cant possibly be right'
        assert utils.verify_raw_google_hash_header(google_hash) == False

    def test_decode_and_hexlify_hashes(self):

        base64_encoded_md5 = 'mkaUfJxiLXeSEl2OpExGOA=='
        etag = '9a46947c9c622d7792125d8ea44c4638'

        assert etag == utils.decode_and_hexlify_hashes(base64_encoded_md5)
