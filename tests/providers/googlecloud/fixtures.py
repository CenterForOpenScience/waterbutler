import io
import os

import pytest

from waterbutler.core.path import WaterButlerPath
from waterbutler.core.streams import FileStreamReader


@pytest.fixture()
def mock_auth():
    return {'name': 'Roger Deng', 'email': 'roger@deng.com'}


@pytest.fixture()
def mock_creds():
    return {
        'bucket': 'gcloud-test.longzechen.com',
        'region': 'US-EAST1',
        'access_token': 'GlxLBdGqh56rEStTEs0KeMdEFmRJlGpg7e95y8jvzQoHbFZrnPDNBsYTIG2txg8SmacwtERkU',
    }


@pytest.fixture()
def mock_settings():
    return {}


@pytest.fixture()
def batch_id_prefix():
    return 'b29c5de2-0db4-490b-b421-6a51b598bd22'


@pytest.fixture()
def batch_boundary():
    return '===============7330845974216740156=='


@pytest.fixture()
def file_wb_path():
    return WaterButlerPath('/test-folder-1-copy/DSC_0235.JPG')


@pytest.fixture()
def src_file_wb_path():
    return WaterButlerPath('/test-folder-1/DSC_0244.JPG')


@pytest.fixture()
def dest_file_wb_path():
    return WaterButlerPath('/test-folder-1/DSC_0244_COPY.JPG')


@pytest.fixture()
def folder_path():
    return '/test-folder-1/'


@pytest.fixture()
def sub_folder_1_path():
    return '/test-folder-1/test-folder-5/'


@pytest.fixture()
def sub_folder_2_path():
    return '/test-folder-1/test-folder-6/'


@pytest.fixture()
def sub_file_1_path():
    return '/test-folder-1/DSC_0235.JPG'


@pytest.fixture()
def sub_file_2_path():
    return '/test-folder-1/DSC_0244.JPG'


@pytest.fixture()
def file_path():
    return '/test-folder-1-copy/DSC_0235.JPG'


@pytest.fixture()
def src_folder_wb_path():
    return WaterButlerPath('/test-folder-1/')


@pytest.fixture()
def dest_folder_wb_path():
    return WaterButlerPath('/test-folder-1-copy/')


@pytest.fixture()
def src_file_object_name():
    return 'test-folder-1/DSC_0244.JPG'


@pytest.fixture()
def dest_file_object_name():
    return 'test-folder-1-copy/DSC_0244.JPG'


@pytest.fixture()
def src_folder_object_name():
    return 'test-folder-1/'


@pytest.fixture()
def dest_folder_object_name():
    return 'test-folder-1-copy/'


@pytest.fixture
def file_stream():
    return FileStreamReader(io.BytesIO(b'Test File Content'))


@pytest.fixture()
def metadata_folder_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_folder_extra():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-extra.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_folder_immediate():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-immediate.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_folder_all():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-all.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_sub_folder_1_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/sub-folder-1-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_sub_folder_2_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/sub-folder-2-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_file_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/file-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def metadata_file_extra():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/file-extra.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_request():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-request.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_request_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-request-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_response():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-response.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_response_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-response-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_response_failed_part():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-response-failed-part.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_request():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-request.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_request_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-request-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_response():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_response_part():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response-part.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_response_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_response_failed_part():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response-failed-part.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def error_response_401_unauthorized():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/errors/401-unauthorized.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def error_response_404_not_found():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/errors/404-not-found.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def failed_requests_list():
    return [1, 3, 5]
