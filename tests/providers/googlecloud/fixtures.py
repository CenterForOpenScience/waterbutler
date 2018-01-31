import os

import pytest

from waterbutler.core.path import WaterButlerPath


@pytest.fixture()
def mock_auth():
    return {
        'name': 'Roger Deng',
        'email': 'roger@deng.com'
    }


@pytest.fixture()
def mock_auth_2():
    return {
        'name': 'Deng Roger',
        'email': 'deng@roger.com'
    }


@pytest.fixture()
def mock_creds():
    return {
        'token': 'GlxLBdGqh56rEStTEs0KeMdEFmRJlGpg7e95y8jvzQoHbFZrnPDNB'
    }


@pytest.fixture()
def mock_creds_2():
    return {
        'token': 'eMdEFmRJlGpg7e95y8jvzQoHbFZrnPDNBsYTIG2txg8SmacwtERkU'
    }


@pytest.fixture()
def mock_settings():
    return {
        'bucket': 'gcloud-test.longzechen.com',
        'region': 'US-EAST1'
    }


@pytest.fixture()
def mock_settings_2():
    return {
        'bucket': 'gcloud-test-2.longzechen.com',
        'region': 'US-EAST1'
    }


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
    return '/test-folder-1/DSC_0235.JPG'


@pytest.fixture()
def dest_file_path():
    return '/test-folder-2/DSC_0235_2.JPG'


@pytest.fixture()
def src_folder_wb_path():
    return WaterButlerPath('/test-folder-1/')


@pytest.fixture()
def dest_folder_wb_path():
    return WaterButlerPath('/test-folder-1-copy/')


@pytest.fixture()
def src_file_obj_name():
    return 'test-folder-1/DSC_0244.JPG'


@pytest.fixture()
def dest_file_obj_name():
    return 'test-folder-1-copy/DSC_0244.JPG'


@pytest.fixture()
def src_folder_obj_name():
    return 'test-folder-1/'


@pytest.fixture()
def dest_folder_obj_name():
    return 'test-folder-1-copy/'


@pytest.fixture
def test_file_1():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/files/DSC_0235.JPG'
            ),
            'rb'
    ) as fp:
        return fp.read()


@pytest.fixture
def test_file_2():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/files/DSC_0244.JPG'
            ),
            'rb'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_folder_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_folder_extra():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-extra.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_folder_immediate():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-immediate.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_folder_all():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/folder-all.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_sub_folder_1_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/sub-folder-1-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_sub_folder_2_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/sub-folder-2-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_file_itself():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/file-itself.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def meta_file_extra():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/metadata/file-extra.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_req():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-request.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_req_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-request-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_resp():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-response.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_resp_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-response-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_delete_resp_failed_part():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/delete-response-failed-part.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_req():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-request.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_req_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-request-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_resp():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_resp_part():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response-part.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_resp_failed():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response-failed.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def batch_copy_resp_failed_part():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/batch-requests/copy-response-failed-part.txt'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def err_resp_unauthorized():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/errors/401-unauthorized.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def err_resp_not_found():

    with open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/errors/404-not-found.json'
            ),
            'r'
    ) as fp:
        return fp.read()


@pytest.fixture()
def failed_req_list():
    return [1, 3, 5]
