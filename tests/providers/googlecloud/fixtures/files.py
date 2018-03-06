import os

import pytest

from waterbutler.core.path import WaterButlerPath


@pytest.fixture()
def file_wb_path():
    return WaterButlerPath('/xml-api/folder-1/text-file-1.txt')


@pytest.fixture()
def file_name():
    return 'text-file-1.txt'


@pytest.fixture()
def file_obj_name():
    return 'xml-api/folder-1/text-file-1.txt'


@pytest.fixture()
def meta_file_raw():
    with open(os.path.join(os.path.dirname(__file__), 'metadata/file-raw.json'), 'r') as fp:
        return fp.read()


@pytest.fixture()
def meta_file_parsed():
    with open(os.path.join(os.path.dirname(__file__), 'metadata/file-parsed.json'), 'r') as fp:
        return fp.read()


@pytest.fixture()
def meta_file_extra():
    with open(os.path.join(os.path.dirname(__file__), 'metadata/file-extra.json'), 'r') as fp:
        return fp.read()


@pytest.fixture()
def meta_file_resp_headers_raw():
    with open(os.path.join(os.path.dirname(__file__), 'resp_headers/file-raw.txt'), 'r') as fp:
        return fp.read()
