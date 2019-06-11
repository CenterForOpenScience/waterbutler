import os
import pytest


@pytest.fixture()
def repo_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/repo_metadata.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def file_history_page_1():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_history_page_1.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def file_history_page_2():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/file_history_page_2.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def path_metadata_folder():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/path_metadata_folder.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def path_metadata_file():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/path_metadata_file.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def branch_metadata():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/branch_metadata.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def folder_contents_page_1():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/folder_contents_page_1.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def folder_contents_page_2():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/folder_contents_page_2.json'),
              'r') as file_pointer:
        return file_pointer.read()


@pytest.fixture()
def folder_full_contents_list():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/folder_full_contents_list.json'),
              'r') as file_pointer:
        return file_pointer.read()
