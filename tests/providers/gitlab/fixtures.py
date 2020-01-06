import os
import json
import pytest


@pytest.fixture
def simple_tree():
    return [
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": "folder1",
                "type": "tree",
                "path": "folder1",
                "mode": "040000"
            },
            {
                "id":"a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": "file1",
                "type": "blob",
                "path": "folder1/file1",
                "mode": "040000"
            }
        ]


@pytest.fixture
def gitlab_example_sub_project_tree():
    return [
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": ".gitkeep",
                "type": "blob",
                "path": "files/html/.gitkeep",
                "mode": "040000"
            },
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": ".gitkeep",
                "type": "tree",
                "path": "files/html/static",
                "mode": "040000"
            }
        ]


@pytest.fixture
def subfolder_tree():
    return [
            {
                "id": "a1e8f8d745cc87e3a9248358d9352bb7f9a0aeba",
                "name": "html",
                "type": "tree",
                "path": "files/html",
                "mode": "040000"
            },
            {
                "id": "4535904260b1082e14f867f7a24fd8c21495bde3",
                "name": "images",
                "type": "tree",
                "path": "files/images",
                "mode": "040000"
            },
            {
                "id": "31405c5ddef582c5a9b7a85230413ff90e2fe720",
                "name": "js",
                "type": "tree",
                "path": "files/js",
                "mode": "040000"
            },
            {
                "id": "cc71111cfad871212dc99572599a568bfe1e7e00",
                "name": "lfs",
                "type": "tree",
                "path": "files/lfs",
                "mode": "040000"
            },
            {
                "id": "fd581c619bf59cfdfa9c8282377bb09c2f897520",
                "name": "markdown",
                "type": "tree",
                "path": "files/markdown",
                "mode": "040000"
            },
            {
                "id": "23ea4d11a4bdd960ee5320c5cb65b5b3fdbc60db",
                "name": "ruby",
                "type": "tree",
                "path": "files/ruby",
                "mode": "040000"
            },
            {
                "id": "7d70e02340bac451f281cecf0a980907974bd8be",
                "name": "whitespace",
                "type": "blob",
                "path": "files/whitespace",
                "mode": "100644"
            }
        ]


@pytest.fixture
def simple_file_metadata():
    return {
        'file_name': 'file',
        'blob_id': 'abc123',
        'commit_id': 'xxxyyy',
        'file_path': '/folder1/folder2/file',
        'size': 123
    }


@pytest.fixture
def revisions_for_file():
    return [
        {
            "id": "931aece9275c0d084dfa7f6e0b3b2bb250e4b089",
            "short_id": "931aece9",
            "title": "deepi",
            "created_at": "2017-07-24T16:02:17.000-04:00",
            "parent_ids": [
                "d9901e83728f5aa034ef1c6193be89f7b644729f"
            ],
            "message": "deepi\n",
            "author_name": "Fitz Elliott",
            "author_email": "fitz@cos.io",
            "authored_date": "2017-07-24T16:02:17.000-04:00",
            "committer_name": "Fitz Elliott",
            "committer_email": "fitz@cos.io",
            "committed_date": "2017-07-24T16:02:17.000-04:00"
        },
        {
            "id": "b993ab399b22986a298efa509ca3a6bd605a62c4",
            "short_id": "b993ab39",
            "title": "morp",
            "created_at": "2017-07-24T15:57:24.000-04:00",
            "parent_ids": [
                "3f6f31fcbf0f70e8a59298fdccfd15f7c5f3cb2e"
            ],
            "message": "morp\n",
            "author_name": "Fitz Elliott",
            "author_email": "fitz@cos.io",
            "authored_date": "2017-07-24T15:57:24.000-04:00",
            "committer_name": "Fitz Elliott",
            "committer_email": "fitz@cos.io",
            "committed_date": "2017-07-24T15:57:24.000-04:00"
        },
        {
            "id": "d5aac723529e81761c95c71315ac2c747ed50b96",
            "short_id": "d5aac723",
            "title": "save WIP",
            "created_at": "2016-11-30T13:30:23.000-05:00",
            "parent_ids": [
                "ed7af40927525e92ef5b55720e6af32be1dba4ba"
            ],
            "message": "save WIP\n",
            "author_name": "Fitz Elliott",
            "author_email": "fitz@cos.io",
            "authored_date": "2016-11-30T13:30:23.000-05:00",
            "committer_name": "Fitz Elliott",
            "committer_email": "fitz@cos.io",
            "committed_date": "2016-11-30T13:30:23.000-05:00"
        }
    ]


# fixtures for testing file revision metadata
@pytest.fixture()
def default_branches():
    with open(os.path.join(os.path.dirname(__file__), 'fixtures/default-branch.json'), 'r') as fp:
        return json.load(fp)
