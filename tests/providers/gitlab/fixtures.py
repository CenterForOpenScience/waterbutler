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
def weird_ruby_response():
    """See: https://gitlab.com/gitlab-org/gitlab-ce/issues/31790"""
    return ('{:file_name=>"file", :file_path=>"file", :size=>5, '
            ':encoding=>"base64", :content=>"cm9sZgo=", :ref=>"master", '
            ':blob_id=>"cf37e8f1e80b5747301df4e1557036b37294a716", '
            ':commit_id=>"8c7b653eab7191dde3aff9e33ddf309c3d1f440f", '
            ':last_commit_id=>"8c7b653eab7191dde3aff9e33ddf309c3d1f440f"}')

# fixtures for testing file revision metadata
with open(os.path.join(os.path.dirname(__file__), 'fixtures/default-branch.json'), 'r') as fp:
    default_branches = json.load(fp)
