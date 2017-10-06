import os
import json
import pytest


@pytest.fixture
def repo_metadata():
    return {
        "scm": "git",
        "website": "",
        "has_wiki": False,
        "name": "wb-testing",
        "links": {
            "watchers": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/watchers"
            },
            "branches": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/refs/branches"
            },
            "tags": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/refs/tags"
            },
            "commits": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/commits"
            },
            "clone": [
                {
                    "href": "https://fitz_cos@bitbucket.org/fitz_cos/wb-testing.git",
                    "name": "https"
                },
                {
                    "href": "ssh://git@bitbucket.org/fitz_cos/wb-testing.git",
                    "name": "ssh"
                }
            ],
            "self": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing"
            },
            "html": {
                "href": "https://bitbucket.org/fitz_cos/wb-testing"
            },
            "avatar": {
                "href": "https://bitbucket.org/fitz_cos/wb-testing/avatar/32/"
            },
            "hooks": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/hooks"
            },
            "forks": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/forks"
            },
            "downloads": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/downloads"
            },
            "pullrequests": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/pullrequests"
            }
        },
        "fork_policy": "no_public_forks",
        "uuid": "{6d0aa7f9-a529-4cc1-928f-acfca61ca4ac}",
        "language": "",
        "created_on": "2017-02-08T18:52:06.813386+00:00",
        "full_name": "fitz_cos/wb-testing",
        "has_issues": False,
        "owner": {
            "username": "fitz_cos",
            "display_name": "fitz_cos",
            "type": "user",
            "uuid": "{1b3b8b5c-4391-47e2-b64b-c35fbeaf8a5e}",
            "links": {
                "self": {
                    "href": "https://api.bitbucket.org/2.0/users/fitz_cos"
                },
                "html": {
                    "href": "https://bitbucket.org/fitz_cos/"
                },
                "avatar": {
                    "href": "https://bitbucket.org/account/fitz_cos/avatar/32/"
                }
            }
        },
        "updated_on": "2017-02-20T18:04:02.003985+00:00",
        "size": 5513193,
        "type": "repository",
        "slug": "wb-testing",
        "is_private": True,
        "description": ""
    }


@pytest.fixture
def branch_metadata():
    return {
        "links": {
            "commits": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/commits/master"
            },
            "self": {
                "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/refs/branches/master"
            },
            "html": {
                "href": "https://bitbucket.org/fitz_cos/wb-testing/branch/master"
            }
        },
        "type": "branch",
        "name": "master",
        "repository": {
            "links": {
                "self": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing"
                },
                "html": {
                    "href": "https://bitbucket.org/fitz_cos/wb-testing"
                },
                "avatar": {
                    "href": "https://bitbucket.org/fitz_cos/wb-testing/avatar/32/"
                }
            },
            "type": "repository",
            "name": "wb-testing",
            "full_name": "fitz_cos/wb-testing",
            "uuid": "{6d0aa7f9-a529-4cc1-928f-acfca61ca4ac}"
        },
        "target": {
            "hash": "0b90ec89434bf7b96b1abb426764c801bbf950b5",
            "repository": {
                "links": {
                    "self": {
                        "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing"
                    },
                    "html": {
                        "href": "https://bitbucket.org/fitz_cos/wb-testing"
                    },
                    "avatar": {
                        "href": "https://bitbucket.org/fitz_cos/wb-testing/avatar/32/"
                    }
                },
                "type": "repository",
                "name": "wb-testing",
                "full_name": "fitz_cos/wb-testing",
                "uuid": "{6d0aa7f9-a529-4cc1-928f-acfca61ca4ac}"
            },
            "links": {
                "self": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/commit/0b90ec89434bf7b96b1abb426764c801bbf950b5"
                },
                "comments": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/commit/0b90ec89434bf7b96b1abb426764c801bbf950b5/comments"
                },
                "patch": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/patch/0b90ec89434bf7b96b1abb426764c801bbf950b5"
                },
                "html": {
                    "href": "https://bitbucket.org/fitz_cos/wb-testing/commits/0b90ec89434bf7b96b1abb426764c801bbf950b5"
                },
                "diff": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/diff/0b90ec89434bf7b96b1abb426764c801bbf950b5"
                },
                "approve": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/commit/0b90ec89434bf7b96b1abb426764c801bbf950b5/approve"
                },
                "statuses": {
                    "href": "https://api.bitbucket.org/2.0/repositories/fitz_cos/wb-testing/commit/0b90ec89434bf7b96b1abb426764c801bbf950b5/statuses"
                }
            },
            "author": {
                "raw": "fitz_cos <fitz@cos.io>",
                "user": {
                    "username": "fitz_cos",
                    "display_name": "fitz_cos",
                    "type": "user",
                    "uuid": "{1b3b8b5c-4391-47e2-b64b-c35fbeaf8a5e}",
                    "links": {
                        "self": {
                            "href": "https://api.bitbucket.org/2.0/users/fitz_cos"
                        },
                        "html": {
                            "href": "https://bitbucket.org/fitz_cos/"
                        },
                        "avatar": {
                            "href": "https://bitbucket.org/account/fitz_cos/avatar/32/"
                        }
                    }
                }
            },
            "parents": [],
            "date": "2017-02-15T21:08:22+00:00",
            "message": "README.md created online with Bitbucket",
            "type": "commit"
        }
    }


@pytest.fixture
def filehistory():
    return [
        {
            "utctimestamp" : "2016-09-08T19:20:59+00:00",
            "node" : "522a6be9f98d",
            "parents" : [
                "c7e72a053216"
            ],
            "raw_author" : "Fitz Elliott <fitz@cos.io>",
            "author" : {
                "is_team" : False,
                "display_name" : "fitz_cos",
                "first_name" : "fitz_cos",
                "avatar" : "https://bitbucket.org/account/fitz_cos/avatar/32/?ts=1487604330",
                "username" : "fitz_cos",
                "is_staff" : False,
                "resource_uri" : "/1.0/users/fitz_cos",
                "last_name" : ""
            },
            "timestamp" : "2016-09-08 21:20:59",
            "revision" : None,
            "message" : "add albert to smallbranch-a\n",
            "raw_node" : "522a6be9f98ddf7938d7e9568a6375cd0f88e40e",
            "size" : -1,
            "files" : [
                {
                    "type" : "added",
                    "file" : "albert.txt"
                }
            ],
            "branch" : "smallbranch-a"
        }
    ]

@pytest.fixture
def owner():
    return 'fitz_cos'

@pytest.fixture
def repo():
    return 'wb-testing'

@pytest.fixture
def file_metadata():
    return {
        "size": 13,
        "path": "plaster/aaa-01-2.txt",
        "timestamp": "2016-10-14T00:37:55Z",
        "utctimestamp": "2016-10-14 00:37:55+00:00",
        "revision": "90c8f7eef948"
    }


@pytest.fixture
def folder_metadata():
    return {'name': 'plaster'}


@pytest.fixture
def revision_metadata():
    return {
        "utctimestamp" : "2016-09-08T19:20:59+00:00",
        "node" : "522a6be9f98d",
        "parents" : [
            "c7e72a053216"
        ],
        "raw_author" : "Fitz Elliott <fitz@cos.io>",
        "author" : {
            "is_team" : False,
            "display_name" : "fitz_cos",
            "first_name" : "fitz_cos",
            "avatar" : "https://bitbucket.org/account/fitz_cos/avatar/32/?ts=1487604330",
            "username" : "fitz_cos",
            "is_staff" : False,
            "resource_uri" : "/1.0/users/fitz_cos",
            "last_name" : ""
        },
        "timestamp" : "2016-09-08 21:20:59",
        "revision" : None,
        "message" : "add albert to smallbranch-a\n",
        "raw_node" : "522a6be9f98ddf7938d7e9568a6375cd0f88e40e",
        "size" : -1,
        "files" : [
            {
                "type" : "added",
                "file" : "albert.txt"
            }
        ],
        "branch" : "smallbranch-a"
    }


# fixtures for testing permutations of validate_v1_path & co.
with open(os.path.join(os.path.dirname(__file__), 'fixtures/validate_path.json'), 'r') as fp:
    validate_path = json.load(fp)


# fixtures for testing file revision metadata
with open(os.path.join(os.path.dirname(__file__), 'fixtures/revisions.json'), 'r') as fp:
    revisions = json.load(fp)
