import pytest

import io
import os
import copy
import json
import base64
import hashlib
from http import client

import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath
from waterbutler.core.provider import build_url

from waterbutler.providers.github import GitHubProvider
from waterbutler.providers.github import settings as github_settings
from waterbutler.providers.github.provider import GitHubPath
from waterbutler.providers.github.metadata import GitHubRevision
from waterbutler.providers.github.metadata import GitHubFileTreeMetadata
from waterbutler.providers.github.metadata import GitHubFolderTreeMetadata
from waterbutler.providers.github.metadata import GitHubFileContentMetadata
from waterbutler.providers.github.metadata import GitHubFolderContentMetadata


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'naps'}


@pytest.fixture
def settings():
    return {
        'owner': 'cat',
        'repo': 'food',
    }


@pytest.fixture
def file_content():
    return b'hungry'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def upload_response():
    return {
        "content": {
            "name": "hello.txt",
            "path": "notes/hello.txt",
            "sha": "95b966ae1c166bd92f8ae7d1c313e738c731dfc3",
            "size": 9,
            "url": "https://api.github.com/repos/octocat/Hello-World/contents/notes/hello.txt",
            "html_url": "https://github.com/octocat/Hello-World/blob/master/notes/hello.txt",
            "git_url": "https://api.github.com/repos/octocat/Hello-World/git/blobs/95b966ae1c166bd92f8ae7d1c313e738c731dfc3",
            "type": "file",
            "_links": {
                "self": "https://api.github.com/repos/octocat/Hello-World/contents/notes/hello.txt",
                "git": "https://api.github.com/repos/octocat/Hello-World/git/blobs/95b966ae1c166bd92f8ae7d1c313e738c731dfc3",
                "html": "https://github.com/octocat/Hello-World/blob/master/notes/hello.txt"
            }
        },
        "commit": {
            "sha": "7638417db6d59f3c431d3e1f261cc637155684cd",
            "url": "https://api.github.com/repos/octocat/Hello-World/git/commits/7638417db6d59f3c431d3e1f261cc637155684cd",
            "html_url": "https://github.com/octocat/Hello-World/git/commit/7638417db6d59f3c431d3e1f261cc637155684cd",
            "author": {
                "date": "2010-04-10T14:10:01-07:00",
                "name": "Scott Chacon",
                "email": "schacon@gmail.com"
            },
            "committer": {
                "date": "2010-04-10T14:10:01-07:00",
                "name": "Scott Chacon",
                "email": "schacon@gmail.com"
            },
            "message": "my commit message",
            "tree": {
                "url": "https://api.github.com/repos/octocat/Hello-World/git/trees/691272480426f78a0138979dd3ce63b77f706feb",
                "sha": "691272480426f78a0138979dd3ce63b77f706feb"
            },
            "parents": [
                {
                    "url": "https://api.github.com/repos/octocat/Hello-World/git/commits/1acc419d4d6a9ce985db7be48c6349a0475975b5",
                    "html_url": "https://github.com/octocat/Hello-World/git/commit/1acc419d4d6a9ce985db7be48c6349a0475975b5",
                    "sha": "1acc419d4d6a9ce985db7be48c6349a0475975b5"
                }
            ]
        }
    }


@pytest.fixture
def create_folder_response():
    return {
        "content": {
            "name": ".gitkeep",
            "path": "i/like/trains/.gitkeep",
            "sha": "95b966ae1c166bd92f8ae7d1c313e738c731dfc3",
            "size": 9,
            "url": "https://api.github.com/repos/octocat/Hello-World/contents/notes/hello.txt",
            "html_url": "https://github.com/octocat/Hello-World/blob/master/notes/hello.txt",
            "git_url": "https://api.github.com/repos/octocat/Hello-World/git/blobs/95b966ae1c166bd92f8ae7d1c313e738c731dfc3",
            "type": "file",
            "_links": {
                "self": "https://api.github.com/repos/octocat/Hello-World/contents/notes/hello.txt",
                "git": "https://api.github.com/repos/octocat/Hello-World/git/blobs/95b966ae1c166bd92f8ae7d1c313e738c731dfc3",
                "html": "https://github.com/octocat/Hello-World/blob/master/notes/hello.txt"
            }
        },
        "commit": {
            "sha": "7638417db6d59f3c431d3e1f261cc637155684cd",
            "url": "https://api.github.com/repos/octocat/Hello-World/git/commits/7638417db6d59f3c431d3e1f261cc637155684cd",
            "html_url": "https://github.com/octocat/Hello-World/git/commit/7638417db6d59f3c431d3e1f261cc637155684cd",
            "author": {
                "date": "2010-04-10T14:10:01-07:00",
                "name": "Scott Chacon",
                "email": "schacon@gmail.com"
            },
            "committer": {
                "date": "2010-04-10T14:10:01-07:00",
                "name": "Scott Chacon",
                "email": "schacon@gmail.com"
            },
            "message": "my commit message",
            "tree": {
                "url": "https://api.github.com/repos/octocat/Hello-World/git/trees/691272480426f78a0138979dd3ce63b77f706feb",
                "sha": "691272480426f78a0138979dd3ce63b77f706feb"
            },
            "parents": [
                {
                    "url": "https://api.github.com/repos/octocat/Hello-World/git/commits/1acc419d4d6a9ce985db7be48c6349a0475975b5",
                    "html_url": "https://github.com/octocat/Hello-World/git/commit/1acc419d4d6a9ce985db7be48c6349a0475975b5",
                    "sha": "1acc419d4d6a9ce985db7be48c6349a0475975b5"
                }
            ]
        }
    }

@pytest.fixture
def comparision_metadata():
    return {'ahead_by': 0,
            'base_commit': {'author': {'avatar_url': 'https://avatars0.githubusercontent.com/u/9688518?v=3',
                                'events_url': 'https://api.github.com/users/Johnetordoff/events{/privacy}',
                                'followers_url': 'https://api.github.com/users/Johnetordoff/followers',
                                'following_url': 'https://api.github.com/users/Johnetordoff/following{/other_user}',
                                'gists_url': 'https://api.github.com/users/Johnetordoff/gists{/gist_id}',
                                'gravatar_id': '',
                                'html_url': 'https://github.com/Johnetordoff',
                                'id': 9688518,
                                'login': 'Johnetordoff',
                                'organizations_url': 'https://api.github.com/users/Johnetordoff/orgs',
                                'received_events_url': 'https://api.github.com/users/Johnetordoff/received_events',
                                'repos_url': 'https://api.github.com/users/Johnetordoff/repos',
                                'site_admin': False,
                                'starred_url': 'https://api.github.com/users/Johnetordoff/starred{/owner}{/repo}',
                                'subscriptions_url': 'https://api.github.com/users/Johnetordoff/subscriptions',
                                'type': 'User',
                                'url': 'https://api.github.com/users/Johnetordoff'},
                     'comments_url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578/comments',
                     'commit': {'author': {'date': '2017-05-02T19:00:19Z',
                                           'email': 'Johnetordoff@users.noreply.github.com',
                                           'name': 'John Tordoff'},
                                'comment_count': 0,
                                'committer': {'date': '2017-05-02T19:00:19Z',
                                              'email': 'vscxw@osf.io',
                                              'name': 'fake'},
                                'message': 'File updated on behalf of WaterButler',
                                'tree': {'sha': '48a98cf824b1ae6d64dc1216d6a3793ae6daf8a8',
                                         'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/git/trees/48a98cf824b1ae6d64dc1216d6a3793ae6daf8a8'},
                                'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/git/commits/b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578'},
                     'committer': None,
                     'html_url': 'https://github.com/Johnetordoff/git-hub-api-test/commit/b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578',
                     'parents': [{
                                     'html_url': 'https://github.com/Johnetordoff/git-hub-api-test/commit/a0f1708eef358227cb0bc1dbd29b604086e71a4e',
                                     'sha': 'a0f1708eef358227cb0bc1dbd29b604086e71a4e',
                                     'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/a0f1708eef358227cb0bc1dbd29b604086e71a4e'}],
                     'sha': 'b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578',
                     'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578'},
     'behind_by': 1,
     'diff_url': 'https://github.com/Johnetordoff/git-hub-api-test/compare/master...a0f1708eef358227cb0bc1dbd29b604086e71a4e.diff',
     'files': [],
     'html_url': 'https://github.com/Johnetordoff/git-hub-api-test/compare/master...a0f1708eef358227cb0bc1dbd29b604086e71a4e',
     'merge_base_commit': {'author': {'avatar_url': 'https://avatars0.githubusercontent.com/u/9688518?v=3',
                                      'events_url': 'https://api.github.com/users/Johnetordoff/events{/privacy}',
                                      'followers_url': 'https://api.github.com/users/Johnetordoff/followers',
                                      'following_url': 'https://api.github.com/users/Johnetordoff/following{/other_user}',
                                      'gists_url': 'https://api.github.com/users/Johnetordoff/gists{/gist_id}',
                                      'gravatar_id': '',
                                      'html_url': 'https://github.com/Johnetordoff',
                                      'id': 9688518,
                                      'login': 'Johnetordoff',
                                      'organizations_url': 'https://api.github.com/users/Johnetordoff/orgs',
                                      'received_events_url': 'https://api.github.com/users/Johnetordoff/received_events',
                                      'repos_url': 'https://api.github.com/users/Johnetordoff/repos',
                                      'site_admin': False,
                                      'starred_url': 'https://api.github.com/users/Johnetordoff/starred{/owner}{/repo}',
                                      'subscriptions_url': 'https://api.github.com/users/Johnetordoff/subscriptions',
                                      'type': 'User',
                                      'url': 'https://api.github.com/users/Johnetordoff'},
                           'comments_url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/a0f1708eef358227cb0bc1dbd29b604086e71a4e/comments',
                           'commit': {'author': {'date': '2017-05-02T18:44:45Z',
                                                 'email': 'Johnetordoff@users.noreply.github.com',
                                                 'name': 'John Tordoff'},
                                      'comment_count': 0,
                                      'committer': {'date': '2017-05-02T18:44:45Z',
                                                    'email': 'vscxw@osf.io',
                                                    'name': 'fake'},
                                      'message': 'Moved on behalf of WaterButler',
                                      'tree': {'sha': 'ed792c2c6dfc1947e19b2b5f3059385cd674b86f',
                                               'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/git/trees/ed792c2c6dfc1947e19b2b5f3059385cd674b86f'},
                                      'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/git/commits/a0f1708eef358227cb0bc1dbd29b604086e71a4e'},
                           'committer': None,
                           'html_url': 'https://github.com/Johnetordoff/git-hub-api-test/commit/a0f1708eef358227cb0bc1dbd29b604086e71a4e',
                           'parents': [{
                                           'html_url': 'https://github.com/Johnetordoff/git-hub-api-test/commit/e5e79a120fd6baa78289df7a3528216088d5a3db',
                                           'sha': 'e5e79a120fd6baa78289df7a3528216088d5a3db',
                                           'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/e5e79a120fd6baa78289df7a3528216088d5a3db'}],
                           'sha': 'a0f1708eef358227cb0bc1dbd29b604086e71a4e',
                           'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/a0f1708eef358227cb0bc1dbd29b604086e71a4e'},
     'patch_url': 'https://github.com/Johnetordoff/git-hub-api-test/compare/master...a0f1708eef358227cb0bc1dbd29b604086e71a4e.patch',
     'permalink_url': 'https://github.com/Johnetordoff/git-hub-api-test/compare/Johnetordoff:b13aadf...Johnetordoff:a0f1708',
     'status': 'behind',
     'total_commits': 0,
     'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/compare/master...a0f1708eef358227cb0bc1dbd29b604086e71a4e'}



@pytest.fixture
def repo_metadata():
    return {
        'full_name': 'octocat/Hello-World',
        'permissions': {
            'push': False,
            'admin': False,
            'pull': True
        },
        'has_downloads': True,
        'notifications_url': 'https://api.github.com/repos/octocat/Hello-World/notifications{?since,all,participating}',
        'releases_url': 'https://api.github.com/repos/octocat/Hello-World/releases{/id}',
        'downloads_url': 'https://api.github.com/repos/octocat/Hello-World/downloads',
        'merges_url': 'https://api.github.com/repos/octocat/Hello-World/merges',
        'owner': {
            'avatar_url': 'https://avatars.githubusercontent.com/u/583231?v=3',
            'organizations_url': 'https://api.github.com/users/octocat/orgs',
            'type': 'User',
            'starred_url': 'https://api.github.com/users/octocat/starred{/owner}{/repo}',
            'url': 'https://api.github.com/users/octocat',
            'html_url': 'https://github.com/octocat',
            'received_events_url': 'https://api.github.com/users/octocat/received_events',
            'subscriptions_url': 'https://api.github.com/users/octocat/subscriptions',
            'site_admin': False,
            'gravatar_id': '',
            'repos_url': 'https://api.github.com/users/octocat/repos',
            'gists_url': 'https://api.github.com/users/octocat/gists{/gist_id}',
            'id': 583231,
            'events_url': 'https://api.github.com/users/octocat/events{/privacy}',
            'login': 'octocat',
            'following_url': 'https://api.github.com/users/octocat/following{/other_user}',
            'followers_url': 'https://api.github.com/users/octocat/followers'
        },
        'html_url': 'https://github.com/octocat/Hello-World',
        'comments_url': 'https://api.github.com/repos/octocat/Hello-World/comments{/number}',
        'git_url': 'git://github.com/octocat/Hello-World.git',
        'ssh_url': 'git@github.com:octocat/Hello-World.git',
        'language': None,
        'pulls_url': 'https://api.github.com/repos/octocat/Hello-World/pulls{/number}',
        'subscribers_count': 1850,
        'forks_count': 1085,
        'watchers_count': 1407,
        'id': 1296269,
        'keys_url': 'https://api.github.com/repos/octocat/Hello-World/keys{/key_id}',
        'default_branch': 'master',
        'stargazers_count': 1407,
        'tags_url': 'https://api.github.com/repos/octocat/Hello-World/tags',
        'clone_url': 'https://github.com/octocat/Hello-World.git',
        'homepage': '',
        'forks_url': 'https://api.github.com/repos/octocat/Hello-World/forks',
        'branches_url': 'https://api.github.com/repos/octocat/Hello-World/branches{/branch}',
        'url': 'https://api.github.com/repos/octocat/Hello-World',
        'contents_url': 'https://api.github.com/repos/octocat/Hello-World/contents/{+path}',
        'hooks_url': 'https://api.github.com/repos/octocat/Hello-World/hooks',
        'git_tags_url': 'https://api.github.com/repos/octocat/Hello-World/git/tags{/sha}',
        'statuses_url': 'https://api.github.com/repos/octocat/Hello-World/statuses/{sha}',
        'trees_url': 'https://api.github.com/repos/octocat/Hello-World/git/trees{/sha}',
        'contributors_url': 'https://api.github.com/repos/octocat/Hello-World/contributors',
        'open_issues': 126,
        'has_pages': False,
        'pushed_at': '2014-06-11T21:51:23Z',
        'network_count': 1085,
        'commits_url': 'https://api.github.com/repos/octocat/Hello-World/commits{/sha}',
        'git_commits_url': 'https://api.github.com/repos/octocat/Hello-World/git/commits{/sha}',
        'svn_url': 'https://github.com/octocat/Hello-World',
        'forks': 1085,
        'fork': False,
        'subscription_url': 'https://api.github.com/repos/octocat/Hello-World/subscription',
        'archive_url': 'https://api.github.com/repos/octocat/Hello-World/{archive_format}{/ref}',
        'subscribers_url': 'https://api.github.com/repos/octocat/Hello-World/subscribers',
        'description': 'This your first repo!',
        'blobs_url': 'https://api.github.com/repos/octocat/Hello-World/git/blobs{/sha}',
        'teams_url': 'https://api.github.com/repos/octocat/Hello-World/teams',
        'compare_url': 'https://api.github.com/repos/octocat/Hello-World/compare/{base}...{head}',
        'issues_url': 'https://api.github.com/repos/octocat/Hello-World/issues{/number}',
        'stargazers_url': 'https://api.github.com/repos/octocat/Hello-World/stargazers',
        'private': False,
        'created_at': '2011-01-26T19:01:12Z',
        'issue_comment_url': 'https://api.github.com/repos/octocat/Hello-World/issues/comments/{number}',
        'has_issues': True,
        'milestones_url': 'https://api.github.com/repos/octocat/Hello-World/milestones{/number}',
        'issue_events_url': 'https://api.github.com/repos/octocat/Hello-World/issues/events{/number}',
        'languages_url': 'https://api.github.com/repos/octocat/Hello-World/languages',
        'name': 'Hello-World',
        'mirror_url': None,
        'has_wiki': True,
        'updated_at': '2014-12-12T16:45:49Z',
        'watchers': 1407,
        'open_issues_count': 126,
        'labels_url': 'https://api.github.com/repos/octocat/Hello-World/labels{/name}',
        'collaborators_url': 'https://api.github.com/repos/octocat/Hello-World/collaborators{/collaborator}',
        'assignees_url': 'https://api.github.com/repos/octocat/Hello-World/assignees{/user}',
        'size': 558,
        'git_refs_url': 'https://api.github.com/repos/octocat/Hello-World/git/refs{/sha}',
        'events_url': 'https://api.github.com/repos/octocat/Hello-World/events'
    }


@pytest.fixture
def branch_metadata():
    return {
        'commit': {
            'html_url': 'https://github.com/octocat/Hello-World/commit/7fd1a60b01f91b314f59955a4e4d4e80d8edf11d',
            'url': 'https://api.github.com/repos/octocat/Hello-World/commits/7fd1a60b01f91b314f59955a4e4d4e80d8edf11d',
            'committer': {
                'html_url': 'https://github.com/octocat',
                'login': 'octocat',
                'type': 'User',
                'gravatar_id': '',
                'avatar_url': 'https://avatars.githubusercontent.com/u/583231?v=3',
                'received_events_url': 'https://api.github.com/users/octocat/received_events',
                'id': 583231,
                'starred_url': 'https://api.github.com/users/octocat/starred{/owner}{/repo}',
                'subscriptions_url': 'https://api.github.com/users/octocat/subscriptions',
                'organizations_url': 'https://api.github.com/users/octocat/orgs',
                'url': 'https://api.github.com/users/octocat',
                'following_url': 'https://api.github.com/users/octocat/following{/other_user}',
                'followers_url': 'https://api.github.com/users/octocat/followers',
                'repos_url': 'https://api.github.com/users/octocat/repos',
                'events_url': 'https://api.github.com/users/octocat/events{/privacy}',
                'gists_url': 'https://api.github.com/users/octocat/gists{/gist_id}',
                'site_admin': False
            },
            'parents': [{
                            'html_url': 'https://github.com/octocat/Hello-World/commit/553c2077f0edc3d5dc5d17262f6aa498e69d6f8e',
                            'url': 'https://api.github.com/repos/octocat/Hello-World/commits/553c2077f0edc3d5dc5d17262f6aa498e69d6f8e',
                            'sha': '553c2077f0edc3d5dc5d17262f6aa498e69d6f8e'
                        }, {
                            'html_url': 'https://github.com/octocat/Hello-World/commit/762941318ee16e59dabbacb1b4049eec22f0d303',
                            'url': 'https://api.github.com/repos/octocat/Hello-World/commits/762941318ee16e59dabbacb1b4049eec22f0d303',
                            'sha': '762941318ee16e59dabbacb1b4049eec22f0d303'
                        }],
            'sha': '7fd1a60b01f91b314f59955a4e4d4e80d8edf11d',
            'author': {
                'html_url': 'https://github.com/octocat',
                'login': 'octocat',
                'type': 'User',
                'gravatar_id': '',
                'avatar_url': 'https://avatars.githubusercontent.com/u/583231?v=3',
                'received_events_url': 'https://api.github.com/users/octocat/received_events',
                'id': 583231,
                'starred_url': 'https://api.github.com/users/octocat/starred{/owner}{/repo}',
                'subscriptions_url': 'https://api.github.com/users/octocat/subscriptions',
                'organizations_url': 'https://api.github.com/users/octocat/orgs',
                'url': 'https://api.github.com/users/octocat',
                'following_url': 'https://api.github.com/users/octocat/following{/other_user}',
                'followers_url': 'https://api.github.com/users/octocat/followers',
                'repos_url': 'https://api.github.com/users/octocat/repos',
                'events_url': 'https://api.github.com/users/octocat/events{/privacy}',
                'gists_url': 'https://api.github.com/users/octocat/gists{/gist_id}',
                'site_admin': False
            },
            'comments_url': 'https://api.github.com/repos/octocat/Hello-World/commits/7fd1a60b01f91b314f59955a4e4d4e80d8edf11d/comments',
            'commit': {
                'url': 'https://api.github.com/repos/octocat/Hello-World/git/commits/7fd1a60b01f91b314f59955a4e4d4e80d8edf11d',
                'message': 'Merge pull request #6 from Spaceghost/patch-1\n\nNew line at end of file.',
                'committer': {
                    'email': 'octocat@nowhere.com',
                    'date': '2012-03-06T23:06:50Z',
                    'name': 'The Octocat'
                },
                'tree': {
                    'url': 'https://api.github.com/repos/octocat/Hello-World/git/trees/b4eecafa9be2f2006ce1b709d6857b07069b4608',
                    'sha': 'b4eecafa9be2f2006ce1b709d6857b07069b4608'
                },
                'comment_count': 51,
                'author': {
                    'email': 'octocat@nowhere.com',
                    'date': '2012-03-06T23:06:50Z',
                    'name': 'The Octocat'
                }
            }
        },
        '_links': {
            'html': 'https://github.com/octocat/Hello-World/tree/master',
            'self': 'https://api.github.com/repos/octocat/Hello-World/branches/master'
        },
        'name': 'master'
    }

@pytest.fixture
def branch_list_metadata():
    return [
            {'commit': {
                'sha': 'b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578',
                'url': 'https://api.github.com/repos/Johnetordoff/git-hub-api-test/commits/b13aadfbe32408eea7d4e0eb0ffe7c6da78e3578'},
                'name': 'master'
            }
            ]

@pytest.fixture
def content_repo_metadata_root():
    return [
        {
            'path': 'file.txt',
            'type': 'file',
            'html_url': 'https://github.com/icereval/test/blob/master/file.txt',
            'git_url': 'https://api.github.com/repos/icereval/test/git/blobs/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
            'url': 'https://api.github.com/repos/icereval/test/contents/file.txt?ref=master',
            'sha': 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
            '_links': {
                'git': 'https://api.github.com/repos/icereval/test/git/blobs/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
                'self': 'https://api.github.com/repos/icereval/test/contents/file.txt?ref=master',
                'html': 'https://github.com/icereval/test/blob/master/file.txt'
            },
            'name': 'file.txt',
            'size': 0,
            'download_url': 'https://raw.githubusercontent.com/icereval/test/master/file.txt'
        }, {
            'path': 'level1',
            'type': 'dir',
            'html_url': 'https://github.com/icereval/test/tree/master/level1',
            'git_url': 'https://api.github.com/repos/icereval/test/git/trees/bc1087ebfe8354a684bf9f8b75517784143dde86',
            'url': 'https://api.github.com/repos/icereval/test/contents/level1?ref=master',
            'sha': 'bc1087ebfe8354a684bf9f8b75517784143dde86',
            '_links': {
                'git': 'https://api.github.com/repos/icereval/test/git/trees/bc1087ebfe8354a684bf9f8b75517784143dde86',
                'self': 'https://api.github.com/repos/icereval/test/contents/level1?ref=master',
                'html': 'https://github.com/icereval/test/tree/master/level1'
            },
            'name': 'level1',
            'size': 0,
            'download_url': None
        }, {
            'path': 'test.rst',
            'type': 'file',
            'html_url': 'https://github.com/icereval/test/blob/master/test.rst',
            'git_url': 'https://api.github.com/repos/icereval/test/git/blobs/ca39bcbf849231525ce9e775935fcb18ed477b5a',
            'url': 'https://api.github.com/repos/icereval/test/contents/test.rst?ref=master',
            'sha': 'ca39bcbf849231525ce9e775935fcb18ed477b5a',
            '_links': {
                'git': 'https://api.github.com/repos/icereval/test/git/blobs/ca39bcbf849231525ce9e775935fcb18ed477b5a',
                'self': 'https://api.github.com/repos/icereval/test/contents/test.rst?ref=master',
                'html': 'https://github.com/icereval/test/blob/master/test.rst'
            },
            'name': 'test.rst',
            'size': 190,
            'download_url': 'https://raw.githubusercontent.com/icereval/test/master/test.rst'
        }
    ]


@pytest.fixture
def repo_tree_metadata_root():
    return {
        'tree': [
            {
                'url': 'https://api.github.com/repos/icereval/test/git/blobs/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
                'size': 0,
                'type': 'blob',
                'path': 'file.txt',
                'mode': '100644',
                'sha': 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391'
            },
            {
                'type': 'tree',
                'url': 'https://api.github.com/repos/icereval/test/git/trees/05353097666f449344b7f69036c70a52dc504088',
                'path': 'level1',
                'mode': '040000',
                'sha': '05353097666f449344b7f69036c70a52dc504088'
            },
            {
                'url': 'https://api.github.com/repos/icereval/test/git/blobs/ca39bcbf849231525ce9e775935fcb18ed477b5a',
                'size': 190,
                'type': 'blob',
                'path': 'test.rst',
                'mode': '100644',
                'sha': 'ca39bcbf849231525ce9e775935fcb18ed477b5a'
            }
        ],
        'url': 'https://api.github.com/repos/icereval/test/git/trees/cd83e4a08261a54f1c4630fbb1de34d1e48f0c8a',
        'truncated': False,
        'sha': 'cd83e4a08261a54f1c4630fbb1de34d1e48f0c8a'
    }


@pytest.fixture
def content_repo_metadata_root_file_txt():
    return {
        '_links': {
            'git': 'https://api.github.com/repos/icereval/test/git/blobs/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
            'self': 'https://api.github.com/repos/icereval/test/contents/file.txt?ref=master',
            'html': 'https://github.com/icereval/test/blob/master/file.txt'
        },
        'content': '',
        'url': 'https://api.github.com/repos/icereval/test/contents/file.txt?ref=master',
        'html_url': 'https://github.com/icereval/test/blob/master/file.txt',
        'download_url': 'https://raw.githubusercontent.com/icereval/test/master/file.txt',
        'name': 'file.txt',
        'type': 'file',
        'sha': 'e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
        'encoding': 'base64',
        'git_url': 'https://api.github.com/repos/icereval/test/git/blobs/e69de29bb2d1d6434b8b29ae775ad8c2e48c5391',
        'path': 'file.txt',
        'size': 0
    }


@pytest.fixture
def nested_tree_metadata():
    return {
        'tree': [
            {'path': 'alpha.txt', 'type': 'blob', 'mode': '100644', 'size': 11, 'url': 'https://api.github.com/repos/felliott/wb-testing/git/blobs/3e72bca321b45548d7a7cfd1e8570afec6e5f2f1', 'sha': '3e72bca321b45548d7a7cfd1e8570afec6e5f2f1'},
            {'path': 'beta', 'type': 'tree', 'mode': '040000', 'url': 'https://api.github.com/repos/felliott/wb-testing/git/trees/48cf869b1f09e4b0cfa765ce3c0812fb719973e9', 'sha': '48cf869b1f09e4b0cfa765ce3c0812fb719973e9'},
            {'path': 'beta/gamma.txt', 'type': 'blob', 'mode': '100644', 'size': 11, 'url': 'https://api.github.com/repos/felliott/wb-testing/git/blobs/f59573b4169cee7da926e6508961438952ba0aaf', 'sha': 'f59573b4169cee7da926e6508961438952ba0aaf'},
            {'path': 'beta/delta', 'type': 'tree', 'mode': '040000', 'url': 'https://api.github.com/repos/felliott/wb-testing/git/trees/bb0c11bb86d7fc4807f6c8dc2a2bb9513802bf33','sha': 'bb0c11bb86d7fc4807f6c8dc2a2bb9513802bf33'},
            {'path': 'beta/delta/epsilon.txt', 'type': 'blob', 'mode': '100644', 'size': 13, 'url': 'https://api.github.com/repos/felliott/wb-testing/git/blobs/44b20789279ae90266791ba07f87a3ab42264690', 'sha': '44b20789279ae90266791ba07f87a3ab42264690'},
        ],
        'truncated': False,
        'url': 'https://api.github.com/repos/felliott/wb-testing/git/trees/076cc413680157d4dea4c17831687873998a4928',
        'sha': '076cc413680157d4dea4c17831687873998a4928'
    }


@pytest.fixture
def provider(auth, credentials, settings, repo_metadata):
    provider = GitHubProvider(auth, credentials, settings)
    provider._repo = repo_metadata
    provider.default_branch = repo_metadata['default_branch']
    return provider


class TestHelpers:

    async def test_build_repo_url(self, provider, settings):
        expected = provider.build_url('repos', settings['owner'], settings['repo'], 'contents')
        assert provider.build_repo_url('contents') == expected

    async def test_committer(self, auth, provider):
        expected = {
            'name': auth['name'],
            'email': auth['email'],
        }
        assert provider.committer == expected


class TestValidatePath:

    def test_child_gets_branch(self):
        parent = GitHubPath('/', _ids=[('master', None)], folder=True)

        child_file = parent.child('childfile', folder=False)
        assert child_file.identifier[0] == 'master'

        child_folder = parent.child('childfolder', folder=True)
        assert child_folder.identifier[0] == 'master'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, branch_metadata, repo_tree_metadata_root):
        branch_url = provider.build_repo_url('branches', provider.default_branch)
        tree_url = provider.build_repo_url('git', 'trees',
                                           branch_metadata['commit']['commit']['tree']['sha'],
                                           recursive=1)

        aiohttpretty.register_json_uri('GET', branch_url, body=branch_metadata)
        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)

        blob_path = 'file.txt'

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + blob_path)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + blob_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + blob_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, branch_metadata, repo_tree_metadata_root):
        branch_url = provider.build_repo_url('branches', provider.default_branch)
        tree_url = provider.build_repo_url('git', 'trees',
                                           branch_metadata['commit']['commit']['tree']['sha'],
                                           recursive=1)

        aiohttpretty.register_json_uri('GET', branch_url, body=branch_metadata)
        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)

        tree_path = 'level1'

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + tree_path + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + tree_path)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + tree_path + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_revision(self, provider, branch_metadata, comparision_metadata, branch_list_metadata, repo_tree_metadata_root):

        params = {'ref' : 'a0f1708eef358227cb0bc1dbd29b604086e71a4e'}

        branch_url = provider.build_repo_url('branches', 'master')
        branches_url = provider.build_repo_url('branches')
        compare_url = provider.build_repo_url('compare', '{}...{}'.format('master', params['ref']))
        tree_url = provider.build_repo_url('git', 'trees',
                                       branch_metadata['commit']['commit']['tree']['sha'], recursive=1)

        aiohttpretty.register_json_uri('GET', branches_url, body=branch_list_metadata)
        aiohttpretty.register_json_uri('GET', branch_url, body=branch_metadata)
        aiohttpretty.register_json_uri('GET', compare_url, body=comparision_metadata)
        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)

        blob_path = 'file.txt'

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + blob_path, **params)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + blob_path + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + blob_path)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    async def test_reject_multiargs(self, provider):

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await provider.validate_v1_path('/foo', ref=['bar','baz'])

        assert exc.value.code == client.BAD_REQUEST

        with pytest.raises(exceptions.InvalidParameters) as exc:
            await provider.validate_path('/foo', ref=['bar','baz'])

        assert exc.value.code == client.BAD_REQUEST

    @pytest.mark.asyncio
    async def test_validate_path(self, provider):
        path = await provider.validate_path('/this/is/my/path')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == (provider.default_branch, None)
        assert path.parts[0].identifier ==  (provider.default_branch, None)


    @pytest.mark.asyncio
    async def test_validate_path_passes_branch(self, provider):
        path = await provider.validate_path('/this/is/my/path', branch='NotMaster')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == ('NotMaster', None)
        assert path.parts[0].identifier ==  ('NotMaster', None)

    @pytest.mark.asyncio
    async def test_validate_path_passes_ref(self, provider):
        path = await provider.validate_path('/this/is/my/path', ref='NotMaster')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == ('NotMaster', None)
        assert path.parts[0].identifier ==  ('NotMaster', None)

    @pytest.mark.asyncio
    async def test_validate_path_passes_file_sha(self, provider):
        path = await provider.validate_path('/this/is/my/path', fileSha='Thisisasha')

        assert path.is_dir is False
        assert path.is_file is True
        assert path.name == 'path'
        assert isinstance(path.identifier, tuple)
        assert path.identifier == (provider.default_branch, 'Thisisasha')
        assert path.parts[0].identifier ==  (provider.default_branch, None)


class TestCRUD:

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_download_by_file_sha(self, provider, content_repo_metadata_root_file_txt):
    #     ref = hashlib.sha1().hexdigest()
    #     url = provider.build_repo_url('git', 'refs', 'heads', 'master')
    #     path = WaterButlerPath('/file.txt', _ids=(None, ('master', ref)))

    #     aiohttpretty.register_uri('GET', url, body=b'delicious')
    #     aiohttpretty.register_json_uri('GET', url, body={'object': {'sha': ref}})

    #     result = await provider.download(path)

    #     content = await result.read()
    #     assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_path(self, provider, repo_tree_metadata_root):
        ref = hashlib.sha1().hexdigest()
        file_sha = repo_tree_metadata_root['tree'][0]['sha']
        path = await provider.validate_path('/file.txt')

        url = provider.build_repo_url('git', 'blobs', file_sha)
        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        latest_sha_url = provider.build_repo_url('git', 'refs', 'heads', path.identifier[0])
        commit_url = provider.build_repo_url('commits', path=path.path.lstrip('/'), sha=path.identifier[0])

        aiohttpretty.register_uri('GET', url, body=b'delicious')
        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)
        aiohttpretty.register_json_uri('GET', commit_url, body=[{'commit': {'tree': {'sha': ref}}}])

        result = await provider.download(path)
        content = await result.read()
        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_path_ref_branch(self, provider, repo_tree_metadata_root):
        ref = hashlib.sha1().hexdigest()
        file_sha = repo_tree_metadata_root['tree'][0]['sha']
        path = await provider.validate_path('/file.txt', branch='other_branch')

        url = provider.build_repo_url('git', 'blobs', file_sha)
        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url('commits', path=path.path.lstrip('/'), sha=path.identifier[0])

        aiohttpretty.register_uri('GET', url, body=b'delicious')
        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)
        aiohttpretty.register_json_uri('GET', commit_url, body=[{'commit': {'tree': {'sha': ref}}}])

        result = await provider.download(path)
        content = await result.read()
        assert content == b'delicious'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_by_path_revision(self, provider, repo_tree_metadata_root):
        ref = hashlib.sha1().hexdigest()
        file_sha = repo_tree_metadata_root['tree'][0]['sha']
        path = await provider.validate_path('/file.txt', branch='other_branch')

        url = provider.build_repo_url('git', 'blobs', file_sha)
        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url('commits', path=path.path.lstrip('/'), sha='Just a test')

        aiohttpretty.register_uri('GET', url, body=b'delicious')
        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)
        aiohttpretty.register_json_uri('GET', commit_url, body=[{'commit': {'tree': {'sha': ref}}}])

        result = await provider.download(path, revision='Just a test')
        content = await result.read()
        assert content == b'delicious'

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_download_bad_status(self, provider):
    #     ref = hashlib.sha1().hexdigest()
    #     url = provider.build_repo_url('git', 'blobs', ref)
    #     aiohttpretty.register_uri('GET', url, body=b'delicious', status=418)
    #     with pytest.raises(exceptions.DownloadError):
    #         await provider.download('', fileSha=ref)

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_upload_create(self, provider, upload_response, file_content, file_stream):
    #     message = 'so hungry'
    #     path = upload_response['content']['path'][::-1]
    #     metadata_url = provider.build_repo_url('contents', os.path.dirname(path))
    #     aiohttpretty.register_json_uri('GET', metadata_url, body=[upload_response['content']], status=200)
    #     upload_url = provider.build_repo_url('contents', path)
    #     aiohttpretty.register_json_uri('PUT', upload_url, body=upload_response, status=201)
    #     await provider.upload(file_stream, path, message)
    #     expected_data = {
    #         'path': path,
    #         'message': message,
    #         'content': base64.b64encode(file_content).decode('utf-8'),
    #         'committer': provider.committer,
    #     }
    #     assert aiohttpretty.has_call(method='GET', uri=metadata_url)
    #     assert aiohttpretty.has_call(method='PUT', uri=upload_url, data=json.dumps(expected_data))
    #
    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_upload_update(self, provider, upload_response, file_content, file_stream):
    #     message = 'so hungry'
    #     sha = upload_response['content']['sha']
    #     path = '/' + upload_response['content']['path']
    #
    #     upload_url = provider.build_repo_url('contents', provider.build_path(path))
    #     metadata_url = provider.build_repo_url('contents', os.path.dirname(path))
    #
    #     aiohttpretty.register_json_uri('PUT', upload_url, body=upload_response)
    #     aiohttpretty.register_json_uri('GET', metadata_url, body=[upload_response['content']])
    #
    #     await provider.upload(file_stream, path, message)
    #
    #     expected_data = {
    #         'path': path,
    #         'message': message,
    #         'content': base64.b64encode(file_content).decode('utf-8'),
    #         'committer': provider.committer,
    #         'sha': sha,
    #     }
    #
    #     assert aiohttpretty.has_call(method='GET', uri=metadata_url)
    #     assert aiohttpretty.has_call(method='PUT', uri=upload_url, data=json.dumps(expected_data))

    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_delete_with_branch(self, provider, repo_contents):
    #     path = os.path.join('/', repo_contents[0]['path'])
    #     sha = repo_contents[0]['sha']
    #     branch = 'master'
    #     message = 'deleted'
    #     url = provider.build_repo_url('contents', path)
    #     aiohttpretty.register_json_uri('DELETE', url)
    #     await provider.delete(path, message, sha, branch=branch)
    #     expected_data = {
    #         'message': message,
    #         'sha': sha,
    #         'committer': provider.committer,
    #         'branch': branch,
    #     }
    #
    #     assert aiohttpretty.has_call(method='DELETE', uri=url, data=json.dumps(expected_data))
    #
    # @pytest.mark.asyncio
    # @pytest.mark.aiohttpretty
    # async def test_delete_without_branch(self, provider, repo_contents):
    #     path = repo_contents[0]['path']
    #     sha = repo_contents[0]['sha']
    #     message = 'deleted'
    #     url = provider.build_repo_url('contents', path)
    #     aiohttpretty.register_json_uri('DELETE', url)
    #     await provider.delete(path, message, sha)
    #     expected_data = {
    #         'message': message,
    #         'sha': sha,
    #         'committer': provider.committer,
    #     }
    #
    #     assert aiohttpretty.has_call(method='DELETE', uri=url, data=json.dumps(expected_data))


class TestMetadata:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file(self, provider, repo_metadata, repo_tree_metadata_root):
        ref = hashlib.sha1().hexdigest()
        path = await provider.validate_path('/file.txt')

        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url('commits', path=path.path.lstrip('/'), sha=path.identifier[0])

        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)
        aiohttpretty.register_json_uri('GET', commit_url, body=[{
            'commit': {
                'tree': {'sha': ref},
                'author': {'date': '1970-01-02T03:04:05Z'}
            },
        }])

        result = await provider.metadata(path)
        item = repo_tree_metadata_root['tree'][0]
        web_view = provider._web_view(path=path)

        assert result == GitHubFileTreeMetadata(item, web_view=web_view, commit={
            'tree': {'sha': ref}, 'author': {'date': '1970-01-02T03:04:05Z'}
        }, ref=path.identifier[0])

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_doesnt_exist(self, provider, repo_metadata, repo_tree_metadata_root):
        ref = hashlib.sha1().hexdigest()
        path = await provider.validate_path('/file.txt')

        tree_url = provider.build_repo_url('git', 'trees', ref, recursive=1)
        commit_url = provider.build_repo_url('commits', path=path.path.lstrip('/'), sha=path.identifier[0])

        aiohttpretty.register_json_uri('GET', tree_url, body=repo_tree_metadata_root)
        aiohttpretty.register_json_uri('GET', commit_url, body=[])

        with pytest.raises(exceptions.NotFoundError):
            await provider.metadata(path)

    # TODO: Additional Tests
    # async def test_metadata_root_file_txt_branch(self, provider, repo_metadata, branch_metadata, repo_metadata_root):
    # async def test_metadata_root_file_txt_commit_sha(self, provider, repo_metadata, branch_metadata, repo_metadata_root):

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_root(self, provider, repo_metadata, content_repo_metadata_root):
        path = await provider.validate_path('/')

        url = provider.build_repo_url('contents', path.path, ref=provider.default_branch)
        aiohttpretty.register_json_uri('GET', url, body=content_repo_metadata_root)

        result = await provider.metadata(path)

        ret = []
        for item in content_repo_metadata_root:
            if item['type'] == 'dir':
                ret.append(GitHubFolderContentMetadata(item, ref=provider.default_branch))
            else:
                ret.append(GitHubFileContentMetadata(item, web_view=item['html_url'], ref=provider.default_branch))

        assert result == ret

    # TODO: Additional Tests
    # async def test_metadata_non_root_folder(self, provider, repo_metadata, branch_metadata, repo_metadata_root):
    # async def test_metadata_non_root_folder_branch(self, provider, repo_metadata, branch_metadata, repo_metadata_root):
    # async def test_metadata_non_root_folder_commit_sha(self, provider, repo_metadata, branch_metadata, repo_metadata_root):


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_errors_out(self, provider, repo_metadata):
        path = await provider.validate_path('/Imarealboy/')
        url = provider.build_repo_url('contents', path.child('.gitkeep').path)

        aiohttpretty.register_uri('PUT', url, status=400)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider, repo_metadata):
        path = await provider.validate_path('/Imarealboy')

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider, repo_metadata):
        path = await provider.validate_path('/Imarealboy/')
        url = provider.build_repo_url('contents', os.path.join(path.path, '.gitkeep'))

        aiohttpretty.register_json_uri('PUT', url, status=422, body={
            'message': 'Invalid request.\n\n"sha" wasn\'t supplied.'
        })

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == 'Cannot create folder "Imarealboy" because a file or folder already exists at path "/Imarealboy/"'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_other_422(self, provider, repo_metadata):
        path = await provider.validate_path('/Imarealboy/')
        url = provider.build_repo_url('contents', os.path.join(path.path, '.gitkeep'))

        aiohttpretty.register_json_uri('PUT', url, status=422, body={
            'message': 'github no likey'
        })

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 422
        assert e.value.data == {'message': 'github no likey'}

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, repo_metadata, create_folder_response):
        path = await provider.validate_path('/i/like/trains/')
        url = provider.build_repo_url('contents', os.path.join(path.path, '.gitkeep'))

        aiohttpretty.register_json_uri('PUT', url, status=201, body=create_folder_response)

        metadata = await provider.create_folder(path)

        assert metadata.kind == 'folder'
        assert metadata.name == 'trains'
        assert metadata.path == '/i/like/trains/'


class TestUtilities:

    def test__path_exists_in_tree(self, provider, nested_tree_metadata):
        _ids = [('master', '')]

        assert provider._path_exists_in_tree(nested_tree_metadata['tree'], GitHubPath('/alpha.txt', _ids=_ids))
        assert provider._path_exists_in_tree(nested_tree_metadata['tree'], GitHubPath('/beta/', _ids=_ids))
        assert not provider._path_exists_in_tree(nested_tree_metadata['tree'], GitHubPath('/gaw-gai.txt', _ids=_ids))
        assert not provider._path_exists_in_tree(nested_tree_metadata['tree'], GitHubPath('/kaw-kai/', _ids=_ids))

    def test__remove_path_from_tree(self, provider, nested_tree_metadata):
        _ids = [('master', '')]

        simple_file_tree = provider._remove_path_from_tree(nested_tree_metadata['tree'], GitHubPath('/alpha.txt', _ids=_ids))
        assert len(simple_file_tree) == (len(nested_tree_metadata['tree']) - 1)
        assert 'alpha.txt' not in [x['path'] for x in simple_file_tree]

        simple_folder_tree = provider._remove_path_from_tree(nested_tree_metadata['tree'], GitHubPath('/beta/', _ids=_ids))
        assert len(simple_folder_tree) == 1
        assert simple_folder_tree[0]['path'] == 'alpha.txt'

        nested_file_tree = provider._remove_path_from_tree(nested_tree_metadata['tree'], GitHubPath('/beta/gamma.txt', _ids=_ids))
        assert len(nested_file_tree) == (len(nested_tree_metadata['tree']) - 1)
        assert 'beta/gamma.txt' not in [x['path'] for x in nested_file_tree]

        nested_folder_tree = provider._remove_path_from_tree(nested_tree_metadata['tree'], GitHubPath('/beta/delta/', _ids=_ids))
        assert len(nested_folder_tree) == 3
        assert len([x for x in nested_folder_tree if x['path'].startswith('beta/delta')]) == 0

        missing_file_tree = provider._remove_path_from_tree(nested_tree_metadata['tree'], GitHubPath('/bet', _ids=_ids))
        assert missing_file_tree == nested_tree_metadata['tree']

        missing_folder_tree = provider._remove_path_from_tree(nested_tree_metadata['tree'], GitHubPath('/beta/gam/', _ids=_ids))
        assert missing_file_tree == nested_tree_metadata['tree']

    def test__reparent_blobs(self, provider, nested_tree_metadata):
        _ids = [('master', '')]

        file_rename_blobs = copy.deepcopy([x for x in nested_tree_metadata['tree'] if x['path'] == 'alpha.txt'])
        provider._reparent_blobs(file_rename_blobs, GitHubPath('/alpha.txt', _ids=_ids), GitHubPath('/zeta.txt', _ids=_ids))
        assert len(file_rename_blobs) == 1
        assert file_rename_blobs[0]['path'] == 'zeta.txt'

        folder_rename_blobs = copy.deepcopy([x for x in nested_tree_metadata['tree'] if x['path'].startswith('beta')])
        provider._reparent_blobs(folder_rename_blobs, GitHubPath('/beta/', _ids=_ids), GitHubPath('/theta/', _ids=_ids))
        assert len(folder_rename_blobs) == 4  # beta/, gamma.txt, delta/, epsilon.txt
        assert len([x for x in folder_rename_blobs if x['path'].startswith('theta/')]) == 3  # gamma.txt, delta/, epsilon.txt
        assert len([x for x in folder_rename_blobs if x['path'] == 'theta']) == 1  # theta/


    def test__prune_subtrees(self, provider, nested_tree_metadata):
        pruned_tree = provider._prune_subtrees(nested_tree_metadata['tree'])
        assert len(pruned_tree) == 3  # alpha.txt, gamma.txt, epsilon.txt
        assert len([x for x in pruned_tree if x['type'] == 'tree']) == 0
