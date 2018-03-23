import pytest


@pytest.fixture()
def mock_auth():
    return {'name': 'Roger Deng', 'email': 'roger@deng.com'}


@pytest.fixture()
def mock_auth_2():
    return {'name': 'Deng Roger', 'email': 'deng@roger.com'}


@pytest.fixture()
def mock_creds():
    return {
        'json_creds': {
            "__comment": "Don't worry, this is a specially crafted fake credential that looks genuine!",
            "type": "service_account",
            "project_id": "gcloud-fake-000000",
            "private_key_id": "1010101010101010101010101010101010101010",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDAPR+9enJSIVDI\n/HRg/RqRLamGD9uVTviK773evzHJv3vve714TpI5uK39lIhY4Xv7U5hpvcL+P9JV\nkzfAc21mI9F29GCUP8WE3r52SBHC/5uQizaAyl1HD07gzVzI+flw1cuT+bBwo0k/\nXc3qHDD7lRSlD6p8BKScO+zL+Mvjo/dCYOFyolbCH99FgNd3EXn6gFe04a+rlvWs\nKJp+syqa/j6XMff68MG5sEQTeD4tI/jqCeZqR2TrGy0riC7htyJROxo3k5oFwAs8\nLxlRXGZmBf9ECWHan47d3kOEmGeuhRt9VN6mlj66m4kWM3eSOz+r4GEDumv3n/GZ\nJMzP85d9AgMBAAECggEABnQG1L1/iPJFW/ndjkPw2F7IChAONvznqwJRRWD1sugS\nqP9mZNt+XSGt9Y0+5nzrRIyR2TrdiNtrnCPNA1Dco0kghvW9KDzbzJINorrYncsm\n1btWoQwqBXCmHTXHn5eEoB0NMHJ5Uc6pbs8fUnSP0GI4xzG3b2JYhEYetNotPf78\nIJIufn6SSH3YPtBjQ8gosfBUWa7AH3iX2YZX1VciOgDJxLlBhczQvhU+0Wrx9Np4\nTBAiPSsDbG2SNAmtHWWlynKoD3qlC11P421ZlJQNL+EDv9Xq1nhJ5RVMeyUhZfV8\nAgZcDZ4c6uzSwhqjdKSdNiE4G7AjiSi/deCb4++1AQKBgQDzbOAElWnt5Zg3LhrT\n6F4eApz3ipEzguzeKJKQ/s5ujdUfkgyQTr3ugUPt5htASHPyDdjFfQIY2xHu2mgI\nWTifHxovm8h3IfsWUkgMf38/Q8S2AGFecP7BQuUxcXl9ISRkpsyl6iBqiZdQhI+w\n2BZJ0Q+eQKWs4KOH/3OE3wQ9+QKBgQDKK1gC8+dUfY/y8/NFKrHoSp6v/UUans55\n8aTGC6Zp/8bv1VFcnJEEOKhGgd7tKQ9PpEmHpAxxXS+R6Vfe9gUfk9JtnkV9a2hr\nDZkzsY7ARsUNllAyYOLa8aQtBPg4Y53s40oTCXlB6v7ntGLlh93+2DSStvNS9As8\nJVIfYhZWpQKBgDOLKUAvNxfllr6QZ6PZPxyRpxUPZUGIBongA3DqU2G7bJZbwYdI\n9RSskGquX7TT4qEtZ2oh1zDWKkzuODsUUVX4Kv5LuT3olxcZ4yGqWZJW0i9Lk1KB\nEKBxfsBia7wgKWmanBjBo42LhtvIxfhHOSj2OJ1kyO/7PQVOBPLsmiRZAoGAYKme\nuDK0LaKnfAuGClEipSVggFcBfnvlz0ppdUPGurHQBYYiE7zMXY9VbfjUhOJ37qVn\nftJCHMXoY8SE/hb1VibQmxbstM3xLBZhZOUFkwuDVj7Dc4L9lJ+q1tekcxm2Pbhj\nB30lHA5m+JQ4IPT4gMwRPwD4kHYdFRHnFRhKA/0CgYA1JDqR3vhcxrUNM/P0Yw0j\ncNXnC1py69q3r6ZbPXa1cgtIsEY37KB0c9+Fk4p8u8BhxRRjBHHlqMEVbGosVbu6\nkQ2fo4oUAeIkfW+FtPE4gt+41sPkTugPybfFRchnB1DmEU7DfasirB/blxUyeddY\nhHBbcqXJ6skziEqqcb43gw==\n-----END PRIVATE KEY-----\n",
            "client_email": "gcloud-client-fake@gcloud-fake-000000.iam.gserviceaccount.com",
            "client_id": "101010101010101010101",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gcloud-client-fake%40gcloud-fake-000000.iam.gserviceaccount.com"
        }

    }


@pytest.fixture()
def mock_creds_2():
    return {
        'json_creds': {
            "__comment": "Don't worry, this is another specially crafted fake credential that looks genuine!",
            "type": "service_account",
            "project_id": "gcloud-fake-000001",
            "private_key_id": "0101010101010101010101010101010101010101",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDAPR+9enJSIVDI\n/HRg/RqRLamGD9uVTviK773evzHJv3vve714TpI5uK39lIhY4Xv7U5hpvcL+P9JV\nkzfAc21mI9F29GCUP8WE3r52SBHC/5uQizaAyl1HD07gzVzI+flw1cuT+bBwo0k/\nXc3qHDD7lRSlD6p8BKScO+zL+Mvjo/dCYOFyolbCH99FgNd3EXn6gFe04a+rlvWs\nKJp+syqa/j6XMff68MG5sEQTeD4tI/jqCeZqR2TrGy0riC7htyJROxo3k5oFwAs8\nLxlRXGZmBf9ECWHan47d3kOEmGeuhRt9VN6mlj66m4kWM3eSOz+r4GEDumv3n/GZ\nJMzP85d9AgMBAAECggEABnQG1L1/iPJFW/ndjkPw2F7IChAONvznqwJRRWD1sugS\nqP9mZNt+XSGt9Y0+5nzrRIyR2TrdiNtrnCPNA1Dco0kghvW9KDzbzJINorrYncsm\n1btWoQwqBXCmHTXHn5eEoB0NMHJ5Uc6pbs8fUnSP0GI4xzG3b2JYhEYetNotPf78\nIJIufn6SSH3YPtBjQ8gosfBUWa7AH3iX2YZX1VciOgDJxLlBhczQvhU+0Wrx9Np4\nTBAiPSsDbG2SNAmtHWWlynKoD3qlC11P421ZlJQNL+EDv9Xq1nhJ5RVMeyUhZfV8\nAgZcDZ4c6uzSwhqjdKSdNiE4G7AjiSi/deCb4++1AQKBgQDzbOAElWnt5Zg3LhrT\n6F4eApz3ipEzguzeKJKQ/s5ujdUfkgyQTr3ugUPt5htASHPyDdjFfQIY2xHu2mgI\nWTifHxovm8h3IfsWUkgMf38/Q8S2AGFecP7BQuUxcXl9ISRkpsyl6iBqiZdQhI+w\n2BZJ0Q+eQKWs4KOH/3OE3wQ9+QKBgQDKK1gC8+dUfY/y8/NFKrHoSp6v/UUans55\n8aTGC6Zp/8bv1VFcnJEEOKhGgd7tKQ9PpEmHpAxxXS+R6Vfe9gUfk9JtnkV9a2hr\nDZkzsY7ARsUNllAyYOLa8aQtBPg4Y53s40oTCXlB6v7ntGLlh93+2DSStvNS9As8\nJVIfYhZWpQKBgDOLKUAvNxfllr6QZ6PZPxyRpxUPZUGIBongA3DqU2G7bJZbwYdI\n9RSskGquX7TT4qEtZ2oh1zDWKkzuODsUUVX4Kv5LuT3olxcZ4yGqWZJW0i9Lk1KB\nEKBxfsBia7wgKWmanBjBo42LhtvIxfhHOSj2OJ1kyO/7PQVOBPLsmiRZAoGAYKme\nuDK0LaKnfAuGClEipSVggFcBfnvlz0ppdUPGurHQBYYiE7zMXY9VbfjUhOJ37qVn\nftJCHMXoY8SE/hb1VibQmxbstM3xLBZhZOUFkwuDVj7Dc4L9lJ+q1tekcxm2Pbhj\nB30lHA5m+JQ4IPT4gMwRPwD4kHYdFRHnFRhKA/0CgYA1JDqR3vhcxrUNM/P0Yw0j\ncNXnC1py69q3r6ZbPXa1cgtIsEY37KB0c9+Fk4p8u8BhxRRjBHHlqMEVbGosVbu6\nkQ2fo4oUAeIkfW+FtPE4gt+41sPkTugPybfFRchnB1DmEU7DfasirB/blxUyeddY\nhHBbcqXJ6skziEqqcb43gw==\n-----END PRIVATE KEY-----\n",
            "client_email": "gcloud-client-fake@gcloud-fake-000001.iam.gserviceaccount.com",
            "client_id": "010101010101010101010",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/gcloud-client-fake%40gcloud-fake-000001.iam.gserviceaccount.com"
        }

    }


@pytest.fixture()
def mock_settings():
    return {'bucket': 'gcloud-test.longzechen.com'}


@pytest.fixture()
def mock_settings_2():
    return {'bucket': 'gcloud-test-2.longzechen.com'}
