import pytest

from waterbutler.core import remote_logging


class TestLogToCallback:

    @pytest.mark.asyncio
    @pytest.mark.parametrize('completed, expected', [(False, False), (True, True)])
    async def test_download_action_sets_completed_flag(self, monkeypatch, completed, expected):
        captured = {}

        async def fake_send_signed_request(method, url, payload):
            captured['payload'] = payload
            return 200, b'success'

        monkeypatch.setattr(remote_logging.utils, 'send_signed_request', fake_send_signed_request)

        class DummySource:
            auth = {'callback_url': 'https://example.com/callback'}

            def serialize(self):
                return {'provider': 'osf'}

        source = DummySource()
        request = {
            'request': {
                'method': 'GET',
                'url': 'https://example.com/file',
                'headers': {},
            },
            'referrer': {'url': None},
            'tech': {'ua': 'test-agent', 'ip': '127.0.0.1'},
        }

        await remote_logging.log_to_callback(
            'download_file',
            source=source,
            request=request,
            completed=completed,
        )

        assert captured['payload']['action_meta']['completed'] is expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize('status_code', [200, 500, None])
    async def test_download_action_forwards_status_code(self, monkeypatch, status_code):
        """The status lets the OSF tell a real failure from a user cancelling mid-stream."""
        captured = {}

        async def fake_send_signed_request(method, url, payload):
            captured['payload'] = payload
            return 200, b'success'

        monkeypatch.setattr(remote_logging.utils, 'send_signed_request', fake_send_signed_request)

        class DummySource:
            auth = {'callback_url': 'https://example.com/callback'}

            def serialize(self):
                return {'provider': 'osf'}

        source = DummySource()
        request = {
            'request': {
                'method': 'GET',
                'url': 'https://example.com/file',
                'headers': {},
            },
            'referrer': {'url': None},
            'tech': {'ua': 'test-agent', 'ip': '127.0.0.1'},
        }

        await remote_logging.log_to_callback(
            'download_zip',
            source=source,
            request=request,
            completed=False,
            status_code=status_code,
        )

        assert captured['payload']['action_meta']['status_code'] == status_code

    @pytest.mark.asyncio
    async def test_non_download_action_omits_completed_flag(self, monkeypatch):
        captured = {}

        async def fake_send_signed_request(method, url, payload):
            captured['payload'] = payload
            return 200, b'success'

        monkeypatch.setattr(remote_logging.utils, 'send_signed_request', fake_send_signed_request)

        class DummySource:
            auth = {'callback_url': 'https://example.com/callback'}

            def serialize(self):
                return {'provider': 'osf'}

        source = DummySource()
        request = {
            'request': {
                'method': 'GET',
                'url': 'https://example.com/file',
                'headers': {},
            },
            'referrer': {'url': None},
            'tech': {'ua': 'test-agent', 'ip': '127.0.0.1'},
        }

        await remote_logging.log_to_callback('create', source=source, request=request)

        assert 'completed' not in captured['payload']['action_meta']

    @pytest.mark.asyncio
    async def test_download_action_forwards_link_tags(self, monkeypatch):
        captured = {}

        async def fake_send_signed_request(method, url, payload):
            captured['payload'] = payload
            return 200, b'success'

        monkeypatch.setattr(remote_logging.utils, 'send_signed_request', fake_send_signed_request)

        class DummySource:
            auth = {'callback_url': 'https://example.com/callback'}

            def serialize(self):
                return {'provider': 'osf'}

        source = DummySource()
        request = {
            'request': {
                'method': 'GET',
                'url': 'https://example.com/folder?zip=&source=files&tz=Europe%2FKyiv',
                'headers': {},
            },
            'referrer': {'url': None},
            'tech': {'ua': 'test-agent', 'ip': '127.0.0.1'},
        }

        await remote_logging.log_to_callback('download_zip', source=source, request=request)

        assert captured['payload']['action_meta']['source'] == 'files'
        assert captured['payload']['action_meta']['tz'] == 'Europe/Kyiv'

    @pytest.mark.asyncio
    async def test_download_action_omits_absent_link_tags(self, monkeypatch):
        captured = {}

        async def fake_send_signed_request(method, url, payload):
            captured['payload'] = payload
            return 200, b'success'

        monkeypatch.setattr(remote_logging.utils, 'send_signed_request', fake_send_signed_request)

        class DummySource:
            auth = {'callback_url': 'https://example.com/callback'}

            def serialize(self):
                return {'provider': 'osf'}

        source = DummySource()
        request = {
            'request': {
                'method': 'GET',
                'url': 'https://example.com/file',
                'headers': {},
            },
            'referrer': {'url': None},
            'tech': {'ua': 'test-agent', 'ip': '127.0.0.1'},
        }

        await remote_logging.log_to_callback('download_file', source=source, request=request)

        assert 'source' not in captured['payload']['action_meta']
        assert 'tz' not in captured['payload']['action_meta']

    @pytest.mark.asyncio
    async def test_download_link_tags_are_length_capped(self, monkeypatch):
        captured = {}

        async def fake_send_signed_request(method, url, payload):
            captured['payload'] = payload
            return 200, b'success'

        monkeypatch.setattr(remote_logging.utils, 'send_signed_request', fake_send_signed_request)

        class DummySource:
            auth = {'callback_url': 'https://example.com/callback'}

            def serialize(self):
                return {'provider': 'osf'}

        source = DummySource()
        oversized = 'f' * (remote_logging.MAX_DOWNLOAD_TAG_LENGTH + 50)
        request = {
            'request': {
                'method': 'GET',
                'url': f'https://example.com/file?source={oversized}',
                'headers': {},
            },
            'referrer': {'url': None},
            'tech': {'ua': 'test-agent', 'ip': '127.0.0.1'},
        }

        await remote_logging.log_to_callback('download_file', source=source, request=request)

        assert len(captured['payload']['action_meta']['source']) == \
            remote_logging.MAX_DOWNLOAD_TAG_LENGTH


class TestScrubPayloadForKeen:

    def test_flat_dict(self):
        payload = {
            'key': 'value',
            'key2': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key': 'value',
            'key2': 'value2'
        }

    def test_flat_dict_needs_scrubbing(self):
        payload = {
            'key.test': 'value',
            'key2': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key-test': 'value',
            'key2': 'value2'
        }

    def test_scrub_and_rename(self):
        payload = {
            'key.test': 'unique value',
            'key-test': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        # "key.test" sorts after "key-test" and will therefore be renamed
        assert result == {
            'key-test': 'value2',
            'key-test-1': 'unique value'
        }

    def test_scrub_and_loop_rename(self):
        payload = {
            'key.test': 'value1',
            'key-test': 'value2',
            'key-test-1': 'value3'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key-test': 'value2',
            'key-test-2': 'value1',
            'key-test-1': 'value3'

        }

    def test_max_iteration(self):
        payload = {
            'key.test': 'value1',
            'key-test': 'value2',
            'key-test-1': 'value3'
        }

        result = remote_logging._scrub_headers_for_keen(payload, MAX_ITERATIONS=1)

        assert result == {
            'key-test': 'value2',
            'key-test-1': 'value3'
        }
