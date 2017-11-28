import pytest

from waterbutler.core import remote_logging


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

        # it will rename whichever one comes second, even though it is the original in this case

        assert result == {
            'key-test': 'value2',
            'key-test (1)': 'unique value'
        }

    def test_scrub_and_loop_rename(self):
        payload = {
            'key.test': 'value1',
            'key-test': 'value2',
            'key-test (1)': 'value3'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'key-test': 'value2',
            'key-test (2)': 'value1',
            'key-test (1)': 'value3'

        }

    def test_max_iteration(self):
        payload = {
            'key.test': 'value1',
            'key-test': 'value2',
            'key-test (1)': 'value3'
        }

        result = remote_logging._scrub_headers_for_keen(payload, MAX_ITERATIONS=1)

        assert result == {
            'key-test': 'value2',
            'key-test (1)': 'value3'
        }
