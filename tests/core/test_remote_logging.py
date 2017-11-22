import pytest

from waterbutler.core import remote_logging


class TestScubPayloadForKeen:

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
        assert result == payload

    def test_flat_dict_needs_scrubbing(self):
        payload = {
            'key.test': 'value',
            'key2': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'keytest': 'value',
            'key2': 'value2'
        }
        assert result != payload

    def test_scrub_and_rename(self):
        payload = {
            'key.test': 'value2',
            'keytest': 'value2'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        # it will rename whichever one comes second, even though it is the original in this case
        assert result == {
            'keytest': 'value2',
            'keytest (1)': 'value2'
        }
        assert result != payload

    def test_scrub_and_loop_rename(self):
        payload = {
            'key.test': 'value',
            'keytest': 'value',
            'key..test': 'value'
        }

        result = remote_logging._scrub_headers_for_keen(payload)

        assert result == {
            'keytest': 'value',
            'keytest (1)': 'value',
            'keytest (2)': 'value'

        }
        assert result != payload
