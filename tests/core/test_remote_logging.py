import pytest

from waterbutler.core import remote_logging


class TestScubPayloadForKeen:

    def test_flat_dict(self):
        payload = {
            'key': 'value',
            'key2': 'value2'
        }

        result = remote_logging._scrub_payload_for_keen(payload)

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

        result = remote_logging._scrub_payload_for_keen(payload)

        assert result == {
            'keytest': 'value',
            'key2': 'value2'
        }
        assert result != payload

    def test_scrub_and_rename(self):
        payload = {
            'key.test': 'value',
            'keytest': 'value2'
        }

        result = remote_logging._scrub_payload_for_keen(payload)

        # it will rename whichever one comes second, even though it is the original in this case
        assert result == {
            'keytest': 'value',
            'keytest (1)': 'value2'
        }
        assert result != payload

    def test_scrub_and_loop_rename(self):
        payload = {
            'key.test': 'value',
            'keytest': 'value',
            'key..test': 'value'
        }

        result = remote_logging._scrub_payload_for_keen(payload)

        assert result == {
            'keytest': 'value',
            'keytest (1)': 'value',
            'keytest (2)': 'value'

        }
        assert result != payload

    def test_nested_dict(self):
        payload = {
            'key': {
                'resource': 'twitter',
                'other_dict': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }

        result = remote_logging._scrub_payload_for_keen(payload)

        assert result == {
            'key': {
                'resource': 'twitter',
                'other_dict': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }
        assert result == payload

    def test_nested_dict_scrub(self):
        payload = {
            'key.test': {
                'resource': 'twitter',
                'other_dict': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }

        result = remote_logging._scrub_payload_for_keen(payload)

        assert result == {
            'keytest': {
                'resource': 'twitter',
                'other_dict': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }
        assert result != payload

    def test_nested_dict_nested_scrub(self):
        payload = {
            'key': {
                'resource.twitter': 'twitter',
                'other_dict': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }

        result = remote_logging._scrub_payload_for_keen(payload)

        assert result == {
            'key': {
                'resourcetwitter': 'twitter',
                'other_dict': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }
        assert result != payload

    def test_nested_dict_nested_scrub_and_rename(self):
        payload = {
            'key': {
                'resource.twitter': 'twitter',
                'resourcetwitter': {
                    'very_nested': 'yes'
                }
            },
            'key2': 'value2'
        }

        result = remote_logging._scrub_payload_for_keen(payload)
        # Test is different from others so it passes on python 3.5
        assert 'resourcetwitter (1)' in result['key']
        assert 'resourcetwitter' in result['key']
        assert result != payload
