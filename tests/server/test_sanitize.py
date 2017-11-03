import pytest
from unittest import mock

from waterbutler.server.sanitize import WBSanitizer


@pytest.fixture
def sanitizer():
    return WBSanitizer(mock.Mock())


class TestWBSanitizer:
    # The sanitize function changes some strings and dictionaries
    # you put into it, so you need to explicitly test most things

    MASK = '*' * 8

    def test_no_sanitization(self, sanitizer):
        assert sanitizer.sanitize('thing', 'ghost science') == 'ghost science'

    def test_fields_sanitized(self, sanitizer):
        fields = sanitizer.FIELDS
        for field in fields:
            assert sanitizer.sanitize(field, 'free speech') == self.MASK

    def test_value_is_none(self, sanitizer):
        assert sanitizer.sanitize('great hair', None) is None

    def test_sanitize_credit_card(self, sanitizer):
        assert sanitizer.sanitize('credit', '424242424242424') == self.MASK
        assert sanitizer.sanitize('credit', '4242424242424243333333') != self.MASK

    def test_sanitize_dictionary(self, sanitizer):
        value_dict = {
            'great_entry': 'very much not a secret or credit card'
        }

        result = sanitizer.sanitize('value_dict', value_dict)
        assert result == {
            'great_entry': 'very much not a secret or credit card'
        }

        sanitize_dict = {
            'key': 'secret',
            'okay_value': 'bears are awesome'
        }
        result = result = sanitizer.sanitize('sanitize_dict', sanitize_dict)

        # Sanity check
        assert result != {
            'key': 'secret',
            'okay_value': 'bears are awesome'
        }

        assert result == {
            'key': '*' * 8,
            'okay_value': 'bears are awesome'
        }

    def test_dataverse_secret(self, sanitizer):

        # Named oddly because if you call it `dv_secret` it will get sanitized by a different
        # part of the sanitizer
        dv_value = 'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
        assert sanitizer.sanitize('dv_value', dv_value) == self.MASK

        dv_value = 'random characters and other things  aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
        expected = 'random characters and other things  ' + self.MASK
        assert sanitizer.sanitize('dv_value', dv_value) == expected

    def test_bytes(self, sanitizer):
        key = b'key'
        assert sanitizer.sanitize(key, 'bossy yogurt') == self.MASK

        other_key = b'should_be_safe'
        assert sanitizer.sanitize(other_key, 'snow science') == 'snow science'
