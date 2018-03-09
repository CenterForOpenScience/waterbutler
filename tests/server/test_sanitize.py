from unittest import mock

import pytest

from waterbutler.server.sanitize import WBSanitizer


@pytest.fixture
def sanitizer():
    client = mock.Mock()
    return WBSanitizer(client)  # WBSanitizier doesn't use client, but parent requires it


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

    def test_key_is_none(self, sanitizer):
        assert sanitizer.sanitize(None, 'best day ever') is 'best day ever'

    def test_sanitize_credit_card(self, sanitizer):
        assert sanitizer.sanitize('credit', '424242424242424') == self.MASK
        # This string is not censored since it is out of the range of what it considers
        # to be a credit card
        assert sanitizer.sanitize('credit', '4242424242424243333333') != self.MASK

    def test_none_key_is_sanitized(self, sanitizer):
        assert sanitizer.sanitize(None, '424242424242424') == self.MASK
        # This string is not censored since it is out of the range of what it considers
        # to be a credit card
        assert sanitizer.sanitize(None, '4242424242424243333333') != self.MASK

    def test_dataverse_secret(self, sanitizer):

        # Named oddly because if you call it `dv_secret` it will get sanitized by a different
        # part of the sanitizer
        dv_value = 'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
        assert sanitizer.sanitize('dv_value', dv_value) == self.MASK

        dv_value = 'random characters and other things  aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
        expected = 'random characters and other things  ' + self.MASK
        assert sanitizer.sanitize('dv_value', dv_value) == expected


        dv_value = 'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc-012345678-bbbb-bbbb-bbbb-cccccccccccc'
        expected = self.MASK + '-0' + self.MASK
        assert sanitizer.sanitize('dv_value', dv_value) == expected

        dv_value = 'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc-bbbb-bbbb-bbbb-cccccccccccc'
        expected = self.MASK + '-bbbb-bbbb-bbbb-cccccccccccc'
        assert sanitizer.sanitize('dv_value', dv_value) == expected


    def test_bytes(self, sanitizer):
        assert sanitizer.sanitize(b'key', 'bossy yogurt') == self.MASK
        assert sanitizer.sanitize(b'should_be_safe', 'snow science') == 'snow science'

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
        result = sanitizer.sanitize('sanitize_dict', sanitize_dict)

        assert result == {
            'key': self.MASK,
            'okay_value': 'bears are awesome'
        }

    def test_nested_dictionary(self, sanitizer):
        value_dict = {
            'value': {
                'other': 'words',
                'key': 'this will be censored',
                'secret': {
                    'secret': {
                        'secret': 'pie is great'
                    }
                },
                'new': 'best'
            }
        }

        result = sanitizer.sanitize('value_dict', value_dict)
        assert result == {
            'value': {
                'other': 'words',
                'key': self.MASK,
                'secret': self.MASK,
                'new': 'best'
            }
        }

    def test_nested_dictionary_with_list(self, sanitizer):
        value_dict = {
            'value': {
                'other': 'words',
                'key': 'this will be censored',
                'secret': {
                    'value': ['bunch', 'of', 'semi', 'random', 'beige', 'run']

                },
                'not_hidden': {
                    'list_of_dict': [
                        {'value': 'value'},
                        {'key': 'secret'}
                    ]
                },
                'new': 'best'
            }
        }
        result = sanitizer.sanitize('value_dict', value_dict)
        assert result == {
            'value': {
                'other': 'words',
                'key': self.MASK,
                'secret': self.MASK,
                'not_hidden': {
                    'list_of_dict': [
                        {'value': 'value'},
                        {'key': self.MASK}
                    ]
                },
                'new': 'best'
            }
        }

    def test_sanitize_list(self, sanitizer):
        value_list = [
            'blarg',
            '10',
            'key',
            'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
        ]

        result = sanitizer.sanitize('value_list', value_list)

        assert result == [
            'blarg',
            '10',
            'key',
            self.MASK
        ]

    def test_sanitize_nested_lists(self, sanitizer):
        value_list = [
            [
                'blarg',
                '10',
                'key',
                'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
            ],
            'blarg',
            'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc',
            [[[[[[[
                ['check out this level of nested'], 'aaaaaaaa-bbbb-bbbb-bbbb-cccccccccccc'
            ]]]]]]],
            {
                'key': 'red leaves',
                'secret': [[[[[[[[]]]]]]]]
            }
        ]

        result = sanitizer.sanitize('value_list', value_list)

        assert result == [
            [
                'blarg',
                '10',
                'key',
                self.MASK
            ],
            'blarg',
            self.MASK,
            [[[[[[[['check out this level of nested'], self.MASK]]]]]]],
            {
                'key': self.MASK,
                'secret': self.MASK
            }
        ]
