import pytest

from waterbutler.providers.dataverse import utils as dv_utils


@pytest.fixture
def format_dict():
    return {
        'xlsx': {
            'originalFileFormat': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'originalFormatLabel': 'MS Excel (XLSX)',
            'contentType': 'text/tab-separated-values',
        },
        'RData': {
            'originalFileFormat': 'application/x-rlang-transport',
            'originalFormatLabel': 'R Data',
            'contentType': 'text/tab-separated-values'
        },
        'sav': {
            'originalFileFormat': 'application/x-spss-sav',
            'originalFormatLabel': 'SPSS SAV',
            'contentType': 'text/tab-separated-values'
        },
        'dta': {
            'originalFileFormat': 'application/x-stata',
            'originalFormatLabel': 'Stata Binary',
            'contentType': 'text/tab-separated-values'
        },
        'por': {
            'originalFileFormat': 'application/x-spss-por',
            'originalFormatLabel': 'SPSS Portable',
            'contentType': 'text/tab-separated-values'
        },
        'csv': {
            'originalFileFormat': 'text/csv',
            'originalFormatLabel': 'Comma Separated Values',
            'contentType': 'text/tab-separated-values'
        }
    }


class TestUtils:

    def test_original_ext_from_raw_metadata(self, format_dict):
        for key in format_dict:
            assert key in dv_utils.original_ext_from_raw_metadata(format_dict[key])

    def test_original_ext_from_raw_metadata_none_case(self, format_dict):
        for key in format_dict:
            format_dict[key]['originalFormatLabel'] = 'blarg'
            assert dv_utils.original_ext_from_raw_metadata(format_dict[key]) is None
