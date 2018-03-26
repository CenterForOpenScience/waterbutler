ORIGINAL_FORMATS = {

    'RData': {
        'original_format': 'application/x-rlang-transport',
        'original_label': 'R Data',
        'content_type': 'text/tab-separated-values',
        'all_extensions': ['rdata', 'Rdata', 'RData']
    },
    'sav': {
        'original_format': 'application/x-spss-sav',
        'original_label': 'SPSS SAV',
        'content_type': 'text/tab-separated-values',
        'all_extensions': ['sav']
    },
    'dta': {
        'original_format': 'application/x-stata',
        'original_label': 'Stata Binary',
        'content_type': 'text/tab-separated-values',
        'all_extensions': ['dta']
    },
    'por': {
        'original_format': 'application/x-spss-por',
        'original_label': 'SPSS Portable',
        'content_type': 'text/tab-separated-values',
        'all_extensions': ['por']
    },
    'csv': {
        'original_format': 'text/csv',
        'original_label': 'Comma Separated Values',
        'content_type': 'text/tab-separated-values',
        'all_extensions': ['csv', 'CSV']
    },
    'xlsx': {
        'original_format': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'original_label': 'MS Excel (XLSX)',
        'content_type': 'text/tab-separated-values',
        'all_extensions': ['xlsx']
    }
}


def original_ext_from_raw_metadata(data):
    """Use the raw metadata to figure out possible original extensions."""
    label = data.get('originalFormatLabel', None)
    file_format = data.get('originalFileFormat', None)
    content_type = data.get('contentType', None)

    if not label or not file_format or not content_type:
        return None

    for key, ext in ORIGINAL_FORMATS.items():
        if (label == ext['original_label'] and
                file_format == ext['original_format'] and
                content_type == ext['content_type']):

            return ext['all_extensions']

    return None
