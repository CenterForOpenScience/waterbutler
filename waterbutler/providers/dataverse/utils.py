ORIGINAL_FORMATS = {
    'xlsx': {
        'original_format': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'original_label': 'MS Excel (XLSX)',
        'content_type': 'text/tab-separated-values',

    },
    # Rdata can come in a few different forms, so just list all of them here
    'RData': {
        'original_format': 'application/x-rlang-transport',
        'original_label': 'R Data',
        'content_type': 'text/tab-separated-values'

    },
    'rdata': {
        'original_format': 'application/x-rlang-transport',
        'original_label': 'R Data',
        'content_type': 'text/tab-separated-values'

    },
    'Rdata': {
        'original_format': 'application/x-rlang-transport',
        'original_label': 'R Data',
        'content_type': 'text/tab-separated-values'

    },
    'sav': {
        'original_format': 'application/x-spss-sav',
        'original_label': 'SPSS SAV',
        'content_type': 'text/tab-separated-values'
    },
    'dta': {
        'original_format': 'application/x-stata',
        'original_label': 'Stata Binary',
        'content_type': 'text/tab-separated-values'

    },
    'por': {
        'original_format': 'application/x-spss-por',
        'original_label': 'SPSS Portable',
        'content_type': 'text/tab-separated-values'

    },
    'csv': {
        'original_format': 'text/csv',
        'original_label': 'Comma Separated Values',
        'content_type': 'text/tab-separated-values'
    }
}


def original_ext_from_raw_metadata(data):
    """Use the raw metadata to figure out the original extension."""
    label = data.get('originalFormatLabel', None)
    file_format = data.get('originalFileFormat', None)
    content_type = data.get('contentType', None)

    if not label or not file_format or not content_type:
        return None

    for key in ORIGINAL_FORMATS:
        if (label == ORIGINAL_FORMATS[key]['original_label'] and
                file_format == ORIGINAL_FORMATS[key]['original_format'] and
                content_type == ORIGINAL_FORMATS[key]['content_type']):

            return key

    return None
