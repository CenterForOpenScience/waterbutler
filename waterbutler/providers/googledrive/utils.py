DOCS_FORMATS = [
    {
        'mime_type': 'application/vnd.google-apps.document',
        'ext': '.gdoc',
        'download_ext': '.docx',
        'type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'alt_download_ext': '.pdf',
        'alt_type': 'application/pdf',
    },
    {
        'mime_type': 'application/vnd.google-apps.drawing',
        'ext': '.gdraw',
        'download_ext': '.jpg',
        'type': 'image/jpeg',
    },
    {
        'mime_type': 'application/vnd.google-apps.spreadsheet',
        'ext': '.gsheet',
        'download_ext': '.xlsx',
        'type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    },
    {
        'mime_type': 'application/vnd.google-apps.presentation',
        'ext': '.gslides',
        'download_ext': '.pptx',
        'type': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    },
]

DOCS_DEFAULT_FORMAT = {
    'ext': '',
    'download_ext': '.pdf',
    'type': 'application/pdf',
}


def is_docs_file(metadata):
    """Only Docs files have the "exportLinks" key."""
    return metadata.get('exportLinks')


def get_mimetype_from_ext(ext):
    for format_type in DOCS_FORMATS:
        if format_type.get('ext') == ext:
            return format_type.get('mime_type')


def get_format(metadata):
    for format_type in DOCS_FORMATS:
        if format_type.get('mime_type') == metadata.get('mimeType'):
            return format_type
    return DOCS_DEFAULT_FORMAT


def get_extension(metadata):
    format_type = get_format(metadata)
    return format_type.get('ext')


def get_download_extension(metadata):
    format_type = get_format(metadata)
    return format_type.get('download_ext')


def get_alt_download_extension(metadata):
    format_type = get_format(metadata)
    return format_type.get('alt_download_ext', None) or format_type.get('download_ext')


def get_alt_export_link(metadata):
    format_type = get_format(metadata)
    export_links = metadata.get('exportLinks')
    if format_type.get('alt_type'):
        return export_links.get(format_type.get('alt_type'))
    else:
        return export_links.get(format_type.get('type'))


def get_export_link(metadata):
    format_type = get_format(metadata)
    return metadata.get('exportLinks').get(format_type.get('type'))
