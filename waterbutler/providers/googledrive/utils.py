DOCS_FORMATS = [
    {
        'mime_type': 'application/vnd.google-apps.document',
        'ext': '.gdoc',
        'download_ext': '.docx',
        'type': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
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
    for docs_format in DOCS_FORMATS:
        if docs_format['ext'] == ext:
            return docs_format['mime_type']


def get_format(metadata):
    for docs_format in DOCS_FORMATS:
        if docs_format['mime_type'] == metadata['mimeType']:
            return docs_format
    return DOCS_DEFAULT_FORMAT


def get_extension(metadata):
    metadata_format = get_format(metadata)
    return metadata_format['ext']


def get_download_extension(metadata):
    metadata_format = get_format(metadata)
    return metadata_format['download_ext']


def get_export_link(metadata):
    metadata_format = get_format(metadata)
    return metadata['exportLinks'][metadata_format['type']]
