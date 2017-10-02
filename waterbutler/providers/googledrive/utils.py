DOCS_FORMATS = [
    {
        'mime_type': 'application/vnd.google-apps.document',
        'ext': '.gdoc',
        'download_ext': '.docx',
        'export_mimetype':
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    },
    {
        'mime_type': 'application/vnd.google-apps.drawing',
        'ext': '.gdraw',
        'download_ext': '.jpg',
        'export_mimetype': 'image/jpeg',
    },
    {
        'mime_type': 'application/vnd.google-apps.spreadsheet',
        'ext': '.gsheet',
        'download_ext': '.xlsx',
        'export_mimetype': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    },
    {
        'mime_type': 'application/vnd.google-apps.presentation',
        'ext': '.gslides',
        'download_ext': '.pptx',
        'export_mimetype':
            'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    },
]
DOCS_MIMES = [doc_format['mime_type'] for doc_format in DOCS_FORMATS]
DOCS_DEFAULT_FORMAT = {
    'mime_type': '',
    'ext': '',
    'download_ext': '.pdf',
    'export_mimetype': 'application/pdf',
}


def is_docs_file(metadata):
    return metadata['mimeType'] in DOCS_MIMES


def get_mimetype_from_ext(ext):
    for doc_format in DOCS_FORMATS:
        if doc_format['ext'] == ext:
            return doc_format['mime_type']


def get_export_mimetype_from_ext(ext):
    for doc_format in DOCS_FORMATS:
        if doc_format['ext'] == ext:
            return doc_format['export_mimetype']


def get_format(metadata):
    for doc_format in DOCS_FORMATS:
        if doc_format['mime_type'] == metadata['mimeType']:
            return doc_format
    return DOCS_DEFAULT_FORMAT


def get_extension(metadata):
    doc_format = get_format(metadata)
    return doc_format['ext']


def get_download_extension(metadata):
    doc_format = get_format(metadata)
    return doc_format['download_ext']
