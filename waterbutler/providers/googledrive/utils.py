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
        'mime_type': 'application/vnd.google-apps.form',
        'ext': '.gform',
        'download_ext': '.jpg',
        'type': 'image/jpeg',
    },
    {
        'mime_type': 'application/vnd.google-apps.map',
        'ext': '.gmap',
        'download_ext': '.kmz',
        'type': 'application/vnd.google-earth.kmz',
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
DOCS_UNEXPORTABLE_MIMES = ['application/vnd.google-apps.map',
                           'application/vnd.google-apps.form']


def is_docs_file(metadata):
    """Check for unexportable file types. (e.g. Maps, Forms)
    Only Docs files have the "exportLinks" key."""
    if metadata.get('mimeType') in DOCS_UNEXPORTABLE_MIMES or metadata.get('exportLinks'):
        return True
    return False


def get_format(metadata):
    for format in DOCS_FORMATS:
        if format['mime_type'] == metadata['mimeType']:
            return format
    return DOCS_DEFAULT_FORMAT


def get_extension(metadata):
    format = get_format(metadata)
    return format['ext']


def get_download_extension(metadata):
    format = get_format(metadata)
    return format['download_ext']


def get_export_link(metadata):
    format = get_format(metadata)
    return metadata['exportLinks'][format['type']]
