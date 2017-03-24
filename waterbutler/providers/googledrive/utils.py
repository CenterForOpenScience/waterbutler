from waterbutler.core import exceptions

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
    for format in DOCS_FORMATS:
        if format['ext'] == ext:
            return format['mime_type']


def get_format(metadata):
    for format in DOCS_FORMATS:
        if format['mime_type'] == metadata['mimeType']:
            return format
    return DOCS_DEFAULT_FORMAT


def get_extension(metadata):
    format = get_format(metadata)
    return format['ext']


def get_ext_from_mime_type(mime_type):
    for format in DOCS_FORMATS:
        if format['mime_type'] == mime_type:
            return format['ext']


def ext_is_gfile(ext):
    return ext in [x['ext'] for x in DOCS_FORMATS]


def get_download_extension(metadata):
    format = get_format(metadata)
    return format['download_ext']


def get_export_link(metadata):
    format = get_format(metadata)
    if metadata.get('exportLinks'):
        return metadata['exportLinks'].get(format['type'])


def disambiguate_files(ext, resp_items, path):
    """
    When calling for an extension-less file there are two cases:
    1. It's a random file the user uploaded without an extension
    2. It's a GFile that's hiding it's extension

    We want the random file when possible, but we also have to disambiguate between GFiles and we
    do that using mime type.

    :param ext: The extension we are given as part of the path, for GFiles it should always be given.
    :param resp_items: The json dicts returned from a GDrive query
    :return: The item the user was looking for.
    """

    if ext_is_gfile(ext):
        items = [x for x in resp_items if x['mimeType'] == get_mimetype_from_ext(ext)]
    else:
        # GFiles never include a 'fileExtension' field, but extension-less files have them as ''
        items = [x for x in resp_items if x.get('fileExtension') == '']

    if len(items) == 1:
        return items[0]
    else:
        raise exceptions.NotFoundError(path)
