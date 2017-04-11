GFILES_FORMATS = [
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
    for format in GFILES_FORMATS:
        if format['ext'] == ext:
            return format['mime_type']


def get_format(metadata):
    for format in GFILES_FORMATS:
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


def filter_title_and_mimeType(items, title, mime_type):
    return [x for x in items if x['title'] == title and x['mimeType'] == mime_type]


def disambiguate_items_with_slash(parts, items, path_points_to_file):
    print(parts)
    if path_points_to_file:
        items = [x for x in items if x['title'] == '/'.join(parts) and
                 x['mimeType'] != 'application/vnd.google-apps.folder']
    else:
        items = filter_title_and_mimeType(items, '/'.join(parts), 'application/vnd.google-apps.folder')

    if len(items) == 1:
        return items
