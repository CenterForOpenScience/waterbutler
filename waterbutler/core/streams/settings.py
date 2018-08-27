from waterbutler import settings


config = settings.child('STREAMS_CONFIG')


ZIP_EXTENSIONS = config.get('ZIP_EXTENSIONS', '.zip .gz .bzip .bzip2 .rar .xz .bz2 .7z').split(' ')
