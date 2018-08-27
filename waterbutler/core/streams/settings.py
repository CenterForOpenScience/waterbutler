import zlib

from waterbutler import settings


config = settings.child('STREAMS_CONFIG')


ZIP_EXTENSIONS = config.get('ZIP_EXTENSIONS', '.zip .gz .bzip .bzip2 .rar .xz .bz2 .7z').split(' ')

# Compression level to apply to zipped files. Value must be an integer from 0 to 9, where
# lower values represent less compression.  -1 is also allowed, meaning the default level
# (approximately equivalent to a 6).  See the zlib docs for more:
# https://docs.python.org/3/library/zlib.html#zlib.compressobj
ZIP_COMPRESSION_LEVEL = int(config.get('ZIP_COMPRESSION_LEVEL', zlib.Z_DEFAULT_COMPRESSION))
