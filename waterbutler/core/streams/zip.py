import zlib
import time
import struct
import asyncio
import logging
import zipfile
import binascii

from waterbutler.core.streams import settings
from waterbutler.core.streams.base import BaseStream, MultiStream, StringStream

logger = logging.getLogger(__name__)


# for some reason python3.5 has this as (1 << 31) - 1, which is 0x7fffffff
ZIP64_LIMIT = 0xffffffff - 1


# empty zip file
EMPTY_ZIP_FILE = b'\x50\x4b\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'


# Basic structure of .zip:

# <Local File Header 0>
# <File Stream 0>
# <Data Descriptor 0>
# .
# .
#
# <Local File Header n>
# <File Stream n>
# <Data Descriptor n>
# <CentralDirectoryFileHeader 0>
# .
# .
# <CentralDirectoryFileHeader n>
# <Zip64 End of Central Directory>
# <Zip64 End of Central Directory Locator>
# <End of Central Directory>


class ZipLocalFileDataDescriptor(BaseStream):
    """The data descriptor (footer) for a local file in a zip archive. Required for streaming
    zip files.  If either the original size or compressed size are larger than 0xfffffffe bytes
    both corresponding fields of data must be increased to 8 bytes.  NB: the spec implies that
    these fields can be set unconditionally to 8 bytes, but in practice, that seems to break some
    unzip implementations.

    The Data Descriptor is described in section 4.3.9 of the PKZIP APPNOTE.TXT.

    Note: This class is tightly coupled to ZipStreamReader and should not be used separately.
    """
    def __init__(self, file):
        super().__init__()
        self.file = file

    @property
    def size(self):
        return 0

    async def _read(self, *args, **kwargs):
        """Create 16 or 24 byte descriptor of file CRC, file size, and compress size"""
        self._eof = True

        if (self.file.original_size > ZIP64_LIMIT) or (self.file.compressed_size > ZIP64_LIMIT):
            self.file.need_zip64_data_descriptor = True
        return self.file.descriptor


class ZipLocalFileData(BaseStream):
    """A thin stream wrapper. Update the original_size, compressed_size, and CRC of a ZipLocalFile
    as chunks are read and compressed.

    See section 4.3.8 of the PKZIP APPNOTE.TXT.

    Note: This class is tightly coupled to ZipStreamReader and should not be used separately.
    """
    def __init__(self, file, stream, *args, **kwargs):
        self.file = file
        self.stream = stream
        self._buffer = bytearray()
        super().__init__(*args, **kwargs)

    @property
    def size(self):
        return 0

    async def _read(self, n=-1, *args, **kwargs):

        ret = self._buffer

        while (n == -1 or len(ret) < n) and not self.stream.at_eof():
            chunk = await self.stream.read(n, *args, **kwargs)

            # Update file info
            self.file.original_size += len(chunk)
            self.file.zinfo.CRC = binascii.crc32(chunk, self.file.zinfo.CRC)

            # compress
            if self.file.compressor:
                compressed = self.file.compressor.compress(chunk)
                compressed += self.file.compressor.flush(
                    zlib.Z_FINISH if self.stream.at_eof() else zlib.Z_SYNC_FLUSH
                )
            else:
                compressed = chunk

            # Update file info
            self.file.compressed_size += len(compressed)
            ret += compressed

        # buffer any overages
        if n != -1 and len(ret) > n:
            self._buffer = ret[n:]
            ret = ret[:n]
        else:
            self._buffer = bytearray()

        # EOF is the buffer and stream are both empty
        if not self._buffer and self.stream.at_eof():
            self.feed_eof()

        return bytes(ret)


class ZipLocalFile(MultiStream):
    """A local file entry in a zip archive. Constructs the local file header,
    file data stream, and data descriptor.

    Note: This class is tightly coupled to ZipStreamReader and should not be
    used separately.
    """
    def __init__(self, file_tuple):

        filename, stream = file_tuple
        # Build a ZipInfo instance to use for the file's header and footer
        self.zinfo = zipfile.ZipInfo(
            filename=filename,
            date_time=time.localtime(time.time())[:6],
        )

        already_zipped = False
        for zip_ext in settings.ZIP_EXTENSIONS:
            if self.zinfo.filename.endswith(zip_ext):
                already_zipped = True
                logger.info('   DONE!')
                break

        logger.debug('file is already compressed: {}'.format(already_zipped))
        # If the file is a `.zip`, set permission and turn off compression
        if already_zipped:
            self.zinfo.external_attr = 0o600 << 16      # -rw-------
            self.zinfo.compress_type = zipfile.ZIP_STORED
            self.compressor = None
        # If the file is a directory, set the directory flag and turn off compression
        elif self.zinfo.filename[-1] == '/':
            self.zinfo.external_attr = 0o40775 << 16    # drwxrwxr-x
            self.zinfo.external_attr |= 0x10            # Directory flag
            self.zinfo.compress_type = zipfile.ZIP_STORED
            self.compressor = None
        # For other types, set permission and define a compressor
        else:
            self.zinfo.external_attr = 0o600 << 16      # -rw-------
            self.zinfo.compress_type = zipfile.ZIP_DEFLATED
            self.compressor = zlib.compressobj(
                settings.ZIP_COMPRESSION_LEVEL,
                zlib.DEFLATED,
                -15,
            )

        self.zinfo.header_offset = 0
        self.zinfo.flag_bits |= 0x08

        # Initial CRC: value will be updated as file is streamed
        self.zinfo.CRC = 0

        # meta information - needed to build the footer
        self.original_size = 0
        self.compressed_size = 0
        self.need_zip64_data_descriptor = False

        super().__init__(
            StringStream(self.local_header),
            ZipLocalFileData(self, stream),
            ZipLocalFileDataDescriptor(self),
        )

    @property
    def local_header(self):
        """The file's header, for inclusion just before the content stream.  The `zip64` flag
        ensures that the correct version from and version needed flags are set.  Some unzippers
        will fail to extract zip64 if the minimum version flags aren't at least `45`.

        See section 4.3.7 of the PKZIP APPNOTE.TXT
        """
        return self.zinfo.FileHeader(zip64=True)

    @property
    def directory_header(self):
        """The file's header, for inclusion in the archive's central directory.

        If the original size, compressed size, or header offset is larger than 0xfffffffe, then
        the value in the central directory header must be set to 0xffffffff, and the real values
        must be saved in the extended information field.  The format of the Zip64 Extended
        Information field is documented in section 4.5.3 of APPNOTE.txt.  If more than one field
        is added to the extended information, it must be in order of: original size, compressed
        size, local header offset.

        The created with version and extract needs version must be at least 45, indicating zip64
        support (section 4.4.3.2).  Some unzippers will not recognize zip64 files unless this is
        set.

        The Central Directory File Header is described in section 4.3.12 of the APPNOTE.TXT.

        """
        dt = self.zinfo.date_time

        # modification date/time, in MSDOS format
        dosdate = (dt[0] - 1980) << 9 | dt[1] << 5 | dt[2]
        dostime = dt[3] << 11 | dt[4] << 5 | (dt[5] // 2)

        extra_64 = []

        reported_original_size = self.original_size
        if self.original_size > ZIP64_LIMIT:
            extra_64.append(self.original_size)
            reported_original_size = 0xFFFFFFFF

        reported_compressed_size = self.compressed_size
        if self.compressed_size > ZIP64_LIMIT:
            extra_64.append(self.compressed_size)
            reported_compressed_size = 0xFFFFFFFF

        reported_header_offset = self.zinfo.header_offset
        if self.zinfo.header_offset > ZIP64_LIMIT:
            extra_64.append(self.zinfo.header_offset)
            reported_header_offset = 0xFFFFFFFF

        extra_data = self.zinfo.extra
        if len(extra_64):
            extra_data = struct.pack(
                '<HH' + 'Q' * len(extra_64),
                1,
                8 * len(extra_64),
                *extra_64
            ) + self.zinfo.extra

        filename, flag_bits = self.zinfo._encodeFilenameFlags()
        centdir = struct.pack(
            zipfile.structCentralDir,
            zipfile.stringCentralDir,
            45,  # self.zinfo.create_version,
            self.zinfo.create_system,
            45,  # self.zinfo.extract_version,
            self.zinfo.reserved,
            flag_bits,
            self.zinfo.compress_type,
            dostime,  # modification time
            dosdate,
            self.zinfo.CRC,
            reported_compressed_size,
            reported_original_size,
            len(self.zinfo.filename.encode('utf-8')),
            len(extra_data),
            len(self.zinfo.comment),
            0,
            self.zinfo.internal_attr,
            self.zinfo.external_attr,
            reported_header_offset,
        )

        return centdir + filename + extra_data + self.zinfo.comment

    @property
    def descriptor(self):
        """Local file data descriptor.  See ZipLocalFileDataDescriptor."""

        fmt = '<4sLQQ' if self.need_zip64_data_descriptor else '<4sLLL'
        signature = b'PK\x07\x08'  # magic number for data descriptor
        return struct.pack(
            fmt,
            signature,
            self.zinfo.CRC,
            self.compressed_size,
            self.original_size,
        )

    @property
    def total_bytes(self):
        """Length, in bytes, of output. Includes header, compressed file, and data descriptor.

        Note: This should be accessed after the file's data has been streamed, since the total
        size, compressed size, and CRC isn't known until then.
        """
        return (
            len(self.local_header) +
            self.compressed_size +
            len(self.descriptor)
        )


class ZipArchiveCentralDirectory(StringStream):
    """The central directory for a zip archive.  Contains the Central Directory File Headers for
    each file.  This class also builds the Zip64 End of Central Directory, the Zip64 End of
    Central Directory Locator, and the End of Central Directory records.  These are always the
    last entries in the zipfile.

    These records are described in sections 4.3.12 through 4.3.16 of APPNOTE.txt.

    Note: This class is tightly coupled to ZipStreamReader and should not be used separately.
    """
    def __init__(self, files):
        self.files = files
        super().__init__(self.build_content())

    def build_content(self):
        file_headers = []
        cumulative_offset = 0

        count = len(self.files)
        if count == 0:
            return EMPTY_ZIP_FILE

        for file in self.files:
            file.zinfo.header_offset = cumulative_offset
            file_headers.append(file.directory_header)
            cumulative_offset += file.total_bytes

        file_headers = b''.join(file_headers)

        # Zip64 End of Central Directory, section 4.3.14
        zip64_endrec = struct.pack(
            zipfile.structEndArchive64,
            zipfile.stringEndArchive64,
            44,  # size of remaining zip64_endrec in bytes
            45,  # version created with (45 indicates ZIP64 support)
            45,  # version need to extract (45 indicates ZIP64 support)
            0,  # number of this disk
            0,  # number of disk with central directory
            count,  # number of entries in cent. dir on this disk
            count,  # total number of cent. dir entries
            len(file_headers),  # size of the central directory
            cumulative_offset,  # offset of central directory
        )

        # Zip64 End of Central Directory Locator, section 4.3.15
        zip64_locator = struct.pack(
            zipfile.structEndArchive64Locator,
            zipfile.stringEndArchive64Locator,
            0,  # disk number with zip64 EOCD
            cumulative_offset + len(file_headers),  # offset to beginning of zip64 EOCD
            1,  # total number of disks
        )

        centdir_count = min(count, 0xFFFF)
        centdir_size = min(len(file_headers), 0xFFFFFFFF)
        centdir_offset = min(cumulative_offset, 0xFFFFFFFF)

        # End of Central Directory, section 4.3.16
        endrec = struct.pack(
            zipfile.structEndArchive,
            zipfile.stringEndArchive,
            0,  # nbr of this disk
            0,  # nbr of disk with start of central directory
            centdir_count,  # nbr of central dir entries on this disk
            centdir_count,  # nbr of central dir entries total
            centdir_size,  # size of central dir in bytes
            centdir_offset,  # offset to start of central dir
            0,  # comment length in bytes
        )

        return b''.join((file_headers, zip64_endrec, zip64_locator, endrec))


class ZipStreamReader(asyncio.StreamReader):
    """Combines one or more streams into a single, Zip-compressed stream"""
    def __init__(self, stream_gen):
        self._eof = False
        self.stream = None
        self.streams = stream_gen
        self.finished_streams = []
        # Each incoming stream should be wrapped in a _ZipFile instance
        super().__init__()

    async def read(self, n=-1):
        if n < 0:
            # Parent class will handle auto chunking for us
            return await super().read(n)

        if not self.stream:
            try:
                self.stream = ZipLocalFile(await self.streams.__anext__())
            except StopAsyncIteration:
                if self._eof:
                    return b''
                self._eof = True
                # Append a stream for the archive's footer (central directory)
                self.stream = ZipArchiveCentralDirectory(self.finished_streams)

        chunk = await self.stream.read(n)
        if len(chunk) < n and self.stream.at_eof():
            self.finished_streams.append(self.stream)
            self.stream = None
            chunk += await self.read(n - len(chunk))

        return chunk
