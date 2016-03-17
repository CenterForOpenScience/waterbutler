import magic
import io


class HashStreamWriter:
    """Stream-like object that hashes and discards its input."""
    def __init__(self, hasher):
        self.hash = hasher()

    @property
    def hexdigest(self):
        return self.hash.hexdigest()

    def can_write_eof(self):
        return False

    def write(self, data):
        self.hash.update(data)

    def close(self):
        pass


class MimeStreamWriter():
    """Stream-like object that determines mime-type and discards its input."""
    def __init__(self):
        self.mybytes = io.BytesIO()

    @property
    def mimetype(self):
        return magic.from_buffer(self.mybytes.getvalue(), mime=True)

    def can_write_eof(self):
        return False

    def write(self, data):
        self.mybytes.write(data)

    def close(self):
        pass
