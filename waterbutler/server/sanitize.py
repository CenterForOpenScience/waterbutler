import re

from raven.processors import SanitizePasswordsProcessor


class WBSanitizer(SanitizePasswordsProcessor):
    """Asterisk out things that look like passwords, keys, etc."""

    # Store mask as a fixed length for security
    MASK = '*' * 8

    # Token and key added from original. Key is used by Dataverse
    FIELDS = frozenset([
        'password',
        'secret',
        'passwd',
        'authorization',
        'api_key',
        'apikey',
        'sentry_dsn',
        'access_token',
        'key',
        'token',
    ])

    # Credit card regex left intact from original processor
    # While we should never have credit card information, its still best to perform the check
    # and keep old functionality
    VALUES_RE = re.compile(r'^(?:\d[ -]*?){13,16}$')

    # Should specifically match Dataverse secrets. Key format checked on demo and on Harvard
    DATAVERSE_SECRET_RE = re.compile(r'[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]'
                                                '{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}')

    def sanitize(self, key, value):
        """Overload the sanitize function of the `SanitizePasswordsProcessor'."""
        if value is None:
            return

        # Part of the original method. Looks for credit cards to sanitize
        if isinstance(value, str) and self.VALUES_RE.search(value):
            return self.MASK

        if isinstance(value, dict):
            for item in value:
                if item in self.FIELDS:
                    value[item] = self.MASK

        # Check for Dataverse secrets
        if isinstance(value, str):
            matches = self.DATAVERSE_SECRET_RE.findall(value)
            for match in matches:
                value = value.replace(match, self.MASK)

        # key can be a NoneType
        if not key:
            return value

        # Just in case we have bytes here, we want to turn them into text
        # properly without failing so we can perform our check.
        if isinstance(key, bytes):
            key = key.decode('utf-8', 'replace')
        else:
            key = str(key)

        key = key.lower()
        for field in self.FIELDS:
            if field in key:
                return self.MASK

        return value
