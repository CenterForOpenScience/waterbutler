import jwe
# PyJWE 0.1.5 and 0.1.6 compatible

from waterbutler.encryption import settings


key = settings.OSFSTORAGE_JWE_SECRET
salt = settings.OSFSTORAGE_JWE_SALT
derived_key = jwe.kdf(key, salt)


def encrypt_block(plain_bytes):
    """
    Encrypt given byte sequences
        :param  plain_bytes:
        :rtype: bytes
    """
    cipher_bytes = jwe.encrypt(plain_bytes, derived_key)
    return cipher_bytes


def decrypt_block(cipher_bytes):
    """
    Decrypt given byte sequences
        :param  cipher_bytes:
        :rtype: bytes
    """
    plain_bytes = jwe.decrypt(cipher_bytes, derived_key)
    return plain_bytes
