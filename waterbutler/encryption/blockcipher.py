import jwe
# PyJWE 0.1.5 and 0.1.6 compatible

# development only, simple default key is used
# TODO: 1. generate key based on user credential
# TODO: 2. store key somewhere safe

key = b'default_key'
salt = b'default_salt'
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
