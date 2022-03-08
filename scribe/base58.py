import hashlib


def sha256(x):
    """ Simple wrapper of hashlib sha256. """
    return hashlib.sha256(x).digest()


def double_sha256(x):
    """ SHA-256 of SHA-256, as used extensively in bitcoin. """
    return sha256(sha256(x))


class Base58Error(Exception):
    """ Exception used for Base58 errors. """


_CHARS = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def _iter_encode(be_bytes: bytes):
    value = int.from_bytes(be_bytes, 'big')
    while value:
        value, mod = divmod(value, 58)
        yield _CHARS[mod]
    for byte in be_bytes:
        if byte != 0:
            break
        yield '1'


def b58_encode(be_bytes: bytes):
    return ''.join(_iter_encode(be_bytes))[::-1]


class Base58:
    """ Class providing base 58 functionality. """

    chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    assert len(chars) == 58
    char_map = {c: n for n, c in enumerate(chars)}

    @classmethod
    def char_value(cls, c):
        val = cls.char_map.get(c)
        if val is None:
            raise Base58Error(f'invalid base 58 character "{c}"')
        return val

    @classmethod
    def decode(cls, txt):
        """ Decodes txt into a big-endian bytearray. """
        if isinstance(txt, memoryview):
            txt = str(txt)

        if isinstance(txt, bytes):
            txt = txt.decode()

        if not isinstance(txt, str):
            raise TypeError(f'a string is required, got {type(txt).__name__}')

        if not txt:
            raise Base58Error('string cannot be empty')

        value = 0
        for c in txt:
            value = value * 58 + cls.char_value(c)

        result = value.to_bytes((value.bit_length() + 7) // 8, 'big')

        # Prepend leading zero bytes if necessary
        count = 0
        for c in txt:
            if c != '1':
                break
            count += 1
        if count:
            result = bytes((0,)) * count + result

        return result

    @classmethod
    def _iter_encode(cls, be_bytes: bytes):
        value = int.from_bytes(be_bytes, 'big')
        while value:
            value, mod = divmod(value, 58)
            yield cls.chars[mod]
        for byte in be_bytes:
            if byte != 0:
                break
            yield '1'

    @classmethod
    def encode(cls, be_bytes):
        return ''.join(cls._iter_encode(be_bytes))[::-1]

    @classmethod
    def decode_check(cls, txt, hash_fn=double_sha256):
        """ Decodes a Base58Check-encoded string to a payload. The version prefixes it. """
        be_bytes = cls.decode(txt)
        result, check = be_bytes[:-4], be_bytes[-4:]
        if check != hash_fn(result)[:4]:
            raise Base58Error(f'invalid base 58 checksum for {txt}')
        return result

    @classmethod
    def encode_check(cls, payload, hash_fn=double_sha256):
        """ Encodes a payload bytearray (which includes the version byte(s))
            into a Base58Check string."""
        be_bytes = payload + hash_fn(payload)[:4]
        return b58_encode(be_bytes)
