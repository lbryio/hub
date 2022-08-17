import re
import struct
import typing
from typing import List
from hashlib import sha256
from decimal import Decimal
from yarl import URL
from hub.schema.base58 import Base58
from hub.schema.bip32 import PublicKey
from hub.common import hash160, hash_to_hex_str, double_sha256
from hub.scribe.transaction import TxOutput, TxInput, Block
from hub.scribe.transaction.deserializer import Deserializer
from hub.scribe.transaction.script import OpCodes, P2PKH_script, P2SH_script, txo_script_parser


HASHX_LEN = 11


class CoinError(Exception):
    """Exception raised for coin-related errors."""


ENCODE_CHECK = Base58.encode_check
DECODE_CHECK = Base58.decode_check


class LBCMainNet:
    NAME = "LBRY"
    SHORTNAME = "LBC"
    NET = "mainnet"
    ENCODE_CHECK = Base58.encode_check
    DECODE_CHECK = Base58.decode_check
    DESERIALIZER = Deserializer
    BASIC_HEADER_SIZE = 112
    CHUNK_SIZE = 96
    XPUB_VERBYTES = bytes.fromhex("0488b21e")
    XPRV_VERBYTES = bytes.fromhex("0488ade4")
    P2PKH_VERBYTE = bytes.fromhex("55")
    P2SH_VERBYTES = bytes.fromhex("7A")
    WIF_BYTE = bytes.fromhex("1C")
    GENESIS_HASH = '9c89283ba0f3227f6c03b70216b9f665f0118d5e0fa729cedf4fb34d6a34f463'
    RPC_PORT = 9245
    REORG_LIMIT = 200
    RPC_URL_REGEX = re.compile('.+@(\\[[0-9a-fA-F:]+\\]|[^:]+)(:[0-9]+)?')
    VALUE_PER_COIN = 100000000

    # Peer discovery
    PEER_DEFAULT_PORTS = {'t': '50001', 's': '50002'}
    PEERS: List[str] = []
    # claimtrie/takeover params
    nOriginalClaimExpirationTime = 262974
    nExtendedClaimExpirationTime = 2102400
    nExtendedClaimExpirationForkHeight = 400155
    nNormalizedNameForkHeight = 539940  # targeting 21 March 2019
    nMinTakeoverWorkaroundHeight = 496850
    nMaxTakeoverWorkaroundHeight = 658300  # targeting 30 Oct 2019
    nWitnessForkHeight = 680770  # targeting 11 Dec 2019
    nAllClaimsInMerkleForkHeight = 658310  # targeting 30 Oct 2019
    proportionalDelayFactor = 32
    maxTakeoverDelay = 4032

    averageBlockOffset = 160.31130145580738
    genesisTime = 1466660400

    @classmethod
    def sanitize_url(cls, url):
        # Remove surrounding ws and trailing /s
        url = url.strip().rstrip('/')
        match = cls.RPC_URL_REGEX.match(url)
        if not match:
            raise CoinError(f'invalid daemon URL: "{url}"')
        if match.groups()[1] is None:
            url += f':{cls.RPC_PORT:d}'
        if not url.startswith('http://') and not url.startswith('https://'):
            url = 'http://' + url
        obj = URL(url)
        if not obj.user or not obj.password:
            raise CoinError(f'unparseable <user>:<pass> in daemon URL: "{url}"')
        return url + '/'

    @classmethod
    def address_to_hashX(cls, address):
        """Return a hashX given a coin address."""
        return cls.hashX_from_script(cls.pay_to_address_script(address))

    @classmethod
    def P2PKH_address_from_hash160(cls, hash160_bytes):
        """Return a P2PKH address given a public key."""
        assert len(hash160_bytes) == 20
        return ENCODE_CHECK(cls.P2PKH_VERBYTE + hash160_bytes)

    @classmethod
    def P2PKH_address_from_pubkey(cls, pubkey):
        """Return a coin address given a public key."""
        return cls.P2PKH_address_from_hash160(hash160(pubkey))

    @classmethod
    def P2SH_address_from_hash160(cls, hash160_bytes):
        """Return a coin address given a hash160."""
        assert len(hash160_bytes) == 20
        return ENCODE_CHECK(cls.P2SH_VERBYTES + hash160_bytes)

    @classmethod
    def hash160_to_P2PKH_script(cls, hash160_bytes):
        return P2PKH_script(hash160_bytes)

    @classmethod
    def hash160_to_P2PKH_hashX(cls, hash160_bytes):
        return cls.hashX_from_script(P2PKH_script(hash160_bytes))

    @classmethod
    def pay_to_address_script(cls, address):
        """Return a pubkey script that pays to a pubkey hash.

        Pass the address (either P2PKH or P2SH) in base58 form.
        """
        raw = DECODE_CHECK(address)

        # Require version byte(s) plus hash160.
        verlen = len(raw) - 20
        if verlen > 0:
            verbyte, hash160_bytes = raw[:verlen], raw[verlen:]
            if verbyte == cls.P2PKH_VERBYTE:
                return P2PKH_script(hash160_bytes)
            if verbyte in cls.P2SH_VERBYTES:
                return P2SH_script(hash160_bytes)

        raise CoinError(f'invalid address: {address}')

    @classmethod
    def privkey_WIF(cls, privkey_bytes, compressed):
        """Return the private key encoded in Wallet Import Format."""
        payload = bytearray(cls.WIF_BYTE) + privkey_bytes
        if compressed:
            payload.append(0x01)
        return cls.ENCODE_CHECK(payload)

    @classmethod
    def header_hash(cls, header):
        """Given a header return hash"""
        return double_sha256(header)

    @classmethod
    def header_prevhash(cls, header):
        """Given a header return previous hash"""
        return header[4:36]

    @classmethod
    def static_header_offset(cls, height):
        """Given a header height return its offset in the headers file.

        If header sizes change at some point, this is the only code
        that needs updating."""
        return height * cls.BASIC_HEADER_SIZE

    @classmethod
    def static_header_len(cls, height):
        """Given a header height return its length."""
        return (cls.static_header_offset(height + 1)
                - cls.static_header_offset(height))

    @classmethod
    def block_header(cls, block, height):
        """Returns the block header given a block and its height."""
        return block[:cls.static_header_len(height)]

    @classmethod
    def block(cls, raw_block, height):
        """Return a Block namedtuple given a raw block and its height."""
        header = cls.block_header(raw_block, height)
        txs = Deserializer(raw_block, start=len(header)).read_tx_block()
        return Block(raw_block, header, txs)

    @classmethod
    def transaction(cls, raw_tx: bytes):
        """Return a Block namedtuple given a raw block and its height."""
        return Deserializer(raw_tx).read_tx()

    @classmethod
    def decimal_value(cls, value):
        """Return the number of standard coin units as a Decimal given a
        quantity of smallest units.

        For example 1 BTC is returned for 100 million satoshis.
        """
        return Decimal(value) / cls.VALUE_PER_COIN

    @classmethod
    def genesis_block(cls, block):
        '''Check the Genesis block is the right one for this coin.

        Return the block less its unspendable coinbase.
        '''
        header = cls.block_header(block, 0)
        header_hex_hash = hash_to_hex_str(cls.header_hash(header))
        if header_hex_hash != cls.GENESIS_HASH:
            raise CoinError(f'genesis block has hash {header_hex_hash} expected {cls.GENESIS_HASH}')

        return block

    @classmethod
    def electrum_header(cls, header, height):
        version, = struct.unpack('<I', header[:4])
        timestamp, bits, nonce = struct.unpack('<III', header[100:112])
        return {
            'version': version,
            'prev_block_hash': hash_to_hex_str(header[4:36]),
            'merkle_root': hash_to_hex_str(header[36:68]),
            'claim_trie_root': hash_to_hex_str(header[68:100]),
            'timestamp': timestamp,
            'bits': bits,
            'nonce': nonce,
            'block_height': height,
            }

    @classmethod
    def claim_address_handler(cls, txo: TxOutput) -> typing.Optional[str]:
        '''Parse a claim script, returns the address
        '''
        if txo.pubkey_hash:
            return cls.P2PKH_address_from_hash160(txo.pubkey_hash)
        elif txo.script_hash:
            return cls.P2SH_address_from_hash160(txo.script_hash)
        elif txo.pubkey:
            return cls.P2PKH_address_from_pubkey(txo.pubkey)

    @classmethod
    def hashX_from_txo(cls, txo: 'TxOutput'):
        address = cls.claim_address_handler(txo)
        if address:
            script = cls.pay_to_address_script(address)
        else:
            script = txo.pk_script
        return sha256(script).digest()[:HASHX_LEN]

    @classmethod
    def hashX_from_script(cls, script: bytes):
        '''
        Overrides electrumx hashX from script by extracting addresses from claim scripts.
        '''
        if script and script[0] == OpCodes.OP_RETURN or not script:
            return None
        if script[0] in [
            OpCodes.OP_CLAIM_NAME,
            OpCodes.OP_UPDATE_CLAIM,
            OpCodes.OP_SUPPORT_CLAIM,
        ]:
            decoded = txo_script_parser(script)
            if not decoded:
                return
            claim, support, pubkey_hash, script_hash, pubkey = decoded
            if pubkey_hash:
                return cls.address_to_hashX(cls.P2PKH_address_from_hash160(pubkey_hash))
            elif script_hash:
                return cls.address_to_hashX(cls.P2SH_address_from_hash160(script_hash))
            elif pubkey:
                return cls.address_to_hashX(cls.P2PKH_address_from_pubkey(pubkey))
        else:
            return sha256(script).digest()[:HASHX_LEN]

    @classmethod
    def get_expiration_height(cls, last_updated_height: int, extended: bool = False) -> int:
        if extended:
            return last_updated_height + cls.nExtendedClaimExpirationTime
        if last_updated_height < cls.nExtendedClaimExpirationForkHeight:
            return last_updated_height + cls.nOriginalClaimExpirationTime
        return last_updated_height + cls.nExtendedClaimExpirationTime

    @classmethod
    def get_delay_for_name(cls, blocks_of_continuous_ownership: int) -> int:
        return min(blocks_of_continuous_ownership // cls.proportionalDelayFactor, cls.maxTakeoverDelay)

    @classmethod
    def verify_signed_metadata(cls, public_key_bytes: bytes, txo: TxOutput, first_input: TxInput):
        m = txo.metadata
        if m.unsigned_payload:
            pieces = (Base58.decode(cls.claim_address_handler(txo)), m.unsigned_payload, m.signing_channel_hash[::-1])
        else:
            pieces = (first_input.prev_hash + first_input.prev_idx.to_bytes(4, byteorder='little'),
                      m.signing_channel_hash, m.to_message_bytes())
        return PublicKey.from_compressed(public_key_bytes).verify(
            m.signature, sha256(b''.join(pieces)).digest()
        )


class LBCRegTest(LBCMainNet):
    NET = "regtest"
    GENESIS_HASH = '6e3fcf1299d4ec5d79c3a4c91d624a4acf9e2e173d95a1a0504f677669687556'
    XPUB_VERBYTES = bytes.fromhex('043587cf')
    XPRV_VERBYTES = bytes.fromhex('04358394')

    P2PKH_VERBYTE = bytes.fromhex("6f")
    P2SH_VERBYTES = bytes.fromhex("c4")

    nOriginalClaimExpirationTime = 500
    nExtendedClaimExpirationTime = 600
    nExtendedClaimExpirationForkHeight = 800
    nNormalizedNameForkHeight = 250
    nMinTakeoverWorkaroundHeight = -1
    nMaxTakeoverWorkaroundHeight = -1
    nWitnessForkHeight = 150
    nAllClaimsInMerkleForkHeight = 350


class LBCTestNet(LBCRegTest):
    NET = "testnet"
    GENESIS_HASH = '9c89283ba0f3227f6c03b70216b9f665f0118d5e0fa729cedf4fb34d6a34f463'
