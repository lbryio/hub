import sys
import functools
import typing
from dataclasses import dataclass
from struct import Struct
from scribe.schema.claim import Claim
from scribe.common import double_sha256

if (sys.version_info.major, sys.version_info.minor) > (3, 7):
    cachedproperty = functools.cached_property
else:
    cachedproperty = property


struct_le_i = Struct('<i')
struct_le_q = Struct('<q')
struct_le_H = Struct('<H')
struct_le_I = Struct('<I')
struct_le_Q = Struct('<Q')
struct_be_H = Struct('>H')
struct_be_I = Struct('>I')
structB = Struct('B')

unpack_le_int32_from = struct_le_i.unpack_from
unpack_le_int64_from = struct_le_q.unpack_from
unpack_le_uint16_from = struct_le_H.unpack_from
unpack_le_uint32_from = struct_le_I.unpack_from
unpack_le_uint64_from = struct_le_Q.unpack_from
unpack_be_uint16_from = struct_be_H.unpack_from
unpack_be_uint32_from = struct_be_I.unpack_from

pack_le_int32 = struct_le_i.pack
pack_le_int64 = struct_le_q.pack
pack_le_uint16 = struct_le_H.pack
pack_le_uint32 = struct_le_I.pack
pack_le_uint64 = struct_le_q.pack
pack_be_uint64 = lambda x: x.to_bytes(8, byteorder='big')
pack_be_uint16 = lambda x: x.to_bytes(2, byteorder='big')
pack_be_uint32 = struct_be_I.pack
pack_byte = structB.pack


def pack_varint(n):
    if n < 253:
        return pack_byte(n)
    if n < 65536:
        return pack_byte(253) + pack_le_uint16(n)
    if n < 4294967296:
        return pack_byte(254) + pack_le_uint32(n)
    return pack_byte(255) + pack_le_uint64(n)


def pack_varbytes(data):
    return pack_varint(len(data)) + data


class NameClaim(typing.NamedTuple):
    name: bytes
    value: bytes


class ClaimUpdate(typing.NamedTuple):
    name: bytes
    claim_hash: bytes
    value: typing.Optional[bytes] = None


class ClaimSupport(typing.NamedTuple):
    name: bytes
    claim_hash: bytes
    value: typing.Optional[bytes] = None


ZERO = bytes(32)
MINUS_1 = 4294967295


class Tx(typing.NamedTuple):
    version: int
    inputs: typing.List['TxInput']
    outputs: typing.List['TxOutput']
    locktime: int
    raw: bytes
    marker: typing.Optional[int] = None
    flag: typing.Optional[int] = None
    witness: typing.Optional[typing.List[typing.List[bytes]]] = None

    def as_dict(self, coin):
        txid = double_sha256(self.raw)[::-1].hex()
        result = {
          "txid": txid,
          "hash": txid,
          "version": self.version,
          "size": len(self.raw),
          "vsize": len(self.raw),
          "weight": None,
          "locktime": self.locktime,
          "vin": [
              {
                  "txid": txin.prev_hash[::-1].hex(),
                  "vout": txin.prev_idx,
                  "scriptSig": {
                      "asm": None,
                      "hex": txin.script.hex()
                  },
                  "sequence": txin.sequence
              } for txin in self.inputs
          ],
          "vout": [
              {
                  "value": txo.value / 1E8,
                  "n": txo.nout,
                  "scriptPubKey": {
                      "asm": None,
                      "hex": txo.pk_script.hex(),
                      "reqSigs": 1,
                      "type": "nonstandard" if (txo.is_support or txo.is_claim or txo.is_update) else "pubkeyhash" if txo.pubkey_hash else "scripthash",
                      "addresses": [
                          coin.claim_address_handler(txo)
                      ]
                  }
              } for txo in self.outputs
          ],
          "hex": self.raw.hex()
        }
        for n, txo in enumerate(self.outputs):
            if txo.is_support or txo.is_claim or txo.is_update:
                result['vout'][n]["scriptPubKey"]["isclaim"] = txo.is_claim or txo.is_update
                result['vout'][n]["scriptPubKey"]["issupport"] = txo.is_support
                result['vout'][n]["scriptPubKey"]["subtype"] = "pubkeyhash" if txo.pubkey_hash else "scripthash"
        return result


class TxInput(typing.NamedTuple):
    prev_hash: bytes
    prev_idx: int
    script: bytes
    sequence: int

    def __str__(self):
        return f"TxInput({self.prev_hash[::-1].hex()}, {self.prev_idx:d}, script={self.script.hex()}, " \
               f"sequence={self.sequence:d})"

    def is_generation(self):
        """Test if an input is generation/coinbase like"""
        return self.prev_idx == MINUS_1 and self.prev_hash == ZERO

    def serialize(self):
        return b''.join((
            self.prev_hash,
            pack_le_uint32(self.prev_idx),
            pack_varbytes(self.script),
            pack_le_uint32(self.sequence),
        ))


@dataclass
class TxOutput:
    nout: int
    value: int
    pk_script: bytes
    claim: typing.Optional[typing.Union[NameClaim, ClaimUpdate]]  # TODO: fix this being mutable, it shouldn't be
    support: typing.Optional[ClaimSupport]
    pubkey_hash: typing.Optional[bytes]
    script_hash: typing.Optional[bytes]
    pubkey: typing.Optional[bytes]

    @property
    def is_claim(self):
        return isinstance(self.claim, NameClaim)

    @property
    def is_update(self):
        return isinstance(self.claim, ClaimUpdate)

    @cachedproperty
    def metadata(self) -> typing.Optional[Claim]:
        return None if not (self.claim or self.support).value else Claim.from_bytes((self.claim or self.support).value)

    @property
    def is_support(self):
        return self.support is not None

    def serialize(self):
        return b''.join((
            pack_le_int64(self.value),
            pack_varbytes(self.pk_script),
        ))


class Block(typing.NamedTuple):
    raw: bytes
    header: bytes
    transactions: typing.List[Tx]
