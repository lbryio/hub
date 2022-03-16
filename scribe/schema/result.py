import base64
from typing import List, TYPE_CHECKING, Union, Optional, Dict, Set, Tuple
from itertools import chain

from scribe.error import ResolveCensoredError
from scribe.schema.types.v2.result_pb2 import Outputs as OutputsMessage
from scribe.schema.types.v2.result_pb2 import Error as ErrorMessage
if TYPE_CHECKING:
    from scribe.db.common import ResolveResult
INVALID = ErrorMessage.Code.Name(ErrorMessage.INVALID)
NOT_FOUND = ErrorMessage.Code.Name(ErrorMessage.NOT_FOUND)
BLOCKED = ErrorMessage.Code.Name(ErrorMessage.BLOCKED)


class Censor:
    NOT_CENSORED = 0
    SEARCH = 1
    RESOLVE = 2


def encode_txo(txo_message: OutputsMessage, resolve_result: Union['ResolveResult', Exception]):
    if isinstance(resolve_result, Exception):
        txo_message.error.text = resolve_result.args[0]
        if isinstance(resolve_result, ValueError):
            txo_message.error.code = ErrorMessage.INVALID
        elif isinstance(resolve_result, LookupError):
            txo_message.error.code = ErrorMessage.NOT_FOUND
        return
    txo_message.tx_hash = resolve_result.tx_hash
    txo_message.nout = resolve_result.position
    txo_message.height = resolve_result.height
    txo_message.claim.short_url = resolve_result.short_url
    txo_message.claim.reposted = resolve_result.reposted
    txo_message.claim.is_controlling = resolve_result.is_controlling
    txo_message.claim.creation_height = resolve_result.creation_height
    txo_message.claim.activation_height = resolve_result.activation_height
    txo_message.claim.expiration_height = resolve_result.expiration_height
    txo_message.claim.effective_amount = resolve_result.effective_amount
    txo_message.claim.support_amount = resolve_result.support_amount

    if resolve_result.canonical_url is not None:
        txo_message.claim.canonical_url = resolve_result.canonical_url
    if resolve_result.last_takeover_height is not None:
        txo_message.claim.take_over_height = resolve_result.last_takeover_height
    if resolve_result.claims_in_channel is not None:
        txo_message.claim.claims_in_channel = resolve_result.claims_in_channel
    if resolve_result.reposted_claim_hash and resolve_result.reposted_tx_hash is not None:
        txo_message.claim.repost.tx_hash = resolve_result.reposted_tx_hash
        txo_message.claim.repost.nout = resolve_result.reposted_tx_position
        txo_message.claim.repost.height = resolve_result.reposted_height
    if resolve_result.channel_hash and resolve_result.channel_tx_hash is not None:
        txo_message.claim.channel.tx_hash = resolve_result.channel_tx_hash
        txo_message.claim.channel.nout = resolve_result.channel_tx_position
        txo_message.claim.channel.height = resolve_result.channel_height


class Outputs:

    __slots__ = 'txos', 'extra_txos', 'txs', 'offset', 'total', 'blocked', 'blocked_total'

    def __init__(self, txos: List, extra_txos: List, txs: set,
                 offset: int, total: int, blocked: List, blocked_total: int):
        self.txos = txos
        self.txs = txs
        self.extra_txos = extra_txos
        self.offset = offset
        self.total = total
        self.blocked = blocked
        self.blocked_total = blocked_total

    def inflate(self, txs):
        tx_map = {tx.hash: tx for tx in txs}
        for txo_message in self.extra_txos:
            self.message_to_txo(txo_message, tx_map)
        txos = [self.message_to_txo(txo_message, tx_map) for txo_message in self.txos]
        return txos, self.inflate_blocked(tx_map)

    def inflate_blocked(self, tx_map):
        return {
            "total": self.blocked_total,
            "channels": [{
                'channel': self.message_to_txo(blocked.channel, tx_map),
                'blocked': blocked.count
            } for blocked in self.blocked]
        }

    def message_to_txo(self, txo_message, tx_map):
        if txo_message.WhichOneof('meta') == 'error':
            error = {
                'error': {
                    'name': txo_message.error.Code.Name(txo_message.error.code),
                    'text': txo_message.error.text,
                }
            }
            if error['error']['name'] == BLOCKED:
                error['error']['censor'] = self.message_to_txo(
                    txo_message.error.blocked.channel, tx_map
                )
            return error

        tx = tx_map.get(txo_message.tx_hash)
        if not tx:
            return
        txo = tx.outputs[txo_message.nout]
        if txo_message.WhichOneof('meta') == 'claim':
            claim = txo_message.claim
            txo.meta = {
                'short_url': f'lbry://{claim.short_url}',
                'canonical_url': f'lbry://{claim.canonical_url or claim.short_url}',
                'reposted': claim.reposted,
                'is_controlling': claim.is_controlling,
                'take_over_height': claim.take_over_height,
                'creation_height': claim.creation_height,
                'activation_height': claim.activation_height,
                'expiration_height': claim.expiration_height,
                'effective_amount': claim.effective_amount,
                'support_amount': claim.support_amount,
                # 'trending_group': claim.trending_group,
                # 'trending_mixed': claim.trending_mixed,
                # 'trending_local': claim.trending_local,
                # 'trending_global': claim.trending_global,
            }
            if claim.HasField('channel'):
                txo.channel = tx_map[claim.channel.tx_hash].outputs[claim.channel.nout]
            if claim.HasField('repost'):
                txo.reposted_claim = tx_map[claim.repost.tx_hash].outputs[claim.repost.nout]
            try:
                if txo.claim.is_channel:
                    txo.meta['claims_in_channel'] = claim.claims_in_channel
            except:
                pass
        return txo

    @classmethod
    def from_base64(cls, data: str) -> 'Outputs':
        return cls.from_bytes(base64.b64decode(data))

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Outputs':
        outputs = OutputsMessage()
        outputs.ParseFromString(data)
        txs = set()
        for txo_message in chain(outputs.txos, outputs.extra_txos):
            if txo_message.WhichOneof('meta') == 'error':
                continue
            txs.add((txo_message.tx_hash[::-1].hex(), txo_message.height))
        return cls(
            outputs.txos, outputs.extra_txos, txs,
            outputs.offset, outputs.total,
            outputs.blocked, outputs.blocked_total
        )

    @classmethod
    def from_grpc(cls, outputs: OutputsMessage) -> 'Outputs':
        txs = set()
        for txo_message in chain(outputs.txos, outputs.extra_txos):
            if txo_message.WhichOneof('meta') == 'error':
                continue
            txs.add((txo_message.tx_hash[::-1].hex(), txo_message.height))
        return cls(
            outputs.txos, outputs.extra_txos, txs,
            outputs.offset, outputs.total,
            outputs.blocked, outputs.blocked_total
        )

    @staticmethod
    def to_base64(txo_rows: List[Union[Exception, 'ResolveResult']], extra_txo_rows: List['ResolveResult'],
                  offset: int = 0, total: Optional[int] = None,
                  censored: Optional[Dict[bytes, Set[bytes]]] = None) -> str:
        return base64.b64encode(Outputs.to_bytes(txo_rows, extra_txo_rows, offset, total, censored)).decode()

    @staticmethod
    def to_bytes(txo_rows: List[Union[Exception, 'ResolveResult']], extra_txo_rows: List['ResolveResult'],
                 offset: int = 0, total: Optional[int] = None,
                 censored: Optional[Dict[bytes, Set[bytes]]] = None) -> bytes:
        page = OutputsMessage()
        page.offset = offset
        if total is not None:
            page.total = total
        censored = censored or {}
        censored_txos: Dict[bytes, List[Tuple[str, 'ResolveResult']]] = {}
        for row in txo_rows:
            txo_message = page.txos.add()
            if isinstance(row, ResolveCensoredError):
                censored_hash = bytes.fromhex(row.censor_id)
                if censored_hash not in censored_txos:
                    censored_txos[censored_hash] = []
                censored_txos[censored_hash].append((str(row), txo_message))
            else:
                encode_txo(txo_message, row)
        for row in extra_txo_rows:
            if row.claim_hash in censored:
                page.blocked_total += len(censored[row.claim_hash])
                blocked = page.blocked.add()
                blocked.count = len(censored[row.claim_hash])
                blocked.channel.tx_hash = row.tx_hash
                blocked.channel.nout = row.position
                blocked.channel.height = row.height
            if row.claim_hash in censored_txos:
                for (text, txo_message) in censored_txos[row.claim_hash]:
                    txo_message.error.code = ErrorMessage.BLOCKED
                    txo_message.error.text = text
                    txo_message.error.blocked.channel.tx_hash = row.tx_hash
                    txo_message.error.blocked.channel.nout = row.position
                    txo_message.error.blocked.channel.height = row.height
            encode_txo(page.extra_txos.add(), row)
        return page.SerializeToString()
