import itertools
import attr
import typing
from collections import defaultdict
from hub.blockchain.transaction.deserializer import Deserializer

if typing.TYPE_CHECKING:
    from hub.db import HubDB


@attr.s(slots=True)
class MemPoolTx:
    prevouts = attr.ib()
    # A pair is a (hashX, value) tuple
    in_pairs = attr.ib()
    out_pairs = attr.ib()
    fee = attr.ib()
    size = attr.ib()
    raw_tx = attr.ib()


@attr.s(slots=True)
class MemPoolTxSummary:
    hash = attr.ib()
    fee = attr.ib()
    has_unconfirmed_inputs = attr.ib()


class MemPool:
    def __init__(self, coin, db: 'HubDB'):
        self.coin = coin
        self._db = db
        self.txs = {}
        self.touched_hashXs: typing.DefaultDict[bytes, typing.Set[bytes]] = defaultdict(set)  # None can be a key

    def mempool_history(self, hashX: bytes) -> str:
        result = ''
        for tx_hash in self.touched_hashXs.get(hashX, ()):
            if tx_hash not in self.txs:
                continue  # the tx hash for the touched address is an input that isn't in mempool anymore
            result += f'{tx_hash[::-1].hex()}:{-any(_hash in self.txs for _hash, idx in self.txs[tx_hash].in_pairs):d}:'
        return result

    def remove(self, to_remove: typing.Dict[bytes, bytes]):
        # Remove txs that aren't in mempool anymore
        for tx_hash in set(self.txs).intersection(to_remove.keys()):
            tx = self.txs.pop(tx_hash)
            tx_hashXs = {hashX for hashX, value in tx.in_pairs}.union({hashX for hashX, value in tx.out_pairs})
            for hashX in tx_hashXs:
                if hashX in self.touched_hashXs and tx_hash in self.touched_hashXs[hashX]:
                    self.touched_hashXs[hashX].remove(tx_hash)
                    if not self.touched_hashXs[hashX]:
                        self.touched_hashXs.pop(hashX)

    def update_mempool(self, to_add: typing.List[typing.Tuple[bytes, bytes]]) -> typing.Set[bytes]:
        prefix_db = self._db.prefix_db
        touched_hashXs = set()

        # Re-sync with the new set of hashes
        tx_map = {}
        for tx_hash, raw_tx in to_add:
            if tx_hash in self.txs:
                continue
            tx, tx_size = Deserializer(raw_tx).read_tx_and_vsize()
            # Convert the inputs and outputs into (hashX, value) pairs
            # Drop generation-like inputs from MemPoolTx.prevouts
            txin_pairs = tuple((txin.prev_hash, txin.prev_idx)
                               for txin in tx.inputs
                               if not txin.is_generation())
            txout_pairs = tuple((self.coin.hashX_from_txo(txout), txout.value)
                                for txout in tx.outputs if txout.pk_script)
            tx_map[tx_hash] = MemPoolTx(None, txin_pairs, txout_pairs, 0, tx_size, raw_tx)

        for tx_hash, tx in tx_map.items():
            prevouts = []
            # Look up the prevouts
            for prev_hash, prev_index in tx.in_pairs:
                if prev_hash in self.txs:  # accepted mempool
                    utxo = self.txs[prev_hash].out_pairs[prev_index]
                elif prev_hash in tx_map:  # this set of changes
                    utxo = tx_map[prev_hash].out_pairs[prev_index]
                else:  # get it from the db
                    prev_tx_num = prefix_db.tx_num.get(prev_hash)
                    if not prev_tx_num:
                        continue
                    prev_tx_num = prev_tx_num.tx_num
                    hashX_val = prefix_db.hashX_utxo.get(prev_hash[:4], prev_tx_num, prev_index)
                    if not hashX_val:
                        continue
                    hashX = hashX_val.hashX
                    utxo_value = prefix_db.utxo.get(hashX, prev_tx_num, prev_index)
                    utxo = (hashX, utxo_value.amount)
                prevouts.append(utxo)

            # Save the prevouts, compute the fee and accept the TX
            tx.prevouts = tuple(prevouts)
            # Avoid negative fees if dealing with generation-like transactions
            # because some in_parts would be missing
            tx.fee = max(0, (sum(v for _, v in tx.prevouts) -
                             sum(v for _, v in tx.out_pairs)))
            self.txs[tx_hash] = tx
            for hashX, value in itertools.chain(tx.prevouts, tx.out_pairs):
                self.touched_hashXs[hashX].add(tx_hash)
                touched_hashXs.add(hashX)

        return touched_hashXs

    def clear(self):
        self.txs.clear()
        self.touched_hashXs.clear()
