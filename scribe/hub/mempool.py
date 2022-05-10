import asyncio
import itertools
import attr
import typing
import logging
from collections import defaultdict
from prometheus_client import Histogram, Gauge
import rocksdb.errors
from scribe import PROMETHEUS_NAMESPACE
from scribe.common import HISTOGRAM_BUCKETS
from scribe.db.common import UTXO
from scribe.blockchain.transaction.deserializer import Deserializer

if typing.TYPE_CHECKING:
    from scribe.hub.session import SessionManager
    from scribe.db import HubDB


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


NAMESPACE = f"{PROMETHEUS_NAMESPACE}_hub"
mempool_process_time_metric = Histogram(
    "processed_mempool", "Time to process mempool and notify touched addresses",
    namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
)
mempool_tx_count_metric = Gauge("mempool_tx_count", "Transactions in mempool", namespace=NAMESPACE)
mempool_touched_address_count_metric = Gauge(
    "mempool_touched_address_count", "Count of addresses touched by transactions in mempool", namespace=NAMESPACE
)


class HubMemPool:
    def __init__(self, coin, db: 'HubDB', refresh_secs=1.0):
        self.coin = coin
        self._db = db
        self.logger = logging.getLogger(__name__)
        self.txs = {}
        self.raw_mempool = {}
        self.tx_touches = {}
        self.touched_hashXs: typing.DefaultDict[bytes, typing.Set[bytes]] = defaultdict(set)  # None can be a key
        self.refresh_secs = refresh_secs
        self.mempool_process_time_metric = mempool_process_time_metric
        self.session_manager: typing.Optional['SessionManager'] = None
        self._notification_q = asyncio.Queue()

    def refresh(self) -> typing.Set[bytes]:  # returns list of new touched hashXs
        prefix_db = self._db.prefix_db
        mempool_tx_hashes = set()
        try:
            lower, upper = prefix_db.mempool_tx.MIN_TX_HASH, prefix_db.mempool_tx.MAX_TX_HASH
            for k, v in prefix_db.mempool_tx.iterate(start=(lower,), stop=(upper,)):
                self.raw_mempool[k.tx_hash] = v.raw_tx
                mempool_tx_hashes.add(k.tx_hash)
            for removed_mempool_tx in set(self.raw_mempool.keys()).difference(mempool_tx_hashes):
                self.raw_mempool.pop(removed_mempool_tx)
        except rocksdb.errors.RocksIOError as err:
            # FIXME: why does this happen? can it happen elsewhere?
            if err.args[0].startswith(b'IO error: No such file or directory: While open a file for random read:'):
                self.logger.error("failed to process mempool, retrying later")
                return set()
            raise err
        # hashXs = self.hashXs  # hashX: [tx_hash, ...]
        touched_hashXs = set()

        # Remove txs that aren't in mempool anymore
        for tx_hash in set(self.txs).difference(self.raw_mempool.keys()):
            tx = self.txs.pop(tx_hash)
            tx_hashXs = self.tx_touches.pop(tx_hash)
            for hashX in tx_hashXs:
                if hashX in self.touched_hashXs:
                    if tx_hash in self.touched_hashXs[hashX]:
                        self.touched_hashXs[hashX].remove(tx_hash)
                    if not len(self.touched_hashXs[hashX]):
                        self.touched_hashXs.pop(hashX)
            touched_hashXs.update(tx_hashXs)

        # Re-sync with the new set of hashes
        tx_map = {}
        for tx_hash, raw_tx in self.raw_mempool.items():
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
            self.tx_touches[tx_hash] = tx_touches = set()
            # print(f"added {tx_hash[::-1].hex()} reader to mempool")

            for hashX, value in itertools.chain(tx.prevouts, tx.out_pairs):
                self.touched_hashXs[hashX].add(tx_hash)
                touched_hashXs.add(hashX)
                tx_touches.add(hashX)

        mempool_tx_count_metric.set(len(self.txs))
        mempool_touched_address_count_metric.set(len(self.touched_hashXs))
        return touched_hashXs

    def transaction_summaries(self, hashX):
        """Return a list of MemPoolTxSummary objects for the hashX."""
        result = []
        for tx_hash in self.touched_hashXs.get(hashX, ()):
            if tx_hash not in self.txs:
                continue  # the tx hash for the touched address is an input that isn't in mempool anymore
            tx = self.txs[tx_hash]
            has_ui = any(_hash in self.txs for _hash, idx in tx.in_pairs)
            result.append(MemPoolTxSummary(tx_hash, tx.fee, has_ui))
        return result

    def mempool_history(self, hashX: bytes) -> str:
        result = ''
        for tx_hash in self.touched_hashXs.get(hashX, ()):
            if tx_hash not in self.txs:
                continue  # the tx hash for the touched address is an input that isn't in mempool anymore
            result += f'{tx_hash[::-1].hex()}:{-any(_hash in self.txs for _hash, idx in self.txs[tx_hash].in_pairs):d}:'
        return result

    def unordered_UTXOs(self, hashX):
        """Return an unordered list of UTXO named tuples from mempool
        transactions that pay to hashX.
        This does not consider if any other mempool transactions spend
        the outputs.
        """
        utxos = []
        for tx_hash in self.touched_hashXs.get(hashX, ()):
            tx = self.txs.get(tx_hash)
            if not tx:
                self.logger.error("%s isn't in mempool", tx_hash[::-1].hex())
                continue
            for pos, (hX, value) in enumerate(tx.out_pairs):
                if hX == hashX:
                    utxos.append(UTXO(-1, pos, tx_hash, 0, value))
        return utxos

    def potential_spends(self, hashX):
        """Return a set of (prev_hash, prev_idx) pairs from mempool
        transactions that touch hashX.
        None, some or all of these may be spends of the hashX, but all
        actual spends of it (in the DB or mempool) will be included.
        """
        result = set()
        for tx_hash in self.touched_hashXs.get(hashX, ()):
            tx = self.txs[tx_hash]
            result.update(tx.prevouts)
        return result

    def balance_delta(self, hashX):
        """Return the unconfirmed amount in the mempool for hashX.
        Can be positive or negative.
        """
        value = 0
        if hashX in self.touched_hashXs:
            for h in self.touched_hashXs[hashX]:
                tx = self.txs[h]
                value -= sum(v for h168, v in tx.in_pairs if h168 == hashX)
                value += sum(v for h168, v in tx.out_pairs if h168 == hashX)
        return value

    def get_mempool_height(self, tx_hash: bytes) -> int:
        # Height Progression
        #   -2: not broadcast
        #   -1: in mempool but has unconfirmed inputs
        #    0: in mempool and all inputs confirmed
        # +num: confirmed in a specific block (height)
        if tx_hash not in self.txs:
            return -2
        tx = self.txs[tx_hash]
        unspent_inputs = any(_hash in self.raw_mempool for _hash, idx in tx.in_pairs)
        if unspent_inputs:
            return -1
        return 0

    async def start(self, height, session_manager: 'SessionManager'):
        self.session_manager = session_manager
        await self._notify_sessions(height, set(), set())

    async def on_mempool(self, touched, new_touched, height):
        await self._notify_sessions(height, touched, new_touched)

    async def on_block(self, touched, height):
        await self._notify_sessions(height, touched, set())

    async def send_notifications_forever(self, started):
        started.set()
        while True:
            (session_id, height_changed, hashXes) = await self._notification_q.get()
            session = self.session_manager.sessions.get(session_id)
            if session:
                if session.subscribe_headers and height_changed:
                    asyncio.create_task(
                        session.send_notification('blockchain.headers.subscribe',
                                                  (self.session_manager.hsub_results[session.subscribe_headers_raw],))
                    )
                if hashXes:
                    asyncio.create_task(session.send_history_notifications(*hashXes))

    async def _notify_sessions(self, height, touched, new_touched):
        """Notify sessions about height changes and touched addresses."""
        height_changed = height != self.session_manager.notified_height
        if height_changed:
            await self.session_manager._refresh_hsub_results(height)

        if not self.session_manager.sessions:
            return

        if height_changed:
            for hashX in touched.intersection(self.session_manager.mempool_statuses.keys()):
                self.session_manager.mempool_statuses.pop(hashX, None)
        # self.bp._chain_executor
        await asyncio.get_event_loop().run_in_executor(
            self._db._executor, touched.intersection_update, self.session_manager.hashx_subscriptions_by_session.keys()
        )

        session_hashxes_to_notify = defaultdict(list)
        notified_hashxs = 0
        sent_headers = 0

        if touched or new_touched or (height_changed and self.session_manager.mempool_statuses):
            to_notify = touched if height_changed else new_touched

            for hashX in to_notify:
                if hashX not in self.session_manager.hashx_subscriptions_by_session:
                    continue
                for session_id in self.session_manager.hashx_subscriptions_by_session[hashX]:
                    session_hashxes_to_notify[session_id].append(hashX)
                    notified_hashxs += 1

        for session_id, session in self.session_manager.sessions.items():
            hashXes = None
            if session_id in session_hashxes_to_notify:
                hashXes = session_hashxes_to_notify[session_id]
            elif not session.subscribe_headers:
                continue
            if session.subscribe_headers and height_changed:
                sent_headers += 1
            self._notification_q.put_nowait((session_id, height_changed, hashXes))
        if sent_headers:
            self.logger.info(f'notified {sent_headers} sessions of new block header')
        if session_hashxes_to_notify:
            self.logger.info(f'notified {len(session_hashxes_to_notify)} sessions/{notified_hashxs:,d} touched addresses')
