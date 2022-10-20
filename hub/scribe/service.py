import time
import asyncio
import typing
from bisect import bisect_right
from struct import pack
from typing import Optional, List, Tuple, Set, DefaultDict, Dict
from prometheus_client import Gauge, Histogram
from collections import defaultdict

from hub import PROMETHEUS_NAMESPACE
from hub.db.prefixes import ACTIVATED_SUPPORT_TXO_TYPE, ACTIVATED_CLAIM_TXO_TYPE
from hub.db.prefixes import PendingActivationKey, PendingActivationValue, ClaimToTXOValue
from hub.error.base import ChainError
from hub.common import hash_to_hex_str, hash160, RPCError, HISTOGRAM_BUCKETS, StagedClaimtrieItem, sha256, LFUCache, LFUCacheWithMetrics
from hub.scribe.db import PrimaryDB
from hub.scribe.daemon import LBCDaemon
from hub.scribe.transaction import Tx, TxOutput, TxInput, Block
from hub.scribe.prefetcher import Prefetcher
from hub.scribe.mempool import MemPool
from hub.schema.url import normalize_name
from hub.service import BlockchainService
if typing.TYPE_CHECKING:
    from hub.scribe.env import BlockchainEnv
    from hub.db.revertable import RevertableOpStack


NAMESPACE = f"{PROMETHEUS_NAMESPACE}_writer"


class BlockchainProcessorService(BlockchainService):
    """Process blocks and update the DB state to match.

    Employ a prefetcher to prefetch blocks in batches for processing.
    Coordinate backing up in case of chain reorganisations.
    """

    block_count_metric = Gauge(
        "block_count", "Number of processed blocks", namespace=NAMESPACE
    )
    block_update_time_metric = Histogram(
        "block_time", "Block update times", namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
    )
    mempool_update_time_metric = Histogram(
        "mempool_time", "Block update times", namespace=NAMESPACE, buckets=HISTOGRAM_BUCKETS
    )
    reorg_count_metric = Gauge(
        "reorg_count", "Number of reorgs", namespace=NAMESPACE
    )

    def __init__(self, env: 'BlockchainEnv'):
        super().__init__(env, secondary_name='', thread_workers=1, thread_prefix='block-processor')
        self.env = env
        self.daemon = LBCDaemon(env.coin, env.daemon_url, daemon_ca_path=env.daemon_ca_path)
        self.mempool = MemPool(env.coin, self.db)
        self.coin = env.coin
        self.wait_for_blocks_duration = 0.1
        self._ready_to_stop = asyncio.Event()
        self.blocks_event = asyncio.Event()
        self.advanced = asyncio.Event()
        self.prefetcher = Prefetcher(self.daemon, env.coin, self.blocks_event)
        self._caught_up_event: Optional[asyncio.Event] = None
        self.height = 0
        self.tip = bytes.fromhex(self.coin.GENESIS_HASH)[::-1]
        self.tx_count = 0

        self.touched_hashXs: Set[bytes] = set()
        self.utxo_cache: Dict[Tuple[bytes, int], Tuple[bytes, int]] = {}

        #################################
        # attributes used for calculating stake activations and takeovers per block
        #################################

        self.taken_over_names: Set[str] = set()
        # txo to pending claim
        self.txo_to_claim: Dict[Tuple[int, int], StagedClaimtrieItem] = {}
        # claim hash to pending claim txo
        self.claim_hash_to_txo: Dict[bytes, Tuple[int, int]] = {}
        # claim hash to lists of pending support txos
        self.support_txos_by_claim: DefaultDict[bytes, List[Tuple[int, int]]] = defaultdict(list)
        # support txo: (supported claim hash, support amount)
        self.support_txo_to_claim: Dict[Tuple[int, int], Tuple[bytes, int, str]] = {}
        # removed supports {name: {claim_hash: [(tx_num, nout), ...]}}
        self.abandoned_claims: Dict[bytes, StagedClaimtrieItem] = {}
        self.updated_claims: Set[bytes] = set()
        # removed activated support amounts by claim hash
        self.removed_active_support_amount_by_claim: DefaultDict[bytes, List[int]] = defaultdict(list)
        # pending activated support amounts by claim hash
        self.activated_support_amount_by_claim: DefaultDict[bytes, List[int]] = defaultdict(list)
        # pending activated name and claim hash to claim/update txo amount
        self.activated_claim_amount_by_name_and_hash: Dict[Tuple[str, bytes], int] = {}
        # pending claim and support activations per claim hash per name,
        # used to process takeovers due to added activations
        activation_by_claim_by_name_type = DefaultDict[str, DefaultDict[bytes, List[Tuple[PendingActivationKey, int]]]]
        self.activation_by_claim_by_name: activation_by_claim_by_name_type = defaultdict(lambda: defaultdict(list))
        # these are used for detecting early takeovers by not yet activated claims/supports
        self.possible_future_support_amounts_by_claim_hash: DefaultDict[bytes, List[int]] = defaultdict(list)
        self.possible_future_claim_amount_by_name_and_hash: Dict[Tuple[str, bytes], int] = {}
        self.possible_future_support_txos_by_claim_hash: DefaultDict[bytes, List[Tuple[int, int]]] = defaultdict(list)

        self.removed_claims_to_send_es = set()  # cumulative changes across blocks to send ES
        self.touched_claims_to_send_es = set()

        self.removed_claim_hashes: Set[bytes] = set()  # per block changes
        self.touched_claim_hashes: Set[bytes] = set()

        self.signatures_changed = set()

        self.pending_reposted = set()
        self.pending_channel_counts = defaultdict(lambda: 0)
        self.pending_support_amount_change = defaultdict(lambda: 0)

        self.pending_channels = {}
        self.amount_cache = {}
        self.expired_claim_hashes: Set[bytes] = set()

        self.doesnt_have_valid_signature: Set[bytes] = set()
        self.claim_channels: Dict[bytes, bytes] = {}
        self.hashXs_by_tx: DefaultDict[bytes, List[int]] = defaultdict(list)

        self.pending_transaction_num_mapping: Dict[bytes, int] = {}
        self.pending_transactions: Dict[int, bytes] = {}

        # {claim_hash: <count>}
        self.reposted_count_delta = defaultdict(int)
        # {claim_hash: <change in effective amount>}
        self.effective_amount_delta = defaultdict(int)
        self.active_support_amount_delta = defaultdict(int)
        self.future_effective_amount_delta = defaultdict(int)

        self.spent_utxos = set()

        self.hashX_history_cache = LFUCacheWithMetrics(max(100, env.hashX_history_cache_size), 'hashX_history', NAMESPACE)
        self.hashX_full_cache = LFUCacheWithMetrics(max(100, env.hashX_history_cache_size), 'hashX_full', NAMESPACE)
        self.history_tx_info_cache = LFUCacheWithMetrics(max(100, env.history_tx_cache_size), 'hashX_tx', NAMESPACE)

    def open_db(self):
        env = self.env
        self.db = PrimaryDB(
            env.coin, env.db_dir, env.reorg_limit, cache_all_claim_txos=env.cache_all_claim_txos,
            cache_all_tx_hashes=env.cache_all_tx_hashes, max_open_files=env.db_max_open_files,
            blocking_channel_ids=env.blocking_channel_ids, filtering_channel_ids=env.filtering_channel_ids,
            executor=self._executor, index_address_status=env.index_address_status,
            enforce_integrity=not env.db_disable_integrity_checks
        )

    async def run_in_thread_with_lock(self, func, *args):
        # Run in a thread to prevent blocking.  Shielded so that
        # cancellations from shutdown don't lose work - when the task
        # completes the data will be flushed and then we shut down.
        # Take the state lock to be certain in-memory state is
        # consistent and not being updated elsewhere.
        async def run_in_thread_locked():
            async with self.lock:
                return await asyncio.get_event_loop().run_in_executor(self._executor, func, *args)
        return await asyncio.shield(run_in_thread_locked())

    async def run_in_thread(self, func, *args):
        async def run_in_thread():
            return await asyncio.get_event_loop().run_in_executor(self._executor, func, *args)
        return await asyncio.shield(run_in_thread())

    async def refresh_mempool(self):
        def fetch_mempool(mempool_prefix):
            lower, upper = mempool_prefix.MIN_TX_HASH, mempool_prefix.MAX_TX_HASH
            return {
                k.tx_hash: v.raw_tx for (k, v) in mempool_prefix.iterate(start=(lower,), stop=(upper,))
            }

        def update_mempool(unsafe_commit, mempool_prefix, to_put, to_delete):
            self.mempool.remove(to_delete)
            touched_hashXs = self.mempool.update_mempool(to_put)
            if self.env.index_address_status:
                for hashX in touched_hashXs:
                    self._get_update_hashX_mempool_status_ops(hashX)
            for tx_hash, raw_tx in to_put:
                mempool_prefix.stash_put((tx_hash,), (raw_tx,))
            for tx_hash, raw_tx in to_delete.items():
                mempool_prefix.stash_delete((tx_hash,), (raw_tx,))
            unsafe_commit()

        async with self.lock:
            current_mempool = await self.run_in_thread(fetch_mempool, self.db.prefix_db.mempool_tx)
            _to_put = []
            try:
                mempool_txids = await self.daemon.mempool_hashes()
            except (TypeError, RPCError) as err:
                self.log.exception("failed to get mempool tx hashes, reorg underway? (%s)", err)
                return
            for mempool_txid in mempool_txids:
                tx_hash = bytes.fromhex(mempool_txid)[::-1]
                if tx_hash in current_mempool:
                    current_mempool.pop(tx_hash)
                else:
                    try:
                        _to_put.append((tx_hash, bytes.fromhex(await self.daemon.getrawtransaction(mempool_txid))))
                    except (TypeError, RPCError):
                        self.log.warning("failed to get a mempool tx, reorg underway?")
                        return
            if current_mempool:
                if bytes.fromhex(await self.daemon.getbestblockhash())[::-1] != self.db.block_hashes[-1]:
                    return
            await self.run_in_thread(
                update_mempool, self.db.prefix_db.unsafe_commit, self.db.prefix_db.mempool_tx, _to_put, current_mempool
            )

    async def check_and_advance_blocks(self, raw_blocks):
        """Process the list of raw blocks passed.  Detects and handles
        reorgs.
        """

        if not raw_blocks:
            return
        first = self.height + 1
        blocks = [self.coin.block(raw_block, first + n)
                  for n, raw_block in enumerate(raw_blocks)]
        headers = [block.header for block in blocks]
        hprevs = [self.coin.header_prevhash(h) for h in headers]
        chain = [self.tip] + [self.coin.header_hash(h) for h in headers[:-1]]

        if hprevs == chain:
            total_start = time.perf_counter()
            try:
                for block in blocks:
                    if self._stopping:
                        return
                    start = time.perf_counter()
                    start_count = self.tx_count
                    txi_count, txo_count, c_added, c_spent, s_added, s_spent, abandoned, abandoned_chan = await self.run_in_thread_with_lock(
                        self.advance_block, block
                    )
                    txs_added = self.tx_count - start_count
                    self.log.info(
                        "advanced to %i, %i(+%i) txs, -%i/+%i utxos, -%i/+%i claims (%i/%i abandoned), -%i/+%i supports in %0.3fs", self.height,
                        self.tx_count, txs_added, txi_count, txo_count, c_spent, c_added, abandoned, abandoned_chan, s_spent, s_added,
                        time.perf_counter() - start
                    )
                    if self.height == self.coin.nExtendedClaimExpirationForkHeight:
                        self.log.warning(
                            "applying extended claim expiration fork on claims accepted by, %i", self.height
                        )
                        await self.run_in_thread_with_lock(self.db.apply_expiration_extension_fork)
                    self.advanced.set()
            except:
                self.log.exception("advance blocks failed")
                raise
            processed_time = time.perf_counter() - total_start
            self.block_count_metric.set(self.height)
            self.block_update_time_metric.observe(processed_time)
            self.touched_hashXs.clear()
        elif hprevs[0] != chain[0]:
            min_start_height = max(self.height - self.coin.REORG_LIMIT, 0)
            count = 1
            block_hashes_from_lbrycrd = await self.daemon.block_hex_hashes(
                min_start_height, self.coin.REORG_LIMIT
            )
            for height, block_hash in zip(
                    reversed(range(min_start_height, min_start_height + self.coin.REORG_LIMIT)),
                    reversed(block_hashes_from_lbrycrd)):
                if self.db.get_block_hash(height)[::-1].hex() == block_hash:
                    break
                count += 1
            self.log.warning(f"blockchain reorg detected at {self.height}, unwinding last {count} blocks")
            try:
                assert count > 0, count
                for _ in range(count):
                    await self.run_in_thread_with_lock(self.backup_block)
                    self.log.info(f'backed up to height {self.height:,d}')

                    if self.env.cache_all_claim_txos:
                        await self.db._read_claim_txos()  # TODO: don't do this
                await self.prefetcher.reset_height(self.height)
                self.reorg_count_metric.inc()
            except:
                self.log.exception("reorg blocks failed")
                raise
            finally:
                self.log.info("backed up to block %i", self.height)
        else:
            # It is probably possible but extremely rare that what
            # bitcoind returns doesn't form a chain because it
            # reorg-ed the chain as it was processing the batched
            # block hash requests.  Should this happen it's simplest
            # just to reset the prefetcher and try again.
            self.log.warning('daemon blocks do not form a chain; '
                                'resetting the prefetcher')
            await self.prefetcher.reset_height(self.height)

    async def wait_until_block(self, height: int):
        while self.height < height:
            await self.advanced.wait()
            self.advanced.clear()
            if self.height >= height:
                break

    def _add_claim_or_update(self, height: int, txo: 'TxOutput', tx_hash: bytes, tx_num: int, nout: int,
                             spent_claims: typing.Dict[bytes, typing.Tuple[int, int, str]], first_input: 'TxInput'):
        try:
            claim_name = txo.claim.name.decode()
        except UnicodeDecodeError:
            claim_name = ''.join(chr(c) for c in txo.claim.name)
        try:
            normalized_name = normalize_name(claim_name)
        except UnicodeDecodeError:
            normalized_name = claim_name
        if txo.is_claim:
            claim_hash = hash160(tx_hash + pack('>I', nout))[::-1]
            # print(f"\tnew {claim_hash.hex()} ({tx_num} {txo.value})")
        else:
            claim_hash = txo.claim.claim_hash[::-1]
            # print(f"\tupdate {claim_hash.hex()} ({tx_num} {txo.value})")

        signing_channel_hash = None
        channel_signature_is_valid = False
        reposted_claim_hash = None

        try:
            signable = txo.metadata
            is_repost = signable.is_repost
            if is_repost:
                reposted_claim_hash = signable.repost.reference.claim_hash[::-1]
                self.pending_reposted.add(reposted_claim_hash)
                self.reposted_count_delta[reposted_claim_hash] += 1
            is_channel = signable.is_channel
            if is_channel:
                self.pending_channels[claim_hash] = signable.channel.public_key_bytes
            if signable.is_signed:
                signing_channel_hash = signable.signing_channel_hash[::-1]
        except:  # google.protobuf.message.DecodeError: Could not parse JSON.
            signable = None
            # is_repost = False
            # is_channel = False
            reposted_claim_hash = None

        self.doesnt_have_valid_signature.add(claim_hash)
        raw_channel_tx = None
        if signable and signable.signing_channel_hash:
            signing_channel = self.db.get_claim_txo(signing_channel_hash)

            if signing_channel:
                raw_channel_tx = self.db.prefix_db.tx.get(
                    self.db.get_tx_hash(signing_channel.tx_num), deserialize_value=False
                )
            channel_pub_key_bytes = None
            try:
                if not signing_channel:
                    if txo.metadata.signing_channel_hash[::-1] in self.pending_channels:
                        channel_pub_key_bytes = self.pending_channels[signing_channel_hash]
                elif raw_channel_tx:
                    chan_output = self.coin.transaction(raw_channel_tx).outputs[signing_channel.position]
                    channel_meta = chan_output.metadata  # TODO: catch decode/type errors explicitly
                    channel_pub_key_bytes = channel_meta.channel.public_key_bytes
                if channel_pub_key_bytes:
                    channel_signature_is_valid = self.coin.verify_signed_metadata(
                        channel_pub_key_bytes, txo, first_input
                    )
                    if channel_signature_is_valid:
                        # print("\tvalidated signed claim")
                        self.pending_channel_counts[signing_channel_hash] += 1
                        self.doesnt_have_valid_signature.remove(claim_hash)
                        self.claim_channels[claim_hash] = signing_channel_hash
                    # else:
                    #     print("\tfailed to validate signed claim")
            except:
                self.log.exception(f"error validating channel signature for %s:%i", tx_hash[::-1].hex(), nout)

        if txo.is_claim:  # it's a root claim
            root_tx_num, root_idx = tx_num, nout
            previous_amount = 0
        else:  # it's a claim update
            if claim_hash not in spent_claims:
                # print(f"\tthis is a wonky tx, contains unlinked claim update {claim_hash.hex()}")
                return
            if normalized_name != spent_claims[claim_hash][2]:
                self.log.warning(
                    f"{tx_hash[::-1].hex()} contains mismatched name for claim update {claim_hash.hex()}"
                )
                return
            (prev_tx_num, prev_idx, _) = spent_claims.pop(claim_hash)
            # print(f"\tupdate {claim_hash.hex()} {tx_hash[::-1].hex()} {txo.value}")
            if (prev_tx_num, prev_idx) in self.txo_to_claim:
                previous_claim = self.txo_to_claim.pop((prev_tx_num, prev_idx))
                self.claim_hash_to_txo.pop(claim_hash)
                root_tx_num, root_idx = previous_claim.root_tx_num, previous_claim.root_position
            else:
                previous_claim = self._make_pending_claim_txo(claim_hash)
                root_tx_num, root_idx = previous_claim.root_tx_num, previous_claim.root_position
                activation = self.db.get_activation(prev_tx_num, prev_idx)
                claim_name = previous_claim.name
                self.get_remove_activate_ops(
                    ACTIVATED_CLAIM_TXO_TYPE, claim_hash, prev_tx_num, prev_idx, activation, normalized_name,
                    previous_claim.amount
                )
            previous_amount = previous_claim.amount
            self.updated_claims.add(claim_hash)

        if self.env.cache_all_claim_txos:
            self.db.claim_to_txo[claim_hash] = ClaimToTXOValue(
                tx_num, nout, root_tx_num, root_idx, txo.value, channel_signature_is_valid, claim_name
            )
            self.db.txo_to_claim[tx_num][nout] = claim_hash

        pending = StagedClaimtrieItem(
            claim_name, normalized_name, claim_hash, txo.value, self.coin.get_expiration_height(height), tx_num, nout,
            root_tx_num, root_idx, channel_signature_is_valid, signing_channel_hash, reposted_claim_hash
        )
        self.txo_to_claim[(tx_num, nout)] = pending
        self.claim_hash_to_txo[claim_hash] = (tx_num, nout)
        self.get_add_claim_utxo_ops(pending)
        self.future_effective_amount_delta[claim_hash] += txo.value

    def get_add_claim_utxo_ops(self, pending: StagedClaimtrieItem):
        # claim tip by claim hash
        self.db.prefix_db.claim_to_txo.stash_put(
            (pending.claim_hash,), (pending.tx_num, pending.position, pending.root_tx_num, pending.root_position,
                                    pending.amount, pending.channel_signature_is_valid, pending.name)
        )
        # claim hash by txo
        self.db.prefix_db.txo_to_claim.stash_put(
            (pending.tx_num, pending.position), (pending.claim_hash, pending.normalized_name)
        )

        # claim expiration
        self.db.prefix_db.claim_expiration.stash_put(
            (pending.expiration_height, pending.tx_num, pending.position),
            (pending.claim_hash, pending.normalized_name)
        )

        # short url resolution
        for prefix_len in range(10):
            self.db.prefix_db.claim_short_id.stash_put(
                (pending.normalized_name, pending.claim_hash.hex()[:prefix_len + 1],
                        pending.root_tx_num, pending.root_position),
                (pending.tx_num, pending.position)
            )

        if pending.signing_hash and pending.channel_signature_is_valid:
            # channel by stream
            self.db.prefix_db.claim_to_channel.stash_put(
                (pending.claim_hash, pending.tx_num, pending.position), (pending.signing_hash,)
            )
            # stream by channel
            self.db.prefix_db.channel_to_claim.stash_put(
                (pending.signing_hash, pending.normalized_name, pending.tx_num, pending.position),
                (pending.claim_hash,)
            )

        if pending.reposted_claim_hash:
            self.db.prefix_db.repost.stash_put((pending.claim_hash,), (pending.reposted_claim_hash,))
            self.db.prefix_db.reposted_claim.stash_put(
                (pending.reposted_claim_hash, pending.tx_num, pending.position), (pending.claim_hash,)
            )

    def get_remove_claim_utxo_ops(self, pending: StagedClaimtrieItem):
        # claim tip by claim hash
        self.db.prefix_db.claim_to_txo.stash_delete(
            (pending.claim_hash,), (pending.tx_num, pending.position, pending.root_tx_num, pending.root_position,
                                    pending.amount, pending.channel_signature_is_valid, pending.name)
        )
        # claim hash by txo
        self.db.prefix_db.txo_to_claim.stash_delete(
            (pending.tx_num, pending.position), (pending.claim_hash, pending.normalized_name)
        )

        # claim expiration
        self.db.prefix_db.claim_expiration.stash_delete(
            (pending.expiration_height, pending.tx_num, pending.position),
            (pending.claim_hash, pending.normalized_name)
        )

        # short url resolution
        for prefix_len in range(10):
            self.db.prefix_db.claim_short_id.stash_delete(
                (pending.normalized_name, pending.claim_hash.hex()[:prefix_len + 1],
                 pending.root_tx_num, pending.root_position),
                (pending.tx_num, pending.position)
            )

        if pending.signing_hash and pending.channel_signature_is_valid:
            # channel by stream
            self.db.prefix_db.claim_to_channel.stash_delete(
                (pending.claim_hash, pending.tx_num, pending.position), (pending.signing_hash,)
            )
            # stream by channel
            self.db.prefix_db.channel_to_claim.stash_delete(
                (pending.signing_hash, pending.normalized_name, pending.tx_num, pending.position),
                (pending.claim_hash,)
            )

        if pending.reposted_claim_hash:
            self.db.prefix_db.repost.stash_delete((pending.claim_hash,), (pending.reposted_claim_hash,))
            self.db.prefix_db.reposted_claim.stash_delete(
                (pending.reposted_claim_hash, pending.tx_num, pending.position), (pending.claim_hash,)
            )
            self.reposted_count_delta[pending.reposted_claim_hash] -= 1

    def _add_support(self, height: int, txo: 'TxOutput', tx_num: int, nout: int):
        supported_claim_hash = txo.support.claim_hash[::-1]
        self.support_txos_by_claim[supported_claim_hash].append((tx_num, nout))
        try:
            normalized_name = normalize_name(txo.support.name.decode())
        except UnicodeDecodeError:
            normalized_name = ''.join(chr(x) for x in txo.support.name)
        self.support_txo_to_claim[(tx_num, nout)] = supported_claim_hash, txo.value, normalized_name
        # print(f"\tsupport claim {supported_claim_hash.hex()} +{txo.value}")

        self.db.prefix_db.claim_to_support.stash_put((supported_claim_hash, tx_num, nout), (txo.value,))
        self.db.prefix_db.support_to_claim.stash_put((tx_num, nout), (supported_claim_hash,))
        self.pending_support_amount_change[supported_claim_hash] += txo.value
        self.future_effective_amount_delta[supported_claim_hash] += txo.value

    def _add_claim_or_support(self, height: int, tx_hash: bytes, tx_num: int, nout: int, txo: 'TxOutput',
                              spent_claims: typing.Dict[bytes, Tuple[int, int, str]], first_input: 'TxInput') -> int:
        if txo.is_claim or txo.is_update:
            self._add_claim_or_update(height, txo, tx_hash, tx_num, nout, spent_claims, first_input)
            return 1
        elif txo.is_support:
            self._add_support(height, txo, tx_num, nout)
            return 2
        return 0

    def _spend_claims_and_supports(self, txis: List[TxInput], spent_claims: Dict[bytes, Tuple[int, int, str]]):
        tx_nums = self.db.get_tx_nums(
            list(
                {tx_hash for (tx_hash, nout, _, _) in txis if tx_hash not in self.pending_transaction_num_mapping}
            )
        )
        need_check_claim_or_support = []
        spent_support_count = 0
        spent_claim_count = 0
        for (tx_hash, nout, _, _) in txis:
            if tx_hash in self.pending_transaction_num_mapping:
                txin_num = self.pending_transaction_num_mapping[tx_hash]
            else:
                txin_num = tx_nums[tx_hash]
            if not txin_num:
                continue

            if (txin_num, nout) not in self.txo_to_claim and (txin_num, nout) not in self.support_txo_to_claim:
                need_check_claim_or_support.append((txin_num, tx_hash, nout))
        spent_supports = {
            (txin_num, tx_hash, nout): support_v.claim_hash for (txin_num, tx_hash, nout), support_v in zip(
                need_check_claim_or_support, self.db.prefix_db.support_to_claim.multi_get(
                    [(tx_num, n) for (tx_num, _, n) in need_check_claim_or_support]))
            if support_v is not None
        }

        spent_support_amounts = {
            (tx_num, nout): packed_support_amount.amount for (tx_num, _, nout), packed_support_amount in zip(
                spent_supports.keys(),
                self.db.prefix_db.claim_to_support.multi_get([
                    (claim_hash, tx_num, nout)
                    for (tx_num, tx_hash, nout), claim_hash in spent_supports.items()]
                )
            )
        }

        to_check_spent_claims = [
            (txin_num, tx_hash, nout) for (txin_num, tx_hash, nout) in need_check_claim_or_support
            if (txin_num, tx_hash, nout) not in spent_supports
        ]
        spent_claim_hashes = {
            (txin_num, tx_hash, nout): v.claim_hash
            for (txin_num, tx_hash, nout), v in zip(
                to_check_spent_claims, self.db.prefix_db.txo_to_claim.multi_get(
                    [(tx_num, n) for (tx_num, _, n) in to_check_spent_claims]
                )
            ) if v is not None
        }
        spent_claim_txos = {
            claim_hash: claim_txo for claim_hash, claim_txo in zip(
                spent_claim_hashes.values(), self.db.prefix_db.claim_to_txo.multi_get(
                    [(c_h,) for c_h in spent_claim_hashes.values()]
                )
            )
        }

        needed_channels_for_claims = []
        needed_reposted = set()
        needed_activations = []

        for (tx_hash, nout, _, _) in txis:
            if tx_hash in self.pending_transaction_num_mapping:
                txin_num = self.pending_transaction_num_mapping[tx_hash]
            else:
                txin_num = tx_nums[tx_hash]
            if not txin_num:
                continue
            if (txin_num, tx_hash, nout) in spent_claim_hashes:
                spent_claim_count += 1
                claim_hash = spent_claim_hashes[(txin_num, tx_hash, nout)]
                claim = spent_claim_txos[claim_hash]
                if claim_hash not in self.doesnt_have_valid_signature:
                    needed_channels_for_claims.append((claim_hash, claim.tx_num, claim.position))

                    # signing_hash = self.db.get_channel_for_claim(claim_hash, claim.tx_num, claim.position)
                # reposted_claim_hash = self.db.get_repost(claim_hash)
                needed_reposted.add(claim_hash)
                # activation = self.db.get_activation(txin_num, nout, is_support=False)
                needed_activations.append((1, txin_num, nout))
            elif (txin_num, tx_hash, nout) in spent_supports:
                spent_support_count += 1
                needed_activations.append((2, txin_num, nout))
                # activation = self.db.get_activation(txin_num, nout, is_support=True)
            else:
                pass
        needed_reposted = list(needed_reposted)
        reposts = {
            claim_hash: v.reposted_claim_hash for claim_hash, v in zip(
                needed_reposted, self.db.prefix_db.repost.multi_get([(c_h,) for c_h in needed_reposted])
            ) if v is not None
        }
        activations = {
            (tx_num, nout): -1 if not v else v.height for (_, tx_num, nout), v in zip(
                needed_activations, self.db.prefix_db.activated.multi_get(needed_activations)
            )
        }
        signing_channels = {
            k: None if not v else v.signing_hash for k, v in zip(
                needed_channels_for_claims, self.db.prefix_db.claim_to_channel.multi_get(needed_channels_for_claims)
            )
        }

        for (tx_hash, nout, _, _) in txis:
            if tx_hash in self.pending_transaction_num_mapping:
                txin_num = self.pending_transaction_num_mapping[tx_hash]
            else:
                txin_num = tx_nums[tx_hash]
            if not txin_num:
                continue
            if (txin_num, nout) in self.txo_to_claim or (txin_num, tx_hash, nout) in spent_claim_hashes:
                if (txin_num, nout) in self.txo_to_claim:
                    spent = self.txo_to_claim[(txin_num, nout)]
                else:
                    claim_hash = spent_claim_hashes[(txin_num, tx_hash, nout)]
                    claim = spent_claim_txos[claim_hash]
                    if claim_hash in self.doesnt_have_valid_signature:
                        signing_hash = None
                    else:
                        signing_hash = signing_channels[(claim_hash, claim.tx_num, claim.position)]
                    reposted_claim_hash = reposts.get(claim_hash)
                    spent = StagedClaimtrieItem(
                        claim.name, claim.normalized_name, claim_hash, claim.amount,
                        self.coin.get_expiration_height(
                            bisect_right(self.db.tx_counts, claim.tx_num),
                            extended=self.height >= self.coin.nExtendedClaimExpirationForkHeight
                        ),
                        claim.tx_num, claim.position, claim.root_tx_num, claim.root_position,
                        claim.channel_signature_is_valid, signing_hash, reposted_claim_hash
                    )
                    activation = activations[(txin_num, nout)]
                    if 0 < activation <= self.height:
                        self.effective_amount_delta[claim_hash] -= spent.amount
                self.future_effective_amount_delta[spent.claim_hash] -= spent.amount
                if self.env.cache_all_claim_txos:
                    claim_hash = self.db.txo_to_claim[txin_num].pop(nout)
                    if not self.db.txo_to_claim[txin_num]:
                        self.db.txo_to_claim.pop(txin_num)
                    self.db.claim_to_txo.pop(claim_hash)
                if spent.reposted_claim_hash:
                    self.pending_reposted.add(spent.reposted_claim_hash)
                if spent.signing_hash and spent.channel_signature_is_valid and spent.signing_hash not in self.abandoned_claims:
                    self.pending_channel_counts[spent.signing_hash] -= 1
                spent_claims[spent.claim_hash] = (spent.tx_num, spent.position, spent.normalized_name)
                self.get_remove_claim_utxo_ops(spent)
                # print(f"\tspent lbry://{spent.name}#{spent.claim_hash.hex()}")
            elif (txin_num, nout) in self.support_txo_to_claim or (txin_num, tx_hash, nout) in spent_supports:
                activation = 0
                if (txin_num, nout) in self.support_txo_to_claim:
                    spent_support, support_amount, supported_name = self.support_txo_to_claim.pop((txin_num, nout))
                    self.support_txos_by_claim[spent_support].remove((txin_num, nout))
                else:
                    spent_support = spent_supports[(txin_num, tx_hash, nout)]
                    support_amount = spent_support_amounts[(txin_num, nout)]
                    supported_name = self._get_pending_claim_name(spent_support)
                    activation = activations[(txin_num, nout)]
                    if 0 < activation <= self.height:
                        self.removed_active_support_amount_by_claim[spent_support].append(support_amount)
                        self.effective_amount_delta[spent_support] -= support_amount
                        self.active_support_amount_delta[spent_support] -= support_amount
                    if supported_name is not None and activation > 0:
                        self.get_remove_activate_ops(
                            ACTIVATED_SUPPORT_TXO_TYPE, spent_support, txin_num, nout, activation, supported_name,
                            support_amount
                        )
                self.db.prefix_db.claim_to_support.stash_delete((spent_support, txin_num, nout), (support_amount,))
                self.db.prefix_db.support_to_claim.stash_delete((txin_num, nout), (spent_support,))
                self.pending_support_amount_change[spent_support] -= support_amount
                self.future_effective_amount_delta[spent_support] -= support_amount
                # print(f"\tspent support for {spent_support.hex()} activation:{activation} {support_amount} {tx_hash[::-1].hex()}:{nout}")
            else:
                pass
        return spent_claim_count, spent_support_count

    def _abandon_claim(self, claim_hash: bytes, tx_num: int, nout: int, normalized_name: str):
        if (tx_num, nout) in self.txo_to_claim:
            pending = self.txo_to_claim.pop((tx_num, nout))
            self.claim_hash_to_txo.pop(claim_hash)
            self.abandoned_claims[pending.claim_hash] = pending
            claim_root_tx_num, claim_root_idx = pending.root_tx_num, pending.root_position
            prev_amount, prev_signing_hash = pending.amount, pending.signing_hash
            reposted_claim_hash, name = pending.reposted_claim_hash, pending.name
            expiration = self.coin.get_expiration_height(self.height)
            signature_is_valid = pending.channel_signature_is_valid
        else:
            v = self.db.get_claim_txo(
                claim_hash
            )
            claim_root_tx_num, claim_root_idx, prev_amount = v.root_tx_num,  v.root_position, v.amount
            signature_is_valid, name = v.channel_signature_is_valid, v.name
            prev_signing_hash = self.db.get_channel_for_claim(claim_hash, tx_num, nout)
            reposted_claim_hash = self.db.get_repost(claim_hash)
            expiration = self.coin.get_expiration_height(bisect_right(self.db.tx_counts, tx_num))
        self.abandoned_claims[claim_hash] = staged = StagedClaimtrieItem(
            name, normalized_name, claim_hash, prev_amount, expiration, tx_num, nout, claim_root_tx_num,
            claim_root_idx, signature_is_valid, prev_signing_hash, reposted_claim_hash
        )
        if normalized_name.startswith('@'):  # abandon a channel, invalidate signatures
            self._invalidate_channel_signatures(claim_hash)

    def _get_invalidate_signature_ops(self, pending: StagedClaimtrieItem):
        if not pending.signing_hash:
            return
        self.db.prefix_db.claim_to_channel.stash_delete(
            (pending.claim_hash, pending.tx_num, pending.position), (pending.signing_hash,)
        )
        if pending.channel_signature_is_valid:
            self.db.prefix_db.channel_to_claim.stash_delete(
                (pending.signing_hash, pending.normalized_name, pending.tx_num, pending.position),
                (pending.claim_hash,)
            )
            self.db.prefix_db.claim_to_txo.stash_delete(
                (pending.claim_hash,),
                (pending.tx_num, pending.position, pending.root_tx_num, pending.root_position, pending.amount,
                 pending.channel_signature_is_valid, pending.name)
            )
            self.db.prefix_db.claim_to_txo.stash_put(
                (pending.claim_hash,),
                (pending.tx_num, pending.position, pending.root_tx_num, pending.root_position, pending.amount,
                 False, pending.name)
            )

    def _invalidate_channel_signatures(self, claim_hash: bytes): # TODO: multi_put
        for (signed_claim_hash, ) in self.db.prefix_db.channel_to_claim.iterate(
                prefix=(claim_hash, ), include_key=False):
            if signed_claim_hash in self.abandoned_claims or signed_claim_hash in self.expired_claim_hashes:
                continue
            # there is no longer a signing channel for this claim as of this block
            if signed_claim_hash in self.doesnt_have_valid_signature:
                continue
            # the signing channel changed in this block
            if signed_claim_hash in self.claim_channels and signed_claim_hash != self.claim_channels[signed_claim_hash]:
                continue

            # if the claim with an invalidated signature is in this block, update the StagedClaimtrieItem
            # so that if we later try to spend it in this block we won't try to delete the channel info twice
            if signed_claim_hash in self.claim_hash_to_txo:
                signed_claim_txo = self.claim_hash_to_txo[signed_claim_hash]
                claim = self.txo_to_claim[signed_claim_txo]
                if claim.signing_hash != claim_hash:  # claim was already invalidated this block
                    continue
                self.txo_to_claim[signed_claim_txo] = claim.invalidate_signature()
            else:
                claim = self._make_pending_claim_txo(signed_claim_hash)
            self.signatures_changed.add(signed_claim_hash)
            self.pending_channel_counts[claim_hash] -= 1
            self._get_invalidate_signature_ops(claim)

        for staged in list(self.txo_to_claim.values()):
            needs_invalidate = staged.claim_hash not in self.doesnt_have_valid_signature
            if staged.signing_hash == claim_hash and needs_invalidate:
                self._get_invalidate_signature_ops(staged)
                self.txo_to_claim[self.claim_hash_to_txo[staged.claim_hash]] = staged.invalidate_signature()
                self.signatures_changed.add(staged.claim_hash)
                self.pending_channel_counts[claim_hash] -= 1

    def _make_pending_claim_txo(self, claim_hash: bytes):
        claim = self.db.get_claim_txo(claim_hash)
        if claim_hash in self.doesnt_have_valid_signature:
            signing_hash = None
        else:
            signing_hash = self.db.get_channel_for_claim(claim_hash, claim.tx_num, claim.position)
        reposted_claim_hash = self.db.get_repost(claim_hash)
        return StagedClaimtrieItem(
            claim.name, claim.normalized_name, claim_hash, claim.amount,
            self.coin.get_expiration_height(
                bisect_right(self.db.tx_counts, claim.tx_num),
                extended=self.height >= self.coin.nExtendedClaimExpirationForkHeight
            ),
            claim.tx_num, claim.position, claim.root_tx_num, claim.root_position,
            claim.channel_signature_is_valid, signing_hash, reposted_claim_hash
        )

    def _expire_claims(self, height: int):
        expired = self.db.get_expired_by_height(height)
        self.expired_claim_hashes.update(set(expired.keys()))
        spent_claims = {}
        txis = [txi for expired_claim_hash, (tx_num, position, name, txi) in expired.items() if (tx_num, position) not in self.txo_to_claim]
        self._spend_claims_and_supports(txis, spent_claims)
        if expired:
            # abandon the channels last to handle abandoned signed claims in the same tx,
            # see test_abandon_channel_and_claims_in_same_tx
            expired_channels = {}
            for abandoned_claim_hash, (tx_num, nout, normalized_name) in spent_claims.items():
                self._abandon_claim(abandoned_claim_hash, tx_num, nout, normalized_name)

                if normalized_name.startswith('@'):
                    expired_channels[abandoned_claim_hash] = (tx_num, nout, normalized_name)
                else:
                    # print(f"\texpire {abandoned_claim_hash.hex()} {tx_num} {nout}")
                    self._abandon_claim(abandoned_claim_hash, tx_num, nout, normalized_name)

            # do this to follow the same content claim removing pathway as if a claim (possible channel) was abandoned
            for abandoned_claim_hash, (tx_num, nout, normalized_name) in expired_channels.items():
                # print(f"\texpire {abandoned_claim_hash.hex()} {tx_num} {nout}")
                self._abandon_claim(abandoned_claim_hash, tx_num, nout, normalized_name)

    def _get_pending_effective_amounts(self, names_and_claim_hashes: List[Tuple[str, bytes]]):
        def _get_pending_claim_amount(name: str, claim_hash: bytes) -> Optional[int]:
            if (name, claim_hash) in self.activated_claim_amount_by_name_and_hash:
                if claim_hash in self.claim_hash_to_txo:
                    return self.txo_to_claim[self.claim_hash_to_txo[claim_hash]].amount
                return self.activated_claim_amount_by_name_and_hash[(name, claim_hash)]
            if (name, claim_hash) in self.possible_future_claim_amount_by_name_and_hash:
                return self.possible_future_claim_amount_by_name_and_hash[(name, claim_hash)]
            if (claim_hash, ACTIVATED_CLAIM_TXO_TYPE, self.height + 1) in self.amount_cache:
                return self.amount_cache[(claim_hash, ACTIVATED_CLAIM_TXO_TYPE, self.height + 1)]
            if claim_hash in self.claim_hash_to_txo:
                return self.txo_to_claim[self.claim_hash_to_txo[claim_hash]].amount
            else:
                return

        def _get_pending_support_amount(claim_hash):
            amount = 0
            if claim_hash in self.activated_support_amount_by_claim:
                amount += sum(self.activated_support_amount_by_claim[claim_hash])
            if claim_hash in self.possible_future_support_amounts_by_claim_hash:
                amount += sum(self.possible_future_support_amounts_by_claim_hash[claim_hash])
            if claim_hash in self.removed_active_support_amount_by_claim:
                amount -= sum(self.removed_active_support_amount_by_claim[claim_hash])
            if (claim_hash, ACTIVATED_SUPPORT_TXO_TYPE, self.height + 1) in self.amount_cache:
                amount += self.amount_cache[(claim_hash, ACTIVATED_SUPPORT_TXO_TYPE, self.height + 1)]
                return amount, False
            return amount, True

        amount_infos = {}
        for name, claim_hash in names_and_claim_hashes:
            claim_amount = _get_pending_claim_amount(name, claim_hash)
            support_amount, need_supports = _get_pending_support_amount(claim_hash)
            amount_infos[claim_hash] = claim_amount is None, need_supports, (claim_amount or 0) + support_amount

        needed_amounts = [
            claim_hash for claim_hash, (need_claim, need_support, _) in amount_infos.items()
            if need_claim or need_support
        ]
        amounts = {
            claim_hash: v for claim_hash, v in zip(
                needed_amounts, self.db.prefix_db.effective_amount.multi_get([(c_h,) for c_h in needed_amounts])
            )
        }
        pending_effective_amounts = {}
        for claim_hash, (need_claim, need_support, pending_amount) in amount_infos.items():
            amount = pending_amount
            if (not need_claim and not need_support) or not amounts[claim_hash]:
                pending_effective_amounts[claim_hash] = amount
                continue
            existing = amounts[claim_hash]
            if need_claim:
                amount += (existing.activated_sum - existing.activated_support_sum)
            if need_support:
                amount += existing.activated_support_sum
            pending_effective_amounts[claim_hash] = amount
        return pending_effective_amounts

    def _get_pending_claim_name(self, claim_hash: bytes) -> Optional[str]:
        assert claim_hash is not None
        if claim_hash in self.claim_hash_to_txo:
            return self.txo_to_claim[self.claim_hash_to_txo[claim_hash]].normalized_name
        claim_info = self.db.get_claim_txo(claim_hash)
        if claim_info:
            return claim_info.normalized_name

    def _get_future_effective_amounts(self, claim_hashes: List[bytes]):
        return {
            claim_hash: (0 if not v else v.future_effective_amount) + self.future_effective_amount_delta.get(
                claim_hash, 0
            )
            for claim_hash, v in zip(
                claim_hashes,
                self.db.prefix_db.future_effective_amount.multi_get(
                    [(claim_hash,) for claim_hash in claim_hashes]
                )
            )
        }

    def get_activate_ops(self, txo_type: int, claim_hash: bytes, tx_num: int, position: int,
                          activation_height: int, name: str, amount: int):
        self.db.prefix_db.activated.stash_put(
            (txo_type, tx_num, position), (activation_height, claim_hash, name)
        )
        self.db.prefix_db.pending_activation.stash_put(
            (activation_height, txo_type, tx_num, position), (claim_hash, name)
        )
        self.db.prefix_db.active_amount.stash_put(
            (claim_hash, txo_type, activation_height, tx_num, position), (amount,)
        )

    def get_remove_activate_ops(self, txo_type: int, claim_hash: bytes, tx_num: int, position: int,
                                activation_height: int, name: str, amount: int):
        self.db.prefix_db.activated.stash_delete(
            (txo_type, tx_num, position), (activation_height, claim_hash, name)
        )
        self.db.prefix_db.pending_activation.stash_delete(
            (activation_height, txo_type, tx_num, position), (claim_hash, name)
        )
        self.db.prefix_db.active_amount.stash_delete(
            (claim_hash, txo_type, activation_height, tx_num, position), (amount,)
        )

    def _get_takeover_ops(self, height: int):

        # cache for controlling claims as of the previous block
        controlling_claims = {}

        def get_controlling(_name):
            if _name not in controlling_claims:
                _controlling = self.db.get_controlling_claim(_name)
                controlling_claims[_name] = _controlling
            else:
                _controlling = controlling_claims[_name]
            return _controlling

        names_with_abandoned_or_updated_controlling_claims: List[str] = []

        # get the claims and supports previously scheduled to be activated at this block
        activated_at_height = self.db.get_activated_at_height(height)
        activate_in_future = defaultdict(lambda: defaultdict(list))
        future_activations = defaultdict(dict)

        def get_delayed_activate_ops(name: str, claim_hash: bytes, is_new_claim: bool, tx_num: int, nout: int,
                                     amount: int, is_support: bool):
            controlling = get_controlling(name)
            nothing_is_controlling = not controlling
            staged_is_controlling = False if not controlling else claim_hash == controlling.claim_hash
            controlling_is_abandoned = False if not controlling else \
                name in names_with_abandoned_or_updated_controlling_claims
            if nothing_is_controlling or staged_is_controlling or controlling_is_abandoned:
                delay = 0
            elif is_new_claim:
                delay = self.coin.get_delay_for_name(height - controlling.height)
            else:
                _amounts = self._get_pending_effective_amounts([(name, controlling.claim_hash), (name, claim_hash)])
                controlling_effective_amount = _amounts[controlling.claim_hash]
                staged_effective_amount = _amounts[claim_hash]
                staged_update_could_cause_takeover = staged_effective_amount > controlling_effective_amount
                delay = 0 if not staged_update_could_cause_takeover else self.coin.get_delay_for_name(
                    height - controlling.height
                )
            if delay == 0:  # if delay was 0 it needs to be considered for takeovers
                activated_at_height[PendingActivationValue(claim_hash, name)].append(
                    PendingActivationKey(
                        height, ACTIVATED_SUPPORT_TXO_TYPE if is_support else ACTIVATED_CLAIM_TXO_TYPE, tx_num, nout
                    )
                )
            else:  # if the delay was higher if still needs to be considered if something else triggers a takeover
                activate_in_future[name][claim_hash].append((
                    PendingActivationKey(
                        height + delay, ACTIVATED_SUPPORT_TXO_TYPE if is_support else ACTIVATED_CLAIM_TXO_TYPE,
                        tx_num, nout
                    ), amount
                ))
                if is_support:
                    self.possible_future_support_txos_by_claim_hash[claim_hash].append((tx_num, nout))
            self.get_activate_ops(
                ACTIVATED_SUPPORT_TXO_TYPE if is_support else ACTIVATED_CLAIM_TXO_TYPE, claim_hash, tx_num, nout,
                height + delay, name, amount
            )

        # determine names needing takeover/deletion due to controlling claims being abandoned
        # and add ops to deactivate abandoned claims
        for claim_hash, staged in self.abandoned_claims.items():
            controlling = get_controlling(staged.normalized_name)
            if controlling and controlling.claim_hash == claim_hash:
                names_with_abandoned_or_updated_controlling_claims.append(staged.normalized_name)
                # print(f"\t{staged.name} needs takeover")
            activation = self.db.get_activation(staged.tx_num, staged.position)
            if activation > 0:  #  db returns -1 for non-existent txos
                # removed queued future activation from the db
                self.get_remove_activate_ops(
                    ACTIVATED_CLAIM_TXO_TYPE, staged.claim_hash, staged.tx_num, staged.position,
                    activation, staged.normalized_name, staged.amount
                )
            else:
                # it hadn't yet been activated
                pass

        # get the removed activated supports for controlling claims to determine if takeovers are possible
        abandoned_support_check_need_takeover = defaultdict(list)
        for claim_hash, amounts in self.removed_active_support_amount_by_claim.items():
            name = self._get_pending_claim_name(claim_hash)
            if name is None:
                continue
            controlling = get_controlling(name)
            if controlling and controlling.claim_hash == claim_hash and \
                    name not in names_with_abandoned_or_updated_controlling_claims:
                abandoned_support_check_need_takeover[(name, claim_hash)].extend(amounts)

        # get the controlling claims with updates to the claim to check if takeover is needed
        for claim_hash in self.updated_claims:
            if claim_hash in self.abandoned_claims:
                continue
            name = self._get_pending_claim_name(claim_hash)
            if name is None:
                continue
            controlling = get_controlling(name)
            if controlling and controlling.claim_hash == claim_hash and \
                    name not in names_with_abandoned_or_updated_controlling_claims:
                names_with_abandoned_or_updated_controlling_claims.append(name)

        # prepare to activate or delay activation of the pending claims being added this block
        for (tx_num, nout), staged in self.txo_to_claim.items():
            is_delayed = not staged.is_update
            prev_txo = self.db.get_cached_claim_txo(staged.claim_hash)
            if prev_txo:
                prev_activation = self.db.get_activation(prev_txo.tx_num, prev_txo.position)
                if height < prev_activation or prev_activation < 0:
                    is_delayed = True
            get_delayed_activate_ops(
                staged.normalized_name, staged.claim_hash, is_delayed, tx_num, nout, staged.amount,
                is_support=False
            )

        # and the supports
        check_supported_claim_exists = set()
        for (tx_num, nout), (claim_hash, amount, supported_name) in self.support_txo_to_claim.items():
            if claim_hash in self.abandoned_claims:
                continue
            elif claim_hash in self.claim_hash_to_txo:
                delay = not self.txo_to_claim[self.claim_hash_to_txo[claim_hash]].is_update
            else:
                check_supported_claim_exists.add(claim_hash)
                delay = True
            get_delayed_activate_ops(
                supported_name, claim_hash, delay, tx_num, nout, amount, is_support=True
            )
        check_supported_claim_exists = list(check_supported_claim_exists)
        # claims that are supported that don't exist (yet, maybe never)
        headless_supported_claims = {
            claim_hash for claim_hash, v in zip(
                check_supported_claim_exists, self.db.prefix_db.claim_to_txo.multi_get(
                    [(c_h,) for c_h in check_supported_claim_exists]
                )
            ) if v is None
        }

        activated_added_to_effective_amount = set()

        # add the activation/delayed-activation ops
        for activated, activated_txos in activated_at_height.items():
            controlling = get_controlling(activated.normalized_name)
            reactivate = False
            if not controlling or controlling.claim_hash == activated.claim_hash:
                # there is no delay for claims to a name without a controlling value or to the controlling value
                reactivate = True
            for activated_txo in activated_txos:
                if (activated_txo.tx_num, activated_txo.position) in self.spent_utxos:
                    continue
                txo_tup = (activated_txo.tx_num, activated_txo.position)
                if activated_txo.is_claim:
                    if txo_tup in self.txo_to_claim:
                        amount = self.txo_to_claim[txo_tup].amount
                    else:
                        amount = self.db.get_claim_txo_amount(
                            activated.claim_hash
                        )
                    if amount is None:
                        # print("\tskip activate for non existent claim")
                        continue
                    self.activated_claim_amount_by_name_and_hash[(activated.normalized_name, activated.claim_hash)] = amount
                else:
                    if txo_tup in self.support_txo_to_claim:
                        amount = self.support_txo_to_claim[txo_tup][1]
                    else:
                        amount = self.db.get_support_txo_amount(
                            activated.claim_hash, activated_txo.tx_num, activated_txo.position
                        )
                    if amount is None:
                        # print("\tskip activate support for non existent claim")
                        continue
                    self.activated_support_amount_by_claim[activated.claim_hash].append(amount)
                    self.active_support_amount_delta[activated.claim_hash] += amount
                self.effective_amount_delta[activated.claim_hash] += amount
                activated_added_to_effective_amount.add(activated.claim_hash)
                self.activation_by_claim_by_name[activated.normalized_name][activated.claim_hash].append((activated_txo, amount))
                # print(f"\tactivate {'support' if txo_type == ACTIVATED_SUPPORT_TXO_TYPE else 'claim'} "
                #       f"{activated.claim_hash.hex()} @ {activated_txo.height}")

        # go through claims where the controlling claim or supports to the controlling claim have been abandoned
        # check if takeovers are needed or if the name node is now empty
        need_reactivate_if_takes_over = {}
        for need_takeover in names_with_abandoned_or_updated_controlling_claims:
            existing = self.db.get_claim_txos_for_name(need_takeover)
            has_candidate = False
            # add existing claims to the queue for the takeover
            # track that we need to reactivate these if one of them becomes controlling
            for candidate_claim_hash, (tx_num, nout) in existing.items():
                if candidate_claim_hash in self.abandoned_claims:
                    continue
                has_candidate = True
                existing_activation = self.db.get_activation(tx_num, nout)
                activate_key = PendingActivationKey(
                    existing_activation, ACTIVATED_CLAIM_TXO_TYPE, tx_num, nout
                )
                self.activation_by_claim_by_name[need_takeover][candidate_claim_hash].append((
                    activate_key, self.db.get_claim_txo_amount(candidate_claim_hash)
                ))
                need_reactivate_if_takes_over[(need_takeover, candidate_claim_hash)] = activate_key
                # print(f"\tcandidate to takeover abandoned controlling claim for "
                #       f"{activate_key.tx_num}:{activate_key.position} {activate_key.is_claim}")
            if not has_candidate:
                # remove name takeover entry, the name is now unclaimed
                controlling = get_controlling(need_takeover)
                self.db.prefix_db.claim_takeover.stash_delete(
                    (need_takeover,), (controlling.claim_hash, controlling.height)
                )

        # scan for possible takeovers out of the accumulated activations, of these make sure there
        # aren't any future activations for the taken over names with yet higher amounts, if there are
        # these need to get activated now and take over instead. for example:
        # claim A is winning for 0.1 for long enough for a > 1 takeover delay
        # claim B is made for 0.2
        # a block later, claim C is made for 0.3, it will schedule to activate 1 (or rarely 2) block(s) after B
        # upon the delayed activation of B, we need to detect to activate C and make it take over early instead

        claim_exists = {}
        needed_future_activated = {}

        future_activated = self.db.get_future_activated(height)

        needed_future_activated_amounts = []

        for activated, activated_claim_txo in future_activated.items():
            needed_future_activated_amounts.append(activated.claim_hash)

        future_amounts = self._get_future_effective_amounts(needed_future_activated_amounts)

        for activated, activated_claim_txo in future_activated.items():
            # uses the pending effective amount for the future activation height, not the current height
            future_amount = future_amounts[activated.claim_hash]
            if activated.claim_hash in self.claim_hash_to_txo:
                claim_exists[activated.claim_hash] = True
            needed_future_activated[activated.claim_hash] = activated, activated_claim_txo, future_amount

        if needed_future_activated:
            for (claim_hash, (activated, activated_claim_txo, future_amount)), claim_txo in zip(
                    needed_future_activated.items(), self.db.prefix_db.claim_to_txo.multi_get(
                        [(c_h,) for c_h in needed_future_activated]
                    )):
                if claim_hash not in claim_exists:
                   claim_exists[claim_hash] = claim_txo is not None
                if claim_exists[activated.claim_hash] and activated.claim_hash not in self.abandoned_claims:
                    future_activations[activated.normalized_name][activated.claim_hash] = future_amount, \
                                                                                          activated, \
                                                                                          activated_claim_txo
        check_exists = set()
        for name, future_activated in activate_in_future.items():
            for claim_hash, activated in future_activated.items():
                if claim_hash not in claim_exists:
                    check_exists.add(claim_hash)

        if check_exists:
            check_exists = list(check_exists)
            for claim_hash, claim_txo in zip(check_exists, self.db.prefix_db.claim_to_txo.multi_get(
                    [(c_h,) for c_h in check_exists])):
                claim_exists[claim_hash] = (claim_txo is not None) or claim_hash in self.claim_hash_to_txo

        for name, future_activated in activate_in_future.items():
            for claim_hash, activated in future_activated.items():
                if not claim_exists[claim_hash]:
                    continue
                if claim_hash in self.abandoned_claims:
                    continue
                for txo in activated:
                    v = txo[1], PendingActivationValue(claim_hash, name), txo[0]
                    future_activations[name][claim_hash] = v
                    if txo[0].is_claim:
                        self.possible_future_claim_amount_by_name_and_hash[(name, claim_hash)] = txo[1]
                    else:
                        self.possible_future_support_amounts_by_claim_hash[claim_hash].append(txo[1])

        # process takeovers

        need_activated_amounts = []
        for name, activated in self.activation_by_claim_by_name.items():
            for claim_hash in activated.keys():
                if claim_hash not in self.abandoned_claims:
                    need_activated_amounts.append((name, claim_hash))
            controlling = controlling_claims[name]
            # if there is a controlling claim include it in the amounts to ensure it remains the max
            if controlling and controlling.claim_hash not in self.abandoned_claims:
                need_activated_amounts.append((name, controlling.claim_hash))
        cumulative_amounts = self._get_pending_effective_amounts(need_activated_amounts)

        checked_names = set()
        for name, activated in self.activation_by_claim_by_name.items():
            checked_names.add(name)
            controlling = controlling_claims[name]
            amounts = {
                claim_hash: cumulative_amounts[claim_hash]
                for claim_hash in activated.keys() if claim_hash not in self.abandoned_claims
            }
            if controlling and controlling.claim_hash not in self.abandoned_claims:
                amounts[controlling.claim_hash] = cumulative_amounts[controlling.claim_hash]
            winning_claim_hash = max(amounts, key=lambda x: amounts[x])
            if not controlling or (winning_claim_hash != controlling.claim_hash and
                                   name in names_with_abandoned_or_updated_controlling_claims) or \
                    ((winning_claim_hash != controlling.claim_hash) and (amounts[winning_claim_hash] > amounts[controlling.claim_hash])):
                amounts_with_future_activations = {claim_hash: amount for claim_hash, amount in amounts.items()}
                amounts_with_future_activations.update(self._get_future_effective_amounts(
                    list(future_activations[name].keys()))
                )
                winning_including_future_activations = max(
                    amounts_with_future_activations, key=lambda x: amounts_with_future_activations[x]
                )
                future_winning_amount = amounts_with_future_activations[winning_including_future_activations]

                if winning_claim_hash != winning_including_future_activations and \
                        future_winning_amount > amounts[winning_claim_hash]:
                    # print(f"\ttakeover by {winning_claim_hash.hex()} triggered early activation and "
                    #       f"takeover by {winning_including_future_activations.hex()} at {height}")
                    # handle a pending activated claim jumping the takeover delay when another name takes over
                    if winning_including_future_activations not in self.claim_hash_to_txo:
                        claim = self.db.get_claim_txo(winning_including_future_activations)
                        tx_num = claim.tx_num
                        position = claim.position
                        amount = claim.amount
                        activation = self.db.get_activation(tx_num, position)
                    else:
                        tx_num, position = self.claim_hash_to_txo[winning_including_future_activations]
                        amount = self.txo_to_claim[(tx_num, position)].amount
                        activation = None
                        for (k, tx_amount) in activate_in_future[name][winning_including_future_activations]:
                            if (k.tx_num, k.position) == (tx_num, position):
                                activation = k.height
                                break
                        if activation is None:
                            # TODO: reproduce this in an integration test (block 604718)
                            _k = PendingActivationValue(winning_including_future_activations, name)
                            if _k in activated_at_height:
                                for pending_activation in activated_at_height[_k]:
                                    if (pending_activation.tx_num, pending_activation.position) == (tx_num, position):
                                        activation = pending_activation.height
                                        break
                        assert None not in (amount, activation)
                    # update the claim that's activating early
                    self.get_remove_activate_ops(
                        ACTIVATED_CLAIM_TXO_TYPE, winning_including_future_activations, tx_num,
                        position, activation, name, amount
                    )
                    self.get_activate_ops(
                        ACTIVATED_CLAIM_TXO_TYPE, winning_including_future_activations, tx_num,
                        position, height, name, amount
                    )
                    self.effective_amount_delta[winning_including_future_activations] += amount
                    for (k, future_amount) in activate_in_future[name][winning_including_future_activations]:
                        txo = (k.tx_num, k.position)
                        if txo in self.possible_future_support_txos_by_claim_hash[winning_including_future_activations]:
                            self.get_remove_activate_ops(
                                ACTIVATED_SUPPORT_TXO_TYPE, winning_including_future_activations, k.tx_num,
                                k.position, k.height, name, future_amount
                            )
                            self.get_activate_ops(
                                ACTIVATED_SUPPORT_TXO_TYPE, winning_including_future_activations, k.tx_num,
                                k.position, height, name, future_amount
                            )
                            self.effective_amount_delta[winning_including_future_activations] += future_amount
                            self.active_support_amount_delta[winning_including_future_activations] += future_amount

                    self.taken_over_names.add(name)
                    if controlling:
                        self.db.prefix_db.claim_takeover.stash_delete(
                            (name,), (controlling.claim_hash, controlling.height)
                        )
                    self.db.prefix_db.claim_takeover.stash_put((name,), (winning_including_future_activations, height))
                    self.touched_claim_hashes.add(winning_including_future_activations)
                    if controlling and controlling.claim_hash not in self.abandoned_claims:
                        self.touched_claim_hashes.add(controlling.claim_hash)
                elif not controlling or (winning_claim_hash != controlling.claim_hash and
                                       name in names_with_abandoned_or_updated_controlling_claims) or \
                        ((winning_claim_hash != controlling.claim_hash) and (amounts[winning_claim_hash] > amounts[controlling.claim_hash])):
                    if winning_claim_hash in headless_supported_claims:
                        # print(f"\tclaim that would be winning {winning_claim_hash.hex()} at {height} does not exist, no takeover")
                        pass
                    else:
                        # print(f"\ttakeover by {winning_claim_hash.hex()} at {height}")
                        if (name, winning_claim_hash) in need_reactivate_if_takes_over:
                            previous_pending_activate = need_reactivate_if_takes_over[(name, winning_claim_hash)]
                            amount = self.db.get_claim_txo_amount(
                                winning_claim_hash
                            )
                            if winning_claim_hash in self.claim_hash_to_txo:
                                tx_num, position = self.claim_hash_to_txo[winning_claim_hash]
                                amount = self.txo_to_claim[(tx_num, position)].amount
                            else:
                                tx_num, position = previous_pending_activate.tx_num, previous_pending_activate.position
                            if previous_pending_activate.height > height:
                                # the claim had a pending activation in the future, move it to now
                                if tx_num < self.tx_count:
                                    self.get_remove_activate_ops(
                                        ACTIVATED_CLAIM_TXO_TYPE, winning_claim_hash, tx_num,
                                        position, previous_pending_activate.height, name, amount
                                    )
                                pending_activated = self.db.prefix_db.activated.get_pending(
                                    ACTIVATED_CLAIM_TXO_TYPE, tx_num, position
                                )
                                if pending_activated:
                                    self.get_remove_activate_ops(
                                        ACTIVATED_CLAIM_TXO_TYPE, winning_claim_hash, tx_num, position,
                                        pending_activated.height, name, amount
                                    )
                                self.get_activate_ops(
                                    ACTIVATED_CLAIM_TXO_TYPE, winning_claim_hash, tx_num,
                                    position, height, name, amount
                                )
                                if winning_claim_hash not in activated_added_to_effective_amount:
                                    self.effective_amount_delta[winning_claim_hash] += amount
                        self.taken_over_names.add(name)
                        if controlling:
                            self.db.prefix_db.claim_takeover.stash_delete(
                                (name,), (controlling.claim_hash, controlling.height)
                            )
                        self.db.prefix_db.claim_takeover.stash_put((name,), (winning_claim_hash, height))
                        if controlling and controlling.claim_hash not in self.abandoned_claims:
                            self.touched_claim_hashes.add(controlling.claim_hash)
                        self.touched_claim_hashes.add(winning_claim_hash)
                elif winning_claim_hash == controlling.claim_hash:
                    # print("\tstill winning")
                    pass
                else:
                    # print("\tno takeover")
                    pass

        # handle remaining takeovers from abandoned supports
        for (name, claim_hash), amounts in abandoned_support_check_need_takeover.items():
            if name in checked_names:
                continue
            checked_names.add(name)
            controlling = get_controlling(name)
            needed_amounts = [
                claim_hash for claim_hash in self.db.get_claims_for_name(name)
                if claim_hash not in self.abandoned_claims
            ]
            if controlling and controlling.claim_hash not in self.abandoned_claims:
                needed_amounts.append(controlling.claim_hash)
            amounts = self._get_pending_effective_amounts([(name, claim_hash) for claim_hash in needed_amounts])
            winning = max(amounts, key=lambda x: amounts[x])

            if (controlling and winning != controlling.claim_hash) or (not controlling and winning):
                self.taken_over_names.add(name)
                # print(f"\ttakeover from abandoned support {controlling.claim_hash.hex()} -> {winning.hex()}")
                if controlling:
                    self.db.prefix_db.claim_takeover.stash_delete(
                        (name,), (controlling.claim_hash, controlling.height)
                    )
                self.db.prefix_db.claim_takeover.stash_put((name,), (winning, height))
                if controlling:
                    self.touched_claim_hashes.add(controlling.claim_hash)
                self.touched_claim_hashes.add(winning)

    def _get_cumulative_update_ops(self, height: int):
        # update the last takeover height for names with takeovers
        for name in self.taken_over_names:
            self.touched_claim_hashes.update(
                {claim_hash for claim_hash in self.db.get_claims_for_name(name)
                 if claim_hash not in self.abandoned_claims}
            )

        # update the reposted counts
        reposted_to_check = [(claim_hash,) for claim_hash, delta in self.reposted_count_delta.items() if delta != 0]
        existing_repost_counts = {}
        if reposted_to_check:
            existing_repost_counts.update({
                claim_hash: v.reposted_count
                for (claim_hash,), v in zip(reposted_to_check, self.db.prefix_db.reposted_count.multi_get(
                    reposted_to_check
                )) if v is not None
            })
        if existing_repost_counts:
            self.db.prefix_db.multi_delete([
                self.db.prefix_db.reposted_count.pack_item(claim_hash, count)
                for claim_hash, count in existing_repost_counts.items()
            ])
        repost_count_puts = []
        for claim_hash, delta in self.reposted_count_delta.items():
            if delta != 0:
                new_count = existing_repost_counts.get(claim_hash, 0) + delta
                repost_count_puts.append(((claim_hash,), (new_count,)))
        self.db.prefix_db.reposted_count.stash_multi_put(repost_count_puts)

        # gather cumulative removed/touched sets to update the search index
        self.removed_claim_hashes.update(set(self.abandoned_claims.keys()))
        self.touched_claim_hashes.difference_update(self.removed_claim_hashes)
        self.touched_claim_hashes.update(
            set(
                map(lambda item: item[1], self.activated_claim_amount_by_name_and_hash.keys())
            ).union(
                set(self.claim_hash_to_txo.keys())
            ).union(
                self.removed_active_support_amount_by_claim.keys()
            ).union(
                self.signatures_changed
            ).union(
                set(self.removed_active_support_amount_by_claim.keys())
            ).union(
                set(self.activated_support_amount_by_claim.keys())
            ).union(
                set(self.pending_support_amount_change.keys())
            ).difference(
                self.removed_claim_hashes
            )
        )

        # update support amount totals
        for supported_claim, amount in self.pending_support_amount_change.items():
            existing = self.db.prefix_db.support_amount.get(supported_claim)
            total = amount
            if existing is not None:
                total += existing.amount
                self.db.prefix_db.support_amount.stash_delete((supported_claim,), existing)
            self.db.prefix_db.support_amount.stash_put((supported_claim,), (total,))

        # use the cumulative changes to update bid ordered resolve
        #
        # first remove bid orders for removed claims
        for removed in self.removed_claim_hashes:
            removed_claim = self.db.get_claim_txo(removed)
            if removed_claim:
                amt = self.db.get_url_effective_amount(
                    removed_claim.normalized_name, removed
                )
                if amt:
                    self.db.prefix_db.bid_order.stash_delete(
                        (removed_claim.normalized_name, amt.effective_amount, amt.tx_num, amt.position), (removed,)
                    )
        # update or insert new bid orders and prepare to update the effective amount index
        touched_claim_hashes = list(self.touched_claim_hashes)
        touched_claims = {
            claim_hash: v if v is not None else self.txo_to_claim[self.claim_hash_to_txo[claim_hash]]
                        if claim_hash in self.claim_hash_to_txo else None
            for claim_hash, v in zip(
                touched_claim_hashes,
                self.db.prefix_db.claim_to_txo.multi_get(
                    [(claim_hash,) for claim_hash in touched_claim_hashes]
                )
            )
        }
        needed_effective_amounts = [
            (v.normalized_name, claim_hash) for claim_hash, v in touched_claims.items() if v is not None
        ]
        touched_effective_amounts = self._get_pending_effective_amounts(
            needed_effective_amounts
        )

        for touched in self.touched_claim_hashes:
            prev_effective_amount = 0

            if touched in self.claim_hash_to_txo:
                pending = self.txo_to_claim[self.claim_hash_to_txo[touched]]
                name, tx_num, position = pending.normalized_name, pending.tx_num, pending.position
                claim_from_db = touched_claims[touched]
                if claim_from_db:
                    claim_amount_info = self.db.get_url_effective_amount(name, touched)
                    if claim_amount_info:
                        prev_effective_amount = claim_amount_info.effective_amount
                        self.db.prefix_db.bid_order.stash_delete(
                            (name, claim_amount_info.effective_amount, claim_amount_info.tx_num,
                             claim_amount_info.position), (touched,)
                        )
            else:
                v = touched_claims[touched]
                if not v:
                    continue
                name, tx_num, position = v.normalized_name, v.tx_num, v.position
                amt = self.db.get_url_effective_amount(name, touched)
                if amt:
                    prev_effective_amount = amt.effective_amount
                    self.db.prefix_db.bid_order.stash_delete(
                        (name, prev_effective_amount, amt.tx_num, amt.position), (touched,)
                    )

            new_effective_amount = touched_effective_amounts.get(touched, 0)
            self.db.prefix_db.bid_order.stash_put(
                (name, new_effective_amount, tx_num, position), (touched,)
            )
            if touched in self.claim_hash_to_txo or touched in self.removed_claim_hashes \
                    or touched in self.pending_support_amount_change:
                # exclude sending notifications for claims/supports that activated but
                # weren't added/spent in this block
                self.db.prefix_db.trending_notification.stash_put(
                    (height, touched), (prev_effective_amount, new_effective_amount)
                )

        # update the effective amount index
        current_effective_amount_values = {
            claim_hash: v for claim_hash, v in zip(
                self.effective_amount_delta,
                self.db.prefix_db.effective_amount.multi_get(
                    [(claim_hash,) for claim_hash in self.effective_amount_delta]
                )
            )
        }
        current_effective_amounts = {
            claim_hash: 0 if not v else v.activated_sum
            for claim_hash, v in current_effective_amount_values.items()
        }
        current_supports_amount = {
            claim_hash: 0 if not v else v.activated_support_sum
            for claim_hash, v in current_effective_amount_values.items()
        }
        delete_effective_amounts = [
            self.db.prefix_db.effective_amount.pack_item(claim_hash, v.activated_sum, v.activated_support_sum)
            for claim_hash, v in current_effective_amount_values.items() if v is not None
        ]
        claims = set(self.effective_amount_delta.keys()).union(self.active_support_amount_delta.keys())
        new_effective_amounts = {
            claim_hash: ((current_effective_amounts.get(claim_hash, 0) or 0) + self.effective_amount_delta.get(claim_hash, 0),
                         (current_supports_amount.get(claim_hash, 0) or 0) + self.active_support_amount_delta.get(claim_hash, 0))
            for claim_hash in claims
        }
        if delete_effective_amounts:
            self.db.prefix_db.multi_delete(delete_effective_amounts)
        if new_effective_amounts:
            self.db.prefix_db.effective_amount.stash_multi_put(
                [((claim_hash,), (amount, support_sum)) for claim_hash, (amount, support_sum) in new_effective_amounts.items()]
            )

        # update the future effective amount index
        current_future_effective_amount_values = {
            claim_hash: None if not v else v.future_effective_amount for claim_hash, v in zip(
                self.future_effective_amount_delta,
                self.db.prefix_db.future_effective_amount.multi_get(
                    [(claim_hash,) for claim_hash in self.future_effective_amount_delta]
                )
            )
        }
        self.db.prefix_db.future_effective_amount.stash_multi_delete([
            ((claim_hash,), (current_amount,))
            for claim_hash, current_amount in current_future_effective_amount_values.items()
            if current_amount is not None
        ])

        self.db.prefix_db.future_effective_amount.stash_multi_put([
            ((claim_hash,), ((current_future_effective_amount_values[claim_hash] or 0) + delta,))
            for claim_hash, delta in self.future_effective_amount_delta.items()
        ])

        # update or insert channel counts
        for channel_hash, count in self.pending_channel_counts.items():
            if count != 0:
                channel_count_val = self.db.prefix_db.channel_count.get(channel_hash)
                channel_count = 0 if not channel_count_val else channel_count_val.count
                if channel_count_val is not None:
                    self.db.prefix_db.channel_count.stash_delete((channel_hash,), (channel_count,))
                self.db.prefix_db.channel_count.stash_put((channel_hash,), (channel_count + count,))

        # update the sets of touched and removed claims for es sync
        self.touched_claim_hashes.update(
            {k for k in self.pending_reposted if k not in self.removed_claim_hashes}
        )
        self.touched_claim_hashes.update(
            {k for k, v in self.pending_channel_counts.items() if v != 0 and k not in self.removed_claim_hashes}
        )
        self.touched_claims_to_send_es.update(self.touched_claim_hashes)
        self.touched_claims_to_send_es.difference_update(self.removed_claim_hashes)
        self.removed_claims_to_send_es.update(self.removed_claim_hashes)

    def _get_cached_hashX_history(self, hashX: bytes) -> str:
        if hashX in self.hashX_full_cache:
            return self.hashX_full_cache[hashX]
        if hashX not in self.hashX_history_cache:
            self.hashX_history_cache[hashX] = tx_nums = self.db._read_history(hashX, limit=None)
        else:
            tx_nums = self.hashX_history_cache[hashX]
        needed_tx_infos = []
        append_needed_tx_info = needed_tx_infos.append
        tx_infos = {}
        for tx_num in tx_nums:
            cached_tx_info = self.history_tx_info_cache.get(tx_num)
            if cached_tx_info is not None:
                tx_infos[tx_num] = cached_tx_info
            else:
                append_needed_tx_info(tx_num)
        if needed_tx_infos:
            for tx_num, tx_hash in zip(needed_tx_infos, self.db._get_tx_hashes(needed_tx_infos)):
                tx_infos[tx_num] = self.history_tx_info_cache[tx_num] = f'{tx_hash[::-1].hex()}:{bisect_right(self.db.tx_counts, tx_num):d}:'

        history = ''
        for tx_num in tx_nums:
            history += tx_infos[tx_num]
        self.hashX_full_cache[hashX] = history
        return history

    def _get_update_hashX_mempool_status_ops(self, hashX: bytes):
        existing = self.db.prefix_db.hashX_mempool_status.get(hashX)
        if existing:
            self.db.prefix_db.hashX_mempool_status.stash_delete((hashX,), existing)
        history = self._get_cached_hashX_history(hashX) + self.mempool.mempool_history(hashX)
        if history:
            status = sha256(history.encode())
            self.db.prefix_db.hashX_mempool_status.stash_put((hashX,), (status,))

    def advance_block(self, block: Block):
        txo_count = 0
        txi_count = 0
        claim_added_count = 0
        support_added_count = 0
        claim_spent_count = 0
        support_spent_count = 0
        abandoned_cnt = 0
        abandoned_chans_cnt = 0
        height = self.height + 1
        # print("advance ", height)
        # Use local vars for speed in the loops
        tx_count = self.tx_count
        spend_utxos = self.spend_utxos
        add_utxo = self.add_utxo
        add_claim_or_support = self._add_claim_or_support
        # spend_claim_or_support = self._spend_claim_or_support_txo
        txs: List[Tuple[Tx, bytes]] = block.transactions

        self.db.prefix_db.block_hash.stash_put(key_args=(height,), value_args=(self.coin.header_hash(block.header),))
        self.db.prefix_db.header.stash_put(key_args=(height,), value_args=(block.header,))
        self.db.prefix_db.block_txs.stash_put(key_args=(height,), value_args=([tx_hash for tx, tx_hash in txs],))

        for tx, tx_hash in txs:
            spent_claims = {}
            # clean up mempool, delete txs that were already in mempool/staged to be added
            # leave txs in mempool that werent in the block
            mempool_tx = self.db.prefix_db.mempool_tx.get_pending(tx_hash)
            if mempool_tx:
                self.db.prefix_db.mempool_tx.stash_delete((tx_hash,), mempool_tx)

            self.db.prefix_db.tx.stash_put(key_args=(tx_hash,), value_args=(tx.raw,))
            self.db.prefix_db.tx_num.stash_put(key_args=(tx_hash,), value_args=(tx_count,))
            self.db.prefix_db.tx_hash.stash_put(key_args=(tx_count,), value_args=(tx_hash,))

            spent_txos = []
            append_spent_txo = spent_txos.append

            # Spend the inputs
            txi_count += len(tx.inputs)
            for txin in tx.inputs:
                if txin.is_generation():
                    continue
                append_spent_txo((txin.prev_hash, txin.prev_idx))
            spent_claims_cnt, spent_supports_cnt = self._spend_claims_and_supports(tx.inputs, spent_claims)
            claim_spent_count += spent_claims_cnt
            support_spent_count += spent_supports_cnt
            spend_utxos(tx_count, spent_txos)

            # Add the new UTXOs
            txo_count += len(tx.outputs)
            for nout, txout in enumerate(tx.outputs):
                # Get the hashX.  Ignore unspendable outputs
                hashX = add_utxo(tx_hash, tx_count, nout, txout)
                if hashX:
                    # self._set_hashX_cache(hashX)
                    if tx_count not in self.hashXs_by_tx[hashX]:
                        self.hashXs_by_tx[hashX].append(tx_count)
                # add claim/support txo
                added_claim_or_support = add_claim_or_support(
                    height, tx_hash, tx_count, nout, txout, spent_claims, tx.inputs[0]
                )
                if added_claim_or_support == 1:
                    claim_added_count += 1
                elif added_claim_or_support == 2:
                    support_added_count += 1

            # Handle abandoned claims
            abandoned_channels = {}
            # abandon the channels last to handle abandoned signed claims in the same tx,
            # see test_abandon_channel_and_claims_in_same_tx
            for abandoned_claim_hash, (tx_num, nout, normalized_name) in spent_claims.items():
                if normalized_name.startswith('@'):
                    abandoned_chans_cnt += 1
                    abandoned_channels[abandoned_claim_hash] = (tx_num, nout, normalized_name)
                else:
                    abandoned_cnt += 1
                    # print(f"\tabandon {normalized_name} {abandoned_claim_hash.hex()} {tx_num} {nout}")
                    self._abandon_claim(abandoned_claim_hash, tx_num, nout, normalized_name)

            for abandoned_claim_hash, (tx_num, nout, normalized_name) in abandoned_channels.items():
                # print(f"\tabandon {normalized_name} {abandoned_claim_hash.hex()} {tx_num} {nout}")
                self._abandon_claim(abandoned_claim_hash, tx_num, nout, normalized_name)
            self.pending_transactions[tx_count] = tx_hash
            self.pending_transaction_num_mapping[tx_hash] = tx_count
            if self.env.cache_all_tx_hashes:
                self.db.total_transactions.append(tx_hash)
                self.db.tx_num_mapping[tx_hash] = tx_count
            tx_count += 1

        # handle expired claims
        self._expire_claims(height)

        # activate claims and process takeovers
        self._get_takeover_ops(height)

        # update effective amount and update sets of touched and deleted claims
        self._get_cumulative_update_ops(height)

        self.db.prefix_db.touched_hashX.stash_put((height,), (list(sorted(self.touched_hashXs)),))

        self.db.prefix_db.tx_count.stash_put(key_args=(height,), value_args=(tx_count,))

        # clear the mempool tx index
        self._get_clear_mempool_ops()

        # update hashX history status hashes and compactify the histories
        self._get_update_hashX_histories_ops(height)

        # only compactify adddress histories and update the status index if we're already caught up,
        # a bulk update will happen once catchup finishes
        if not self.db.catching_up and self.env.index_address_status:
            self._get_compactify_ops(height)
            self.db.last_indexed_address_status_height = height

        self.tx_count = tx_count
        self.db.tx_counts.append(self.tx_count)

        cached_max_reorg_depth = self.daemon.cached_height() - self.env.reorg_limit

        # if height >= cached_max_reorg_depth:
        self.db.prefix_db.touched_or_deleted.stash_put(
            key_args=(height,), value_args=(self.touched_claim_hashes, self.removed_claim_hashes)
        )

        self.height = height
        self.db.block_hashes.append(self.env.coin.header_hash(block.header))
        self.tip = self.coin.header_hash(block.header)

        self.db.fs_tx_count = self.tx_count
        self.db.hist_flush_count += 1
        self.db.hist_unflushed_count = 0
        self.db.utxo_flush_count = self.db.hist_flush_count
        self.db.db_height = self.height
        self.db.db_tx_count = self.tx_count
        self.db.db_tip = self.tip
        self.db.last_flush_tx_count = self.db.fs_tx_count
        now = time.time()
        self.db.wall_time += now - self.db.last_flush
        self.db.last_flush = now
        self.db.write_db_state()

        # flush the changes
        save_undo = (self.daemon.cached_height() - self.height) <= self.env.reorg_limit

        if save_undo:
            self.db.prefix_db.commit(self.height, self.tip)
        else:
            self.db.prefix_db.unsafe_commit()
        self.clear_after_advance_or_reorg()
        self.db.assert_db_state()
        # print("*************\n")
        return txi_count, txo_count, claim_added_count, claim_spent_count, support_added_count, support_spent_count, abandoned_cnt, abandoned_chans_cnt

    def _get_clear_mempool_ops(self):
        self.db.prefix_db.multi_delete(
            list(self.db.prefix_db.hashX_mempool_status.iterate(start=(b'\x00' * 20, ), stop=(b'\xff' * 20, ),
                                                                deserialize_key=False, deserialize_value=False))
        )

    def _get_update_hashX_histories_ops(self, height: int):
        self.db.prefix_db.hashX_history.stash_multi_put(
            [((hashX, height), (new_tx_nums,)) for hashX, new_tx_nums in self.hashXs_by_tx.items()]
        )

    def _get_compactify_ops(self, height: int):
        existing_hashX_statuses = self.db.prefix_db.hashX_status.multi_get([(hashX,) for hashX in self.hashXs_by_tx.keys()], deserialize_value=False)
        if existing_hashX_statuses:
            pack_key = self.db.prefix_db.hashX_status.pack_key
            keys = [
                pack_key(hashX) for hashX, existing in zip(
                    self.hashXs_by_tx, existing_hashX_statuses
                )
            ]
            self.db.prefix_db.multi_delete([(k, v) for k, v in zip(keys, existing_hashX_statuses) if v is not None])

        block_hashX_history_deletes = []
        append_deletes_hashX_history = block_hashX_history_deletes.append
        block_hashX_history_puts = []

        for (hashX, new_tx_nums), existing in zip(self.hashXs_by_tx.items(), existing_hashX_statuses):
            new_history = [(self.pending_transactions[tx_num], height) for tx_num in new_tx_nums]

            tx_nums = []
            txs_extend = tx_nums.extend
            compact_hist_txs = []
            compact_txs_extend = compact_hist_txs.extend
            history_item_0 = None
            existing_item_0 = None
            reorg_limit = self.env.reorg_limit
            unpack_history = self.db.prefix_db.hashX_history.unpack_value
            unpack_key = self.db.prefix_db.hashX_history.unpack_key
            needs_compaction = False

            total_hist_txs = b''
            for k, hist in self.db.prefix_db.hashX_history.iterate(prefix=(hashX,), deserialize_key=False,
                                                                   deserialize_value=False):
                hist_txs = unpack_history(hist)
                total_hist_txs += hist
                txs_extend(hist_txs)
                hist_height = unpack_key(k).height
                if height > reorg_limit and hist_height < height - reorg_limit:
                    compact_txs_extend(hist_txs)
                    if hist_height == 0:
                        history_item_0 = (k, hist)
                    elif hist_height > 0:
                        needs_compaction = True
                        append_deletes_hashX_history((k, hist))
                        existing_item_0 = history_item_0
            if needs_compaction:
                # add the accumulated histories onto the existing compacted history at height 0
                if existing_item_0 is not None:  # delete if key 0 exists
                    key, existing = existing_item_0
                    append_deletes_hashX_history((key, existing))
                block_hashX_history_puts.append(((hashX, 0), (compact_hist_txs,)))
            if not new_history:
                continue

            needed_tx_infos = []
            append_needed_tx_info = needed_tx_infos.append
            tx_infos = {}
            for tx_num in tx_nums:
                cached_tx_info = self.history_tx_info_cache.get(tx_num)
                if cached_tx_info is not None:
                    tx_infos[tx_num] = cached_tx_info
                else:
                    append_needed_tx_info(tx_num)
            if needed_tx_infos:
                for tx_num, tx_hash in zip(needed_tx_infos, self.db._get_tx_hashes(needed_tx_infos)):
                    tx_info = f'{tx_hash[::-1].hex()}:{bisect_right(self.db.tx_counts, tx_num):d}:'
                    tx_infos[tx_num] = tx_info
                    self.history_tx_info_cache[tx_num] = tx_info
            history = ''.join(map(tx_infos.__getitem__, tx_nums))
            for tx_hash, height in new_history:
                history += f'{tx_hash[::-1].hex()}:{height:d}:'
            if history:
                status = sha256(history.encode())
                self.db.prefix_db.hashX_status.stash_put((hashX,), (status,))

        self.db.prefix_db.multi_delete(block_hashX_history_deletes)
        self.db.prefix_db.hashX_history.stash_multi_put(block_hashX_history_puts)

    def clear_after_advance_or_reorg(self):
        self.txo_to_claim.clear()
        self.claim_hash_to_txo.clear()
        self.support_txos_by_claim.clear()
        self.support_txo_to_claim.clear()
        self.abandoned_claims.clear()
        self.removed_active_support_amount_by_claim.clear()
        self.activated_support_amount_by_claim.clear()
        self.activated_claim_amount_by_name_and_hash.clear()
        self.activation_by_claim_by_name.clear()
        self.possible_future_claim_amount_by_name_and_hash.clear()
        self.possible_future_support_amounts_by_claim_hash.clear()
        self.possible_future_support_txos_by_claim_hash.clear()
        self.pending_channels.clear()
        self.amount_cache.clear()
        self.signatures_changed.clear()
        self.expired_claim_hashes.clear()
        self.doesnt_have_valid_signature.clear()
        self.claim_channels.clear()
        self.utxo_cache.clear()
        self.hashXs_by_tx.clear()
        self.removed_claim_hashes.clear()
        self.touched_claim_hashes.clear()
        self.pending_reposted.clear()
        self.pending_channel_counts.clear()
        self.updated_claims.clear()
        self.taken_over_names.clear()
        self.pending_transaction_num_mapping.clear()
        self.pending_transactions.clear()
        self.pending_support_amount_change.clear()
        self.touched_hashXs.clear()
        self.mempool.clear()
        self.hashX_history_cache.clear()
        self.hashX_full_cache.clear()
        self.reposted_count_delta.clear()
        self.effective_amount_delta.clear()
        self.active_support_amount_delta.clear()
        self.future_effective_amount_delta.clear()
        self.spent_utxos.clear()

    def backup_block(self):
        assert len(self.db.prefix_db._op_stack) == 0
        touched_and_deleted = self.db.prefix_db.touched_or_deleted.get(self.height)
        self.touched_claims_to_send_es.update(touched_and_deleted.touched_claims)
        self.removed_claims_to_send_es.difference_update(touched_and_deleted.touched_claims)
        self.removed_claims_to_send_es.update(touched_and_deleted.deleted_claims)

        # self.db.assert_flushed(self.flush_data())
        self.log.info("backup block %i", self.height)
        # Check and update self.tip

        self.db.tx_counts.pop()
        reverted_block_hash = self.db.block_hashes.pop()
        self.tip = self.db.block_hashes[-1]
        if self.env.cache_all_tx_hashes:
            while len(self.db.total_transactions) > self.db.tx_counts[-1]:
                self.db.tx_num_mapping.pop(self.db.total_transactions.pop())
                if self.tx_count in self.history_tx_info_cache:
                    self.history_tx_info_cache.pop(self.tx_count)
                self.tx_count -= 1
        else:
            new_tx_count = self.db.tx_counts[-1]
            while self.tx_count > new_tx_count:
                if self.tx_count in self.history_tx_info_cache:
                    self.history_tx_info_cache.pop(self.tx_count)
                self.tx_count -= 1
        self.height -= 1

        # self.touched can include other addresses which is
        # harmless, but remove None.
        self.touched_hashXs.discard(None)

        assert self.height < self.db.db_height
        assert not self.db.hist_unflushed

        start_time = time.time()
        tx_delta = self.tx_count - self.db.last_flush_tx_count
        ###
        self.db.fs_tx_count = self.tx_count
        # Truncate header_mc: header count is 1 more than the height.
        self.db.header_mc.truncate(self.height + 1)
        ###
        # Not certain this is needed, but it doesn't hurt
        self.db.hist_flush_count += 1

        while self.db.db_height > self.height:
            self.db.db_height -= 1
        self.db.utxo_flush_count = self.db.hist_flush_count
        self.db.db_tx_count = self.tx_count
        self.db.db_tip = self.tip
        # Flush state last as it reads the wall time.
        now = time.time()
        self.db.wall_time += now - self.db.last_flush
        self.db.last_flush = now
        self.db.last_flush_tx_count = self.db.fs_tx_count

        # rollback
        self.db.prefix_db.rollback(self.height + 1, reverted_block_hash)
        self.db.es_sync_height = self.height
        self.db.write_db_state()
        self.db.prefix_db.unsafe_commit()

        self.clear_after_advance_or_reorg()
        self.db.assert_db_state()

        elapsed = self.db.last_flush - start_time
        self.log.warning(f'backup flush #{self.db.hist_flush_count:,d} took {elapsed:.1f}s. '
                            f'Height {self.height:,d} txs: {self.tx_count:,d} ({tx_delta:+,d})')

    def add_utxo(self, tx_hash: bytes, tx_num: int, nout: int, txout: 'TxOutput') -> Optional[bytes]:
        hashX = self.coin.hashX_from_txo(txout)
        if hashX:
            self.touched_hashXs.add(hashX)
            self.utxo_cache[(tx_hash, nout)] = (hashX, txout.value)
            self.db.prefix_db.utxo.stash_put((hashX, tx_num, nout), (txout.value,))
            self.db.prefix_db.hashX_utxo.stash_put((tx_hash[:4], tx_num, nout), (hashX,))
            return hashX

    def get_pending_tx_num(self, tx_hash: bytes) -> int:
        if tx_hash in self.pending_transaction_num_mapping:
            return self.pending_transaction_num_mapping[tx_hash]
        else:
            return self.db.get_tx_num(tx_hash)

    def spend_utxos(self, tx_count: int, txis: List[Tuple[bytes, int]]):
        tx_nums = self.db.get_tx_nums(
            list(
                {tx_hash for tx_hash, nout in txis if tx_hash not in self.pending_transaction_num_mapping}
            )
        )
        txo_hashXs = {}
        hashX_utxos_needed = {}
        utxos_needed = {}

        for tx_hash, nout in txis:
            if tx_hash in self.pending_transaction_num_mapping:
                txin_num = self.pending_transaction_num_mapping[tx_hash]
            else:
                txin_num = tx_nums[tx_hash]
            self.spent_utxos.add((txin_num, nout))
            hashX, amount = self.utxo_cache.pop((tx_hash, nout), (None, None))
            txo_hashXs[(tx_hash, nout)] = (hashX, amount, txin_num)
            hashX_utxos_needed[(tx_hash[:4], txin_num, nout)] = tx_hash, nout
            utxos_needed[(hashX, txin_num, nout)] = tx_hash, nout
        hashX_utxos = {
            (tx_hash, nout): v for (tx_hash, nout), v in zip(
                hashX_utxos_needed.values(), self.db.prefix_db.hashX_utxo.multi_get(list(hashX_utxos_needed.keys()))
            ) if v is not None
        }

        for (tx_hash, nout), v in hashX_utxos.items():
            if tx_hash in self.pending_transaction_num_mapping:
                txin_num = self.pending_transaction_num_mapping[tx_hash]
            else:
                txin_num = tx_nums[tx_hash]
            utxos_needed[(v.hashX, txin_num, nout)] = tx_hash, nout
        utxos_needed = {
            (hashX, txin_num, nout): v
            for (hashX, txin_num, nout), v in utxos_needed.items() if hashX is not None
        }
        utxos = {
            (tx_hash, nout): v for (tx_hash, nout), v in zip(
                utxos_needed.values(), self.db.prefix_db.utxo.multi_get(list(utxos_needed.keys()))
            )
        }
        for (tx_hash, nout), (hashX, amount, txin_num) in txo_hashXs.items():
            if not hashX:
                hashX_value = hashX_utxos.get((tx_hash[:4], txin_num, nout))
                if not hashX_value:
                    continue
                hashX = hashX_value.hashX
                utxo_value = utxos.get((hashX, txin_num, nout))
                if not utxo_value:
                    self.log.warning(
                        "%s:%s is not found in UTXO db for %s", hash_to_hex_str(tx_hash), nout, hash_to_hex_str(hashX)
                    )
                    raise ChainError(
                        f"{hash_to_hex_str(tx_hash)}:{nout} is not found in UTXO db for {hash_to_hex_str(hashX)}"
                    )
                self.touched_hashXs.add(hashX)
                self.db.prefix_db.hashX_utxo.stash_delete((tx_hash[:4], txin_num, nout), hashX_value)
                self.db.prefix_db.utxo.stash_delete((hashX, txin_num, nout), utxo_value)
                if tx_count not in self.hashXs_by_tx[hashX]:
                    self.hashXs_by_tx[hashX].append(tx_count)
            elif amount is not None:
                self.db.prefix_db.hashX_utxo.stash_delete((tx_hash[:4], txin_num, nout), (hashX,))
                self.db.prefix_db.utxo.stash_delete((hashX, txin_num, nout), (amount,))
                self.touched_hashXs.add(hashX)
                if tx_count not in self.hashXs_by_tx[hashX]:
                    self.hashXs_by_tx[hashX].append(tx_count)

    async def process_blocks_and_mempool_forever(self, caught_up_event):
        """Loop forever processing blocks as they arrive."""
        self._caught_up_event = caught_up_event
        try:
            if self.height != self.daemon.cached_height() and not self.db.catching_up:
                await self._need_catch_up()  # tell the readers that we're still catching up with lbrycrd/lbcd
            while not self._stopping:
                if self.height == self.daemon.cached_height():
                    if not self._caught_up_event.is_set():
                        await self._finished_initial_catch_up()
                        self._caught_up_event.set()
                try:
                    await asyncio.wait_for(self.blocks_event.wait(), self.wait_for_blocks_duration)
                except asyncio.TimeoutError:
                    pass
                self.blocks_event.clear()
                blocks = self.prefetcher.get_prefetched_blocks()
                if self._stopping:
                    break
                if not blocks:
                    try:
                        start_mempool_time = time.perf_counter()
                        await self.refresh_mempool()
                        self.mempool_update_time_metric.observe(time.perf_counter() - start_mempool_time)
                    except asyncio.CancelledError:
                        raise
                    except Exception as err:
                        self.log.exception("error while updating mempool txs: %s", err)
                        raise err
                else:
                    try:
                        await self.check_and_advance_blocks(blocks)
                    except asyncio.CancelledError:
                        raise
                    except Exception as err:
                        self.log.exception("error while processing txs: %s", err)
                        raise err
        except asyncio.CancelledError:
            raise
        except MemoryError:
            self.log.error("out of memory, shutting down")
            self.shutdown_event.set()
        except Exception as err:
            self.log.exception("fatal error in block processor loop: %s", err)
            self.shutdown_event.set()
            raise err
        finally:
            self._ready_to_stop.set()

    async def _need_catch_up(self):
        self.log.info("database has fallen behind blockchain daemon, catching up")

        self.db.catching_up = True

        def flush():
            assert len(self.db.prefix_db._op_stack) == 0
            self.db.write_db_state()
            self.db.prefix_db.unsafe_commit()
            self.db.assert_db_state()

        await self.run_in_thread_with_lock(flush)

    async def _finished_initial_catch_up(self):
        self.log.info(f'caught up to height {self.height}')

        if self.env.index_address_status and self.db.last_indexed_address_status_height < self.db.db_height:
            await self.db.rebuild_hashX_status_index(self.db.last_indexed_address_status_height)

        # Flush everything but with catching_up->False state.
        self.db.catching_up = False

        def flush():
            assert len(self.db.prefix_db._op_stack) == 0
            self.db.write_db_state()
            self.db.prefix_db.unsafe_commit()
            self.db.assert_db_state()

        await self.run_in_thread_with_lock(flush)

    def _iter_start_tasks(self):
        self.block_count_metric.set(0 if not self.last_state else self.last_state.height)
        yield self.start_prometheus()
        while self.db.db_version < max(self.db.DB_VERSIONS):
            if self.db.db_version == 7:
                from hub.db.migrators.migrate7to8 import migrate, FROM_VERSION, TO_VERSION
            elif self.db.db_version == 8:
                from hub.db.migrators.migrate8to9 import migrate, FROM_VERSION, TO_VERSION
                self.db._index_address_status = self.env.index_address_status
            elif self.db.db_version == 9:
                from hub.db.migrators.migrate9to10 import migrate, FROM_VERSION, TO_VERSION
                self.db._index_address_status = self.env.index_address_status
            elif self.db.db_version == 10:
                from hub.db.migrators.migrate10to11 import migrate, FROM_VERSION, TO_VERSION
                self.db._index_address_status = self.env.index_address_status
            elif self.db.db_version == 11:
                from hub.db.migrators.migrate11to12 import migrate, FROM_VERSION, TO_VERSION
                self.db._index_address_status = self.env.index_address_status
            else:
                raise RuntimeError("unknown db version")
            self.log.warning(f"migrating database from version {FROM_VERSION} to version {TO_VERSION}")
            migrate(self.db)
            self.log.info("finished migration")
            self.db.read_db_state()

        # update the hashX status index if was off before and is now on of if requested from a height
        if (self.env.index_address_status and not self.db._index_address_status and self.db.last_indexed_address_status_height < self.db.db_height) or self.env.rebuild_address_status_from_height >= 0:
            starting_height = self.db.last_indexed_address_status_height
            if self.env.rebuild_address_status_from_height >= 0:
                starting_height = self.env.rebuild_address_status_from_height
            yield self.db.rebuild_hashX_status_index(starting_height)
        elif self.db._index_address_status and not self.env.index_address_status:
            self.log.warning("turned off address indexing at block %i", self.db.db_height)
            self.db._index_address_status = False
            self.db.write_db_state()
            self.db.prefix_db.unsafe_commit()

        self.height = self.db.db_height
        self.tip = self.db.db_tip
        self.tx_count = self.db.db_tx_count
        yield self.daemon.height()
        yield self.start_cancellable(self.prefetcher.main_loop, self.height)
        yield self.start_cancellable(self.process_blocks_and_mempool_forever)

    def _iter_stop_tasks(self):
        yield self._ready_to_stop.wait()
        yield self._stop_cancellable_tasks()
        yield self.daemon.close()
        yield self.stop_prometheus()
