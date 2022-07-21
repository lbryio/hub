import os
import asyncio
import array
import time
import typing
import struct
import zlib
import base64
import logging
from typing import Optional, Iterable, Tuple, DefaultDict, Set, Dict, List, TYPE_CHECKING
from functools import partial
from bisect import bisect_right
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from hub import PROMETHEUS_NAMESPACE
from hub.error import ResolveCensoredError
from hub.schema.url import URL, normalize_name
from hub.schema.claim import guess_stream_type
from hub.schema.result import Censor
from hub.scribe.transaction import TxInput
from hub.common import hash_to_hex_str, LRUCacheWithMetrics, LFUCacheWithMetrics
from hub.db.merkle import Merkle, MerkleCache, FastMerkleCacheItem
from hub.db.common import ResolveResult, ExpandedResolveResult, DBError, UTXO
from hub.db.prefixes import PendingActivationValue, ClaimTakeoverValue, ClaimToTXOValue, PrefixDB
from hub.db.prefixes import ACTIVATED_CLAIM_TXO_TYPE, ACTIVATED_SUPPORT_TXO_TYPE, BidOrderKey
from hub.db.prefixes import PendingActivationKey, TXOToClaimValue, DBStatePrefixRow, MempoolTXPrefixRow
from hub.db.prefixes import HashXMempoolStatusPrefixRow


TXO_STRUCT = struct.Struct(b'>LH')
TXO_STRUCT_unpack = TXO_STRUCT.unpack
TXO_STRUCT_pack = TXO_STRUCT.pack
NAMESPACE = f"{PROMETHEUS_NAMESPACE}_db"


class SecondaryDB:
    DB_VERSIONS = [7, 8, 9]

    def __init__(self, coin, db_dir: str, secondary_name: str, max_open_files: int = -1, reorg_limit: int = 200,
                 cache_all_claim_txos: bool = False, cache_all_tx_hashes: bool = False,
                 blocking_channel_ids: List[str] = None,
                 filtering_channel_ids: List[str] = None, executor: ThreadPoolExecutor = None,
                 index_address_status=False, merkle_cache_size=32768, tx_cache_size=32768):
        self.logger = logging.getLogger(__name__)
        self.coin = coin
        self._executor = executor
        self._db_dir = db_dir
        self._reorg_limit = reorg_limit
        self._cache_all_claim_txos = cache_all_claim_txos
        self._cache_all_tx_hashes = cache_all_tx_hashes
        self._secondary_name = secondary_name
        if secondary_name:
            assert max_open_files == -1, 'max open files must be -1 for secondary readers'
        self._db_max_open_files = max_open_files
        self._index_address_status = index_address_status
        self.prefix_db: typing.Optional[PrefixDB] = None

        self.hist_unflushed = defaultdict(partial(array.array, 'I'))
        self.hist_unflushed_count = 0
        self.hist_flush_count = 0
        self.hist_comp_flush_count = -1
        self.hist_comp_cursor = -1

        self.es_sync_height = 0
        self.last_indexed_address_status_height = 0

        # blocking/filtering dicts
        blocking_channels = blocking_channel_ids or []
        filtering_channels = filtering_channel_ids or []
        self.blocked_streams = {}
        self.blocked_channels = {}
        self.blocking_channel_hashes = {
            bytes.fromhex(channel_id) for channel_id in blocking_channels if channel_id
        }
        self.filtered_streams = {}

        self.filtered_channels = {}
        self.filtering_channel_hashes = {
            bytes.fromhex(channel_id) for channel_id in filtering_channels if channel_id
        }

        self.tx_counts = None
        # self.headers = None
        self.block_hashes = None
        self.encoded_headers = LRUCacheWithMetrics(1024, metric_name='encoded_headers', namespace=NAMESPACE)
        self.last_flush = time.time()

        # Header merkle cache
        self.merkle = Merkle()
        self.header_mc = MerkleCache(self.merkle, self.fs_block_hashes)

        # lru cache of tx_hash: (tx_bytes, tx_num, position, tx_height)
        self.tx_cache = LFUCacheWithMetrics(tx_cache_size, metric_name='tx', namespace=NAMESPACE)
        # lru cache of block heights to merkle trees of the block tx hashes
        self.merkle_cache = LFUCacheWithMetrics(merkle_cache_size, metric_name='merkle', namespace=NAMESPACE)

        # these are only used if the cache_all_tx_hashes setting is on
        self.total_transactions: List[bytes] = []
        self.tx_num_mapping: Dict[bytes, int] = {}

        # these are only used if the cache_all_claim_txos setting is on
        self.claim_to_txo: Dict[bytes, ClaimToTXOValue] = {}
        self.txo_to_claim: DefaultDict[int, Dict[int, bytes]] = defaultdict(dict)
        self.genesis_bytes = bytes.fromhex(self.coin.GENESIS_HASH)

    def get_claim_from_txo(self, tx_num: int, tx_idx: int) -> Optional[TXOToClaimValue]:
        claim_hash_and_name = self.prefix_db.txo_to_claim.get(tx_num, tx_idx)
        if not claim_hash_and_name:
            return
        return claim_hash_and_name

    def get_repost(self, claim_hash) -> Optional[bytes]:
        repost = self.prefix_db.repost.get(claim_hash)
        if repost:
            return repost.reposted_claim_hash
        return

    def get_reposted_count(self, claim_hash: bytes) -> int:
        v = self.prefix_db.reposted_count.get(claim_hash)
        if v:
            return v.reposted_count
        return 0

    def get_activation(self, tx_num, position, is_support=False) -> int:
        activation = self.prefix_db.activated.get(
            ACTIVATED_SUPPORT_TXO_TYPE if is_support else ACTIVATED_CLAIM_TXO_TYPE, tx_num, position
        )
        if activation:
            return activation.height
        return -1

    def get_supported_claim_from_txo(self, tx_num: int, position: int) -> typing.Tuple[Optional[bytes], Optional[int]]:
        supported_claim_hash = self.prefix_db.support_to_claim.get(tx_num, position)
        if supported_claim_hash:
            packed_support_amount = self.prefix_db.claim_to_support.get(
                supported_claim_hash.claim_hash, tx_num, position
            )
            if packed_support_amount:
                return supported_claim_hash.claim_hash, packed_support_amount.amount
        return None, None

    def get_support_amount(self, claim_hash: bytes):
        support_amount_val = self.prefix_db.support_amount.get(claim_hash)
        if support_amount_val is None:
            return 0
        return support_amount_val.amount

    def get_supports(self, claim_hash: bytes):
        return [
            (k.tx_num, k.position, v.amount) for k, v in self.prefix_db.claim_to_support.iterate(prefix=(claim_hash,))
        ]

    def get_short_claim_id_url(self, name: str, normalized_name: str, claim_hash: bytes,
                               root_tx_num: int, root_position: int) -> str:
        claim_id = claim_hash.hex()
        for prefix_len in range(10):
            for k in self.prefix_db.claim_short_id.iterate(prefix=(normalized_name, claim_id[:prefix_len+1]),
                                                           include_value=False):
                if k.root_tx_num == root_tx_num and k.root_position == root_position:
                    return f'{name}#{k.partial_claim_id}'
                break
        print(f"{claim_id} has a collision")
        return f'{name}#{claim_id}'

    def _prepare_resolve_result(self, tx_num: int, position: int, claim_hash: bytes, name: str,
                                root_tx_num: int, root_position: int, activation_height: int,
                                signature_valid: bool) -> ResolveResult:
        try:
            normalized_name = normalize_name(name)
        except UnicodeDecodeError:
            normalized_name = name
        controlling_claim = self.get_controlling_claim(normalized_name)

        tx_hash = self.get_tx_hash(tx_num)
        height = bisect_right(self.tx_counts, tx_num)
        created_height = bisect_right(self.tx_counts, root_tx_num)
        last_take_over_height = controlling_claim.height
        expiration_height = self.coin.get_expiration_height(height)
        support_amount = self.get_support_amount(claim_hash)
        claim_amount = self.get_cached_claim_txo(claim_hash).amount

        effective_amount = self.get_effective_amount(claim_hash)
        channel_hash = self.get_channel_for_claim(claim_hash, tx_num, position)
        reposted_claim_hash = self.get_repost(claim_hash)
        reposted_tx_hash = None
        reposted_tx_position = None
        reposted_height = None
        if reposted_claim_hash:
            repost_txo = self.get_cached_claim_txo(reposted_claim_hash)
            if repost_txo:
                reposted_tx_hash = self.get_tx_hash(repost_txo.tx_num)
                reposted_tx_position = repost_txo.position
                reposted_height = bisect_right(self.tx_counts, repost_txo.tx_num)
        short_url = self.get_short_claim_id_url(name, normalized_name, claim_hash, root_tx_num, root_position)
        canonical_url = short_url
        claims_in_channel = self.get_claims_in_channel_count(claim_hash)
        channel_tx_hash = None
        channel_tx_position = None
        channel_height = None
        if channel_hash:
            channel_vals = self.get_cached_claim_txo(channel_hash)
            if channel_vals:
                channel_short_url = self.get_short_claim_id_url(
                    channel_vals.name, channel_vals.normalized_name, channel_hash, channel_vals.root_tx_num,
                    channel_vals.root_position
                )
                canonical_url = f'{channel_short_url}/{short_url}'
                channel_tx_hash = self.get_tx_hash(channel_vals.tx_num)
                channel_tx_position = channel_vals.position
                channel_height = bisect_right(self.tx_counts, channel_vals.tx_num)
        return ResolveResult(
            name, normalized_name, claim_hash, tx_num, position, tx_hash, height, claim_amount, short_url=short_url,
            is_controlling=controlling_claim.claim_hash == claim_hash, canonical_url=canonical_url,
            last_takeover_height=last_take_over_height, claims_in_channel=claims_in_channel,
            creation_height=created_height, activation_height=activation_height,
            expiration_height=expiration_height, effective_amount=effective_amount, support_amount=support_amount,
            channel_hash=channel_hash, reposted_claim_hash=reposted_claim_hash,
            reposted=self.get_reposted_count(claim_hash),
            signature_valid=None if not channel_hash else signature_valid, reposted_tx_hash=reposted_tx_hash,
            reposted_tx_position=reposted_tx_position, reposted_height=reposted_height,
            channel_tx_hash=channel_tx_hash, channel_tx_position=channel_tx_position, channel_height=channel_height,
        )

    async def _batch_resolve_parsed_urls(self, args: List[Tuple[str, Optional[str], Optional[int]]]) -> List[Optional[bytes]]:
        # list of name, claim id tuples (containing one or the other)
        # this is to preserve the ordering
        needed: List[Tuple[Optional[str], Optional[bytes]]] = []
        needed_full_claim_hashes = {}
        run_in_executor = asyncio.get_event_loop().run_in_executor

        def get_txo_for_partial_claim_id(normalized: str, claim_id: str):
            for key, claim_txo in self.prefix_db.claim_short_id.iterate(prefix=(normalized, claim_id[:10])):
                return claim_txo.tx_num, claim_txo.position

        def get_claim_by_amount(normalized: str, order: int):
            order = max(int(order or 1), 1)

            for _idx, (key, claim_val) in enumerate(self.prefix_db.bid_order.iterate(prefix=(normalized,))):
                if order > _idx + 1:
                    continue
                return claim_val.claim_hash

        for idx, (name, claim_id, amount_order) in enumerate(args):
            try:
                normalized_name = normalize_name(name)
            except UnicodeDecodeError:
                normalized_name = name
            if (not amount_order and not claim_id) or amount_order == 1:
                # winning resolution
                needed.append((normalized_name, None))
                continue

            full_claim_hash = None

            if claim_id:
                if len(claim_id) == 40:  # a full claim id
                    needed.append((None, bytes.fromhex(claim_id)))
                    continue

                # resolve by partial/complete claim id
                txo = await run_in_executor(self._executor, get_txo_for_partial_claim_id, normalized_name, claim_id)
                if txo:
                    needed_full_claim_hashes[idx] = txo
                needed.append((None, None))
                continue
            # resolve by amount ordering, 1 indexed
            needed.append((None, await run_in_executor(self._executor, get_claim_by_amount, normalized_name, amount_order)))

        # fetch the full claim hashes needed from urls with partial claim ids
        if needed_full_claim_hashes:
            unique_full_claims = defaultdict(list)
            for idx, partial_claim in needed_full_claim_hashes.items():
                unique_full_claims[partial_claim].append(idx)

            async for partial_claim_txo_k, v in self.prefix_db.txo_to_claim.multi_get_async_gen(self._executor, list(unique_full_claims.keys())):
                for idx in unique_full_claims[partial_claim_txo_k]:
                    needed[idx] = None, v.claim_hash

        # fetch the winning claim hashes for the urls using winning resolution
        needed_winning = list(set(normalized_name for normalized_name, _ in needed if normalized_name is not None))
        winning_indexes = [idx for idx in range(len(needed)) if needed[idx][0] is not None]
        controlling_claims = {
            name: takeover_v async for (name,), takeover_v in self.prefix_db.claim_takeover.multi_get_async_gen(
                self._executor, [(name,) for name in needed_winning]
            )
        }
        for idx in winning_indexes:
            name = needed[idx][0]
            controlling = controlling_claims[name]
            if controlling:
                needed[idx] = name, controlling.claim_hash

        return [
            claim_hash for _, claim_hash in needed
        ]

    def _resolve_claim_in_channel(self, channel_hash: bytes, normalized_name: str, stream_claim_id: Optional[str] = None):
        for key, stream in self.prefix_db.channel_to_claim.iterate(prefix=(channel_hash, normalized_name)):
            if stream_claim_id is not None and not stream.claim_hash.hex().startswith(stream_claim_id):
                continue
            return stream.claim_hash

    async def batch_resolve_urls(self, urls: List[str]) -> Dict[str, ExpandedResolveResult]:
        """
        Resolve a list of urls to a dictionary of urls to claims,
        including any extra claim(s) to expand the result, these could be a channel, a repost, or a repost channel
        """
        run_in_executor = asyncio.get_event_loop().run_in_executor

        # this is split into two stages, first we map the urls to primary claim hashes they resolve to
        # then the claims are collected in a batch, which also collects the extra claims needed for the primary matches

        # prepare to resolve all of the outer most levels of the urls - the first name in a url, a stream or a channel
        needed: List[Tuple[str, str, int]] = []  # (name, partial claim id, amount order) of a url
        needed_streams_in_channels = defaultdict(list)

        parsed_urls = {}
        url_parts_to_resolve = {}
        check_if_has_channel = set()
        resolved_urls = {}

        urls_to_parts_mapping = defaultdict(list)
        for url in urls:
            need_args = None
            try:
                parsed = URL.parse(url)
                parsed_urls[url] = parsed
            except ValueError as e:
                parsed_urls[url] = e
                continue
            stream = channel = None
            if parsed.has_stream_in_channel:
                stream = parsed.stream
                channel = parsed.channel
                need_args = (channel.name, channel.claim_id, channel.amount_order)
                needed_streams_in_channels[url].append(stream)
            elif parsed.has_channel:
                channel = parsed.channel
                need_args = (channel.name, channel.claim_id, channel.amount_order)
            elif parsed.has_stream:
                stream = parsed.stream
                need_args = (stream.name, stream.claim_id, stream.amount_order)
                check_if_has_channel.add(url)
            if need_args:
                needed.append(need_args)
                url_parts_to_resolve[url] = need_args
                urls_to_parts_mapping[need_args].append(url)

        # collect the claim hashes for the outer layer claims in the urls
        outer_layer_claims = {
            claim_hash: urls_to_parts_mapping[args] for args, claim_hash in zip(
                needed, await self._batch_resolve_parsed_urls(needed)
            )
        }
        reverse_mapping_outer_layer_claims = {}
        for claim_hash, _urls in outer_layer_claims.items():
            for url in _urls:
                reverse_mapping_outer_layer_claims[url] = claim_hash

        # needed_claims is a set of the total claim hashes to look up
        needed_claims = set(claim_hash for claim_hash in outer_layer_claims.keys() if claim_hash is not None)
        for claim_hash, _urls in outer_layer_claims.items():
            for url in _urls:
                # if it's a stream not in a channel or is a bare channel then this claim is all that's needed for the url
                if url not in needed_streams_in_channels:
                    if claim_hash:
                        resolved_urls[url] = claim_hash
                        needed_claims.add(claim_hash)

        # check if any claims we've accumulated are in channels, add the channels to the set of needed claims
        if needed_claims:
            claims_to_check_if_in_channel = list(needed_claims)
            txos = {
                claim_hash: txo
                async for (claim_hash, ), txo in self.prefix_db.claim_to_txo.multi_get_async_gen(
                    self._executor, [(claim_hash,) for claim_hash in claims_to_check_if_in_channel]
                )
            }
            needed_claims.update({
                claim.signing_hash
                async for _, claim in self.prefix_db.claim_to_channel.multi_get_async_gen(
                    self._executor, [
                        (claim_hash, txos[claim_hash].tx_num, txos[claim_hash].position)
                        for claim_hash in needed_claims if txos[claim_hash] is not None
                    ]
                )
                if claim is not None and claim.signing_hash is not None
            })

        # add the stream claim hashes for urls with channel streams to the needed set
        for url, streams in needed_streams_in_channels.items():
            resolved_channel_hash = reverse_mapping_outer_layer_claims.get(url)
            if not resolved_channel_hash:
                continue
            for stream in streams:
                stream_claim_hash = await run_in_executor(
                    self._executor, self._resolve_claim_in_channel, resolved_channel_hash, stream.normalized,
                    stream.claim_id
                )
                if stream_claim_hash:  # set the result claim hash to the stream
                    resolved_urls[url] = stream_claim_hash
                    needed_claims.add(stream_claim_hash)

        # collect all of the claim ResolveResults for the urls
        claims = {}
        if needed_claims:
            async for claim_hash, claim, extra in self._prepare_resolve_results(
                    list(needed_claims), apply_filtering=False):
                claims[claim_hash] = claim  # the primary result
                if extra:                   # extra results (channels, reposts, repost channels)
                    claims.update(extra)
        results = {}
        for url in urls:
            claim_hash = resolved_urls.get(url)
            parsed = parsed_urls[url]
            if not claim_hash or not claims[claim_hash]:
                if not isinstance(parsed, Exception) and parsed.has_channel and not parsed.has_stream:
                    results[url] = ExpandedResolveResult(
                        None, LookupError(f'Could not find channel in "{url}".'), None, None
                    )
                elif not isinstance(parsed, Exception) and not parsed.has_channel and parsed.has_stream:
                    results[url] = ExpandedResolveResult(
                        LookupError(f'Could not find claim at "{url}".'), None, None, None
                    )
                elif not isinstance(parsed, Exception) and parsed.has_stream_in_channel:
                    if reverse_mapping_outer_layer_claims.get(url) is None:
                        results[url] = ExpandedResolveResult(
                            None, LookupError(f'Could not find channel in "{url}".'), None, None
                        )
                    else:
                        results[url] = ExpandedResolveResult(
                            LookupError(f'Could not find claim at "{url}".'), None, None, None
                        )
                elif isinstance(parsed, ValueError):
                    results[url] = ExpandedResolveResult(
                        parsed, None, None, None
                    )
                continue

            claim = claims[claim_hash]
            stream = channel = None
            # FIXME: signatures

            if parsed.has_stream_in_channel or (not isinstance(claim, Exception) and claim.channel_hash):
                stream = claim
                if not isinstance(claim, Exception):
                    channel = claims[claim.channel_hash]
            elif url.lstrip('lbry://').startswith('@'):
                channel = claim
            else:
                stream = claim
            repost = reposted_channel = None
            if claim and not isinstance(claim, Exception) and claim.reposted_claim_hash:
                if claim.reposted_claim_hash in claims:
                    repost = claims[stream.reposted_claim_hash]
                    if repost and not isinstance(repost, Exception) and repost.channel_hash and repost.channel_hash in claims:
                        reposted_channel = claims[repost.channel_hash]
            results[url] = ExpandedResolveResult(stream, channel, repost, reposted_channel)
        return results

    async def resolve(self, url) -> ExpandedResolveResult:
        return (await self.batch_resolve_urls([url]))[url]

    def _fs_get_claim_by_hash(self, claim_hash):
        claim = self.get_cached_claim_txo(claim_hash)
        if claim:
            activation = self.get_activation(claim.tx_num, claim.position)
            return self._prepare_resolve_result(
                claim.tx_num, claim.position, claim_hash, claim.name, claim.root_tx_num, claim.root_position,
                activation, claim.channel_signature_is_valid
            )

    async def fs_getclaimbyid(self, claim_id):
        return await asyncio.get_event_loop().run_in_executor(
            self._executor, self._fs_get_claim_by_hash, bytes.fromhex(claim_id)
        )

    def get_claim_txo_amount(self, claim_hash: bytes) -> Optional[int]:
        claim = self.get_claim_txo(claim_hash)
        if claim:
            return claim.amount

    def get_block_hash(self, height: int) -> Optional[bytes]:
        v = self.prefix_db.block_hash.get(height)
        if v:
            return v.block_hash

    def get_support_txo_amount(self, claim_hash: bytes, tx_num: int, position: int) -> Optional[int]:
        v = self.prefix_db.claim_to_support.get(claim_hash, tx_num, position)
        return None if not v else v.amount

    def get_claim_txo(self, claim_hash: bytes) -> Optional[ClaimToTXOValue]:
        assert claim_hash
        return self.prefix_db.claim_to_txo.get(claim_hash)

    def _get_active_amount(self, claim_hash: bytes, txo_type: int, height: int) -> int:
        return sum(
            v.amount for v in self.prefix_db.active_amount.iterate(
                start=(claim_hash, txo_type, 0), stop=(claim_hash, txo_type, height), include_key=False
            )
        )

    def get_active_amount_as_of_height(self, claim_hash: bytes, height: int) -> int:
        for v in self.prefix_db.active_amount.iterate(
                start=(claim_hash, ACTIVATED_CLAIM_TXO_TYPE, 0), stop=(claim_hash, ACTIVATED_CLAIM_TXO_TYPE, height),
                include_key=False, reverse=True):
            return v.amount
        return 0

    def get_effective_amount(self, claim_hash: bytes) -> int:
        v = self.prefix_db.effective_amount.get(claim_hash)
        if v:
            return v.effective_amount
        return 0

    def get_url_effective_amount(self, name: str, claim_hash: bytes) -> Optional['BidOrderKey']:
        for k, v in self.prefix_db.bid_order.iterate(prefix=(name,)):
            if v.claim_hash == claim_hash:
                return k

    def get_claims_for_name(self, name):
        claims = []
        prefix = self.prefix_db.claim_short_id.pack_partial_key(name) + bytes([1])
        stop = self.prefix_db.claim_short_id.pack_partial_key(name) + int(2).to_bytes(1, byteorder='big')
        cf = self.prefix_db.column_families[self.prefix_db.claim_short_id.prefix]
        for _v in self.prefix_db.iterator(column_family=cf, start=prefix, iterate_upper_bound=stop, include_key=False):
            v = self.prefix_db.claim_short_id.unpack_value(_v)
            claim_hash = self.get_claim_from_txo(v.tx_num, v.position).claim_hash
            if claim_hash not in claims:
                claims.append(claim_hash)
        return claims

    def get_claims_in_channel_count(self, channel_hash) -> int:
        channel_count_val = self.prefix_db.channel_count.get(channel_hash)
        if channel_count_val is None:
            return 0
        return channel_count_val.count

    async def _prepare_resolve_results(self, claim_hashes: List[bytes], include_extra: bool = True,
                                       apply_blocking: bool = True, apply_filtering: bool = True):
        # determine which claims are reposts and what they are reposts of
        reposts = {
            claim_hash: repost.reposted_claim_hash
            async for (claim_hash, ), repost in self.prefix_db.repost.multi_get_async_gen(
                self._executor, [(claim_hash,) for claim_hash in claim_hashes]
            ) if repost
        }

        # deduplicate the requested claim hashes plus any found reposts
        claims_and_reposts = list(set(claim_hashes).union(set(reposts.values())))

        # get the claim txos for the claim hashes (including reposts)
        claims = {
            claim_hash: claim
            async for (claim_hash, ), claim in self.prefix_db.claim_to_txo.multi_get_async_gen(
                self._executor, [(claim_hash,) for claim_hash in claims_and_reposts]
            )
        }

        # get the channel hashes for all of the so far collected claim txos
        channels = {
            claim_hash: signed.signing_hash
            async for (claim_hash, tx_num, position), signed in self.prefix_db.claim_to_channel.multi_get_async_gen(
                self._executor, [
                    (claim_hash, claims[claim_hash].tx_num, claims[claim_hash].position)
                    for claim_hash in claims_and_reposts if claims[claim_hash] is not None
                ]
            ) if signed
        }

        # also look up any channel txos that we don't yet have (we could resolve @foo and @foo/test in one batch)
        needed_channels = list({channel_hash for channel_hash in channels.values() if channel_hash not in claims})
        if needed_channels:
            claims.update({
                claim_hash: claim
                async for (claim_hash,), claim in self.prefix_db.claim_to_txo.multi_get_async_gen(
                    self._executor, [(claim_hash,) for claim_hash in needed_channels]
                )
            })

        # collect all of the controlling claims for the set of names we've accumulated
        unique_names = list(
            sorted({claim_txo.normalized_name for claim_txo in claims.values() if claim_txo is not None})
        )
        controlling_claims = {
            name: takeover_v
            async for (name, ), takeover_v in self.prefix_db.claim_takeover.multi_get_async_gen(
                self._executor, [(name,) for name in unique_names]
            )
        }

        # collect all of the tx hashes for the accumulated claim txos
        claim_tx_hashes = {
            tx_num: tx_hash
            async for (tx_num, ), tx_hash in self.prefix_db.tx_hash.multi_get_async_gen(
                self._executor, [(claim.tx_num,) for claim in claims.values() if claim is not None], False
            )
        }

        # collect the short urls
        # TODO: consider a dedicated index for this query to make it multi_get-able
        run_in_executor = asyncio.get_event_loop().run_in_executor
        short_urls = {
            claim_hash: await run_in_executor(
                self._executor, self.get_short_claim_id_url,
                claim.name, claim.normalized_name, claim_hash, claim.root_tx_num, claim.root_position
            ) for claim_hash, claim in claims.items()
            if claim is not None
        }
        # collect all of the activation heights for the accumulated claims
        activations_needed = {
            (1, claim_txo.tx_num, claim_txo.position): claim_hash
            for claim_hash, claim_txo in claims.items()
            if claim_txo is not None
        }
        activations = {
            activations_needed[k]: -1 if not activation else activation.height
            async for k, activation in self.prefix_db.activated.multi_get_async_gen(
                self._executor, list(activations_needed.keys())
            )
        }
        # collect all of the support amounts for the accumulated claim txos
        supports = {
            claim_hash: 0 if support is None else support.amount
            async for (claim_hash, ), support in self.prefix_db.support_amount.multi_get_async_gen(
                self._executor, [(claim_hash,) for claim_hash in claims]
            )
        }
        # collect all of the counts of claims in channels for the accumulated claim txos
        claims_in_channels = {
            claim_hash: 0 if not v else v.count
            async for (claim_hash, ), v in self.prefix_db.channel_count.multi_get_async_gen(
                self._executor, [(claim_hash,) for claim_hash in claims]
            )
        }

        # collect all of the repost counts
        repost_counts = {
            claim_hash: 0 if not v else v.reposted_count
            async for (claim_hash, ), v in self.prefix_db.reposted_count.multi_get_async_gen(
                self._executor, [(claim_hash,) for claim_hash in claims]
            )
        }

        # collect all of the effective amounts
        effective_amounts = {
            claim_hash: 0 if not v else v.effective_amount
            async for (claim_hash, ), v in self.prefix_db.effective_amount.multi_get_async_gen(
                self._executor, [(claim_hash,) for claim_hash in claims]
            )
        }

        censoring_reasons = {}
        censoring_channels = {}
        censoring_reposts = {}

        def _get_censored_error(censor_type: str, canonical_url: str, channel_hash: bytes, repost_hash: bytes):
            channel = censoring_channels.get(channel_hash) or self._fs_get_claim_by_hash(channel_hash)
            censoring_channels[channel_hash] = channel
            claim = censoring_reposts.get(repost_hash) or self._fs_get_claim_by_hash(repost_hash)
            censoring_reposts[repost_hash] = claim
            censoring_url = f'lbry://{channel.name}#{channel_hash.hex()[:10]}/{claim.name}#{repost_hash.hex()[:10]}'
            if (claim.tx_hash, claim.position) in censoring_reasons:
                reason = censoring_reasons[(claim.tx_hash, claim.position)]
            else:
                reason = self.get_claim_metadata(claim.tx_hash, claim.position)
                if reason:
                    reason = reason.repost.description
                else:
                    reason = ''
                censoring_reasons[(claim.tx_hash, claim.position)] = reason
            return ResolveCensoredError(
                censor_type, f'lbry://{canonical_url}', censoring_url, channel_hash.hex(), reason, channel
            )

        def _prepare_result(touched, claim_txo):
            try:
                normalized_name = normalize_name(claim_txo.name)
            except UnicodeDecodeError:
                normalized_name = claim_txo.name
            effective_amount = effective_amounts[touched]
            reposted_count = repost_counts[touched]

            tx_hash = claim_tx_hashes[claim_txo.tx_num]
            claims_in_channel = claims_in_channels[touched]
            activation = activations[touched]
            support_amount = supports[touched]
            reposted_claim_hash = reposts.get(touched)
            channel_hash = channels.get(touched)
            controlling_claim = controlling_claims[normalized_name]
            claim_amount = claim_txo.amount

            height = bisect_right(self.tx_counts, claim_txo.tx_num)
            created_height = bisect_right(self.tx_counts, claim_txo.root_tx_num)
            expiration_height = self.coin.get_expiration_height(height)
            last_take_over_height = controlling_claim.height

            reposted_tx_hash = None
            reposted_tx_position = None
            reposted_height = None
            reposted_channel_hash = None
            if reposted_claim_hash:
                repost_txo = claims[reposted_claim_hash]
                if repost_txo and repost_txo.tx_num in claim_tx_hashes:
                    reposted_tx_hash = claim_tx_hashes[repost_txo.tx_num]
                    reposted_tx_position = repost_txo.position
                    reposted_height = bisect_right(self.tx_counts, repost_txo.tx_num)
                    if reposted_claim_hash in channels:
                        reposted_channel_hash = channels[reposted_claim_hash]
            short_url = short_urls[touched]
            channel_tx_hash = None
            channel_tx_position = None
            channel_height = None
            canonical_url = short_url
            if channel_hash:
                channel_txo = claims[channel_hash]
                if channel_txo and channel_txo.tx_num in claim_tx_hashes:
                    channel_short_url = short_urls[channel_hash]
                    canonical_url = f'{channel_short_url}/{short_url}'
                    channel_tx_hash = claim_tx_hashes[channel_txo.tx_num]
                    channel_tx_position = channel_txo.position
                    channel_height = bisect_right(self.tx_counts, channel_txo.tx_num)
            if apply_blocking:
                blocker = self.blocked_streams.get(touched) or self.blocked_streams.get(
                        reposted_claim_hash) or self.blocked_channels.get(touched) or self.blocked_channels.get(
                        reposted_channel_hash) or self.blocked_channels.get(channel_hash)
                if blocker:
                    blocker_channel_hash, blocker_repost_hash = blocker
                    return _get_censored_error(
                        'blocked', canonical_url, blocker_channel_hash, blocker_repost_hash
                    )

            if apply_filtering:
                filter_info = self.filtered_streams.get(touched) or self.filtered_streams.get(
                        reposted_claim_hash) or self.filtered_channels.get(touched) or self.filtered_channels.get(
                        reposted_channel_hash) or self.filtered_channels.get(channel_hash)
                if filter_info:
                    filter_channel_hash, filter_repost_hash = filter_info
                    return _get_censored_error(
                        'filtered', canonical_url, filter_channel_hash, filter_repost_hash
                    )

            return ResolveResult(
                claim_txo.name, normalized_name, touched, claim_txo.tx_num, claim_txo.position, tx_hash, height,
                claim_amount, short_url=short_url,
                is_controlling=controlling_claim.claim_hash == touched, canonical_url=canonical_url,
                last_takeover_height=last_take_over_height, claims_in_channel=claims_in_channel,
                creation_height=created_height, activation_height=activation,
                expiration_height=expiration_height, effective_amount=effective_amount,
                support_amount=support_amount,
                channel_hash=channel_hash, reposted_claim_hash=reposted_claim_hash,
                reposted=reposted_count,
                signature_valid=None if not channel_hash else claim_txo.channel_signature_is_valid,
                reposted_tx_hash=reposted_tx_hash,
                reposted_tx_position=reposted_tx_position, reposted_height=reposted_height,
                channel_tx_hash=channel_tx_hash, channel_tx_position=channel_tx_position,
                channel_height=channel_height,
            )

        total_extra = {}

        for touched in claim_hashes:
            extra = {}
            claim_txo = claims[touched]
            if not claim_txo:
                yield touched, None, extra
                continue
            if touched in total_extra:
                claim = total_extra[touched]
            else:
                claim = total_extra[touched] = _prepare_result(touched, claim_txo)
            if isinstance(claim, Exception):
                yield touched, claim, extra
                continue

            if include_extra:
                if claim.channel_hash:
                    channel_txo = claims[claim.channel_hash]
                    if channel_txo and not isinstance(channel_txo, Exception):
                        if claim.channel_hash in total_extra:
                            extra[claim.channel_hash] = total_extra[claim.channel_hash]
                        else:
                            extra[claim.channel_hash] = total_extra[claim.channel_hash] = _prepare_result(
                                claim.channel_hash, channel_txo
                            )
                if claim.reposted_claim_hash:
                    repost_txo = claims[claim.reposted_claim_hash]
                    if repost_txo and not isinstance(repost_txo, Exception):
                        if claim.reposted_claim_hash in total_extra:
                            extra[claim.reposted_claim_hash] = total_extra[claim.reposted_claim_hash]
                        else:
                            extra[claim.reposted_claim_hash] = total_extra[claim.reposted_claim_hash] = _prepare_result(
                                claim.reposted_claim_hash, repost_txo
                            )
                        if not isinstance(claim, Exception) and claim.reposted_claim_hash in channels:
                            reposted_channel_hash = channels[claim.reposted_claim_hash]
                            repost_channel_txo = claims[reposted_channel_hash]
                            if repost_channel_txo and not isinstance(repost_channel_txo, Exception):
                                if reposted_channel_hash in total_extra:
                                    extra[reposted_channel_hash] = total_extra[reposted_channel_hash]
                                else:
                                    extra[reposted_channel_hash] = total_extra[reposted_channel_hash] = _prepare_result(
                                        reposted_channel_hash, repost_channel_txo
                                    )
                            elif isinstance(repost_channel_txo, Exception):
                                extra[reposted_channel_hash] = repost_channel_txo
                            else:
                                pass  # FIXME: lookup error
                    elif isinstance(repost_txo, Exception):
                        extra[claim.reposted_claim_hash] = repost_txo
                    else:
                        pass  # FIXME: lookup error

            yield touched, claim, extra

    def get_streams_and_channels_reposted_by_channel_hashes(self, reposter_channel_hashes: Set[bytes]):
        streams, channels = {}, {}
        for reposter_channel_hash in reposter_channel_hashes:
            for stream in self.prefix_db.channel_to_claim.iterate((reposter_channel_hash, ), include_key=False):
                repost = self.get_repost(stream.claim_hash)
                if repost:
                    txo = self.get_claim_txo(repost)
                    if txo:
                        if txo.normalized_name.startswith('@'):
                            channels[repost] = reposter_channel_hash, stream.claim_hash
                        else:
                            streams[repost] = reposter_channel_hash, stream.claim_hash
        return streams, channels

    def get_channel_for_claim(self, claim_hash, tx_num, position) -> Optional[bytes]:
        v = self.prefix_db.claim_to_channel.get(claim_hash, tx_num, position)
        if v:
            return v.signing_hash

    def get_expired_by_height(self, height: int) -> Dict[bytes, Tuple[int, int, str, TxInput]]:
        expired = {}
        for k, v in self.prefix_db.claim_expiration.iterate(prefix=(height,)):
            tx_hash = self.get_tx_hash(k.tx_num)
            tx = self.coin.transaction(self.prefix_db.tx.get(tx_hash, deserialize_value=False))
            # treat it like a claim spend so it will delete/abandon properly
            # the _spend_claim function this result is fed to expects a txi, so make a mock one
            # print(f"\texpired lbry://{v.name} {v.claim_hash.hex()}")
            expired[v.claim_hash] = (
                k.tx_num, k.position, v.normalized_name,
                TxInput(prev_hash=tx_hash, prev_idx=k.position, script=tx.outputs[k.position].pk_script, sequence=0)
            )
        return expired

    def get_controlling_claim(self, name: str) -> Optional[ClaimTakeoverValue]:
        controlling = self.prefix_db.claim_takeover.get(name)
        if not controlling:
            return
        return controlling

    def get_claim_txos_for_name(self, name: str):
        txos = {}
        prefix = self.prefix_db.claim_short_id.pack_partial_key(name) + int(1).to_bytes(1, byteorder='big')
        stop = self.prefix_db.claim_short_id.pack_partial_key(name) + int(2).to_bytes(1, byteorder='big')
        cf = self.prefix_db.column_families[self.prefix_db.claim_short_id.prefix]
        for v in self.prefix_db.iterator(column_family=cf, start=prefix, iterate_upper_bound=stop, include_key=False):
            tx_num, nout = self.prefix_db.claim_short_id.unpack_value(v)
            txos[self.get_claim_from_txo(tx_num, nout).claim_hash] = tx_num, nout
        return txos

    def get_claim_metadata(self, tx_hash, nout):
        raw = self.prefix_db.tx.get(tx_hash, deserialize_value=False)
        try:
            return self.coin.transaction(raw).outputs[nout].metadata
        except:
            self.logger.exception("claim parsing for ES failed with tx: %s", tx_hash[::-1].hex())
            return

    async def get_claim_metadatas(self, txos: List[Tuple[bytes, int]]):
        tx_hashes = {tx_hash for tx_hash, _ in txos}
        txs = {
            k: self.coin.transaction(v) async for ((k,), v) in self.prefix_db.tx.multi_get_async_gen(
                self._executor, [(tx_hash,) for tx_hash in tx_hashes], deserialize_value=False
            )
        }

        def get_metadata(txo):
            if not txo:
                return
            try:
                return txo.metadata
            except:
                return

        return {
            (tx_hash, nout): get_metadata(txs[tx_hash].outputs[nout])
            for (tx_hash, nout) in txos
        }

    def get_activated_at_height(self, height: int) -> DefaultDict[PendingActivationValue, List[PendingActivationKey]]:
        activated = defaultdict(list)
        for k, v in self.prefix_db.pending_activation.iterate(prefix=(height,)):
            activated[v].append(k)
        return activated

    def get_future_activated(self, height: int) -> typing.Dict[PendingActivationValue, PendingActivationKey]:
        results = {}
        for k, v in self.prefix_db.pending_activation.iterate(
                start=(height + 1,), stop=(height + 1 + self.coin.maxTakeoverDelay,), reverse=True):
            if v not in results:
                results[v] = k
        return results

    async def _read_tx_counts(self):
        # if self.tx_counts is not None:
        #     return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase

        def get_counts():
            return [
                v.tx_count for v in self.prefix_db.tx_count.iterate(
                    start=(0,), stop=(self.db_height + 1,), include_key=False, fill_cache=False
                )
            ]

        tx_counts = await asyncio.get_event_loop().run_in_executor(self._executor, get_counts)
        assert len(tx_counts) == self.db_height + 1, f"{len(tx_counts)} vs {self.db_height + 1}"
        self.tx_counts = array.array('I', tx_counts)

        if self.tx_counts:
            assert self.db_tx_count == self.tx_counts[-1], \
                f"{self.db_tx_count} vs {self.tx_counts[-1]} ({len(self.tx_counts)} counts)"
        else:
            assert self.db_tx_count == 0

    async def _read_claim_txos(self):
        def read_claim_txos():
            set_claim_to_txo = self.claim_to_txo.__setitem__
            for k, v in self.prefix_db.claim_to_txo.iterate(fill_cache=False):
                set_claim_to_txo(k.claim_hash, v)
                self.txo_to_claim[v.tx_num][v.position] = k.claim_hash

        self.claim_to_txo.clear()
        self.txo_to_claim.clear()
        start = time.perf_counter()
        self.logger.info("loading claims")
        await asyncio.get_event_loop().run_in_executor(self._executor, read_claim_txos)
        ts = time.perf_counter() - start
        self.logger.info("loaded %i claim txos in %ss", len(self.claim_to_txo), round(ts, 4))

    # async def _read_headers(self):
    #     # if self.headers is not None:
    #     #     return
    #
    #     def get_headers():
    #         return [
    #             header for header in self.prefix_db.header.iterate(
    #                 start=(0, ), stop=(self.db_height + 1, ), include_key=False, fill_cache=False, deserialize_value=False
    #             )
    #         ]
    #
    #     headers = await asyncio.get_event_loop().run_in_executor(self._executor, get_headers)
    #     assert len(headers) - 1 == self.db_height, f"{len(headers)} vs {self.db_height}"
    #     self.headers = headers

    async def _read_block_hashes(self):
        def get_block_hashes():
            return [
                block_hash for block_hash in self.prefix_db.block_hash.iterate(
                    start=(0, ), stop=(self.db_height + 1, ), include_key=False, fill_cache=False, deserialize_value=False
                )
            ]

        block_hashes = await asyncio.get_event_loop().run_in_executor(self._executor, get_block_hashes)
        # assert len(block_hashes) == len(self.headers)
        self.block_hashes = block_hashes

    async def _read_tx_hashes(self):
        def _read_tx_hashes():
            return list(self.prefix_db.tx_hash.iterate(start=(0,), stop=(self.db_tx_count + 1,), include_key=False, fill_cache=False, deserialize_value=False))

        self.logger.info("loading tx hashes")
        self.total_transactions.clear()
        self.tx_num_mapping.clear()
        start = time.perf_counter()
        self.total_transactions.extend(await asyncio.get_event_loop().run_in_executor(self._executor, _read_tx_hashes))
        self.tx_num_mapping = {
            tx_hash: tx_num for tx_num, tx_hash in enumerate(self.total_transactions)
        }
        ts = time.perf_counter() - start
        self.logger.info("loaded %i tx hashes in %ss", len(self.total_transactions), round(ts, 4))

    def open_db(self):
        if self.prefix_db:
            return
        secondary_path = '' if not self._secondary_name else os.path.join(
            self._db_dir, self._secondary_name
        )
        db_path = os.path.join(self._db_dir, 'lbry-rocksdb')
        self.prefix_db = PrefixDB(
            db_path, reorg_limit=self._reorg_limit, max_open_files=self._db_max_open_files,
            unsafe_prefixes={DBStatePrefixRow.prefix, MempoolTXPrefixRow.prefix, HashXMempoolStatusPrefixRow.prefix},
            secondary_path=secondary_path
        )

        if secondary_path != '':
            self.logger.info(f'opened db for read only: lbry-rocksdb (%s)', db_path)
        else:
            self.logger.info(f'opened db for writing: lbry-rocksdb (%s)', db_path)

        # read db state
        self.read_db_state()

        # These are our state as we move ahead of DB state
        self.fs_tx_count = self.db_tx_count
        self.last_flush_tx_count = self.fs_tx_count

        # Log some stats
        self.logger.info(f'DB version: {self.db_version:d}')
        self.logger.info(f'coin: {self.coin.NAME}')
        self.logger.info(f'network: {self.coin.NET}')
        self.logger.info(f'height: {self.db_height:,d}')
        self.logger.info(f'tip: {hash_to_hex_str(self.db_tip)}')
        self.logger.info(f'tx count: {self.db_tx_count:,d}')
        if self.last_indexed_address_status_height:
            self.logger.info(f'last indexed address statuses at block {self.last_indexed_address_status_height}')
        self.logger.info(f'using address status index: {self._index_address_status}')
        if self.filtering_channel_hashes:
            self.logger.info("filtering claims reposted by channels: %s", ', '.join(map(lambda x: x.hex(), self.filtering_channel_hashes)))
        if self.blocking_channel_hashes:
            self.logger.info("blocking claims reposted by channels: %s",
                             ', '.join(map(lambda x: x.hex(), self.blocking_channel_hashes)))
        if self.hist_db_version not in self.DB_VERSIONS:
            msg = f'this software only handles DB versions {self.DB_VERSIONS}'
            self.logger.error(msg)
            raise RuntimeError(msg)
        self.logger.info(f'flush count: {self.hist_flush_count:,d}')
        self.utxo_flush_count = self.hist_flush_count

    async def initialize_caches(self):
        await self._read_tx_counts()
        await self._read_block_hashes()
        if self._cache_all_claim_txos:
            await self._read_claim_txos()
        if self._cache_all_tx_hashes:
            await self._read_tx_hashes()
        if self.db_height > 0:
            await self.populate_header_merkle_cache()

    def close(self):
        self.prefix_db.close()
        self.prefix_db = None

    def _get_hashX_status(self, hashX: bytes):
        mempool_status = self.prefix_db.hashX_mempool_status.get(hashX, deserialize_value=False)
        if mempool_status:
            return mempool_status.hex()
        status = self.prefix_db.hashX_status.get(hashX, deserialize_value=False)
        if status:
            return status.hex()

    def _get_hashX_statuses(self, hashXes: List[bytes]):
        statuses = {
            hashX: status
            for hashX, status in zip(hashXes, self.prefix_db.hashX_mempool_status.multi_get(
                [(hashX,) for hashX in hashXes], deserialize_value=False
            )) if status is not None
        }
        if len(statuses) < len(hashXes):
            statuses.update({
                hashX: status
                for hashX, status in zip(hashXes, self.prefix_db.hashX_status.multi_get(
                    [(hashX,) for hashX in hashXes if hashX not in statuses], deserialize_value=False
                )) if status is not None
            })
        return [None if hashX not in statuses else statuses[hashX].hex() for hashX in hashXes]

    async def get_hashX_status(self, hashX: bytes):
        return await asyncio.get_event_loop().run_in_executor(self._executor, self._get_hashX_status, hashX)

    async def get_hashX_statuses(self, hashXes: List[bytes]):
        return await asyncio.get_event_loop().run_in_executor(self._executor, self._get_hashX_statuses, hashXes)

    def get_tx_hash(self, tx_num: int) -> bytes:
        if self._cache_all_tx_hashes:
            return self.total_transactions[tx_num]
        return self.prefix_db.tx_hash.get(tx_num, deserialize_value=False)

    def _get_tx_hashes(self, tx_nums: List[int]) -> List[Optional[bytes]]:
        if self._cache_all_tx_hashes:
            return [None if tx_num > self.db_tx_count else self.total_transactions[tx_num] for tx_num in tx_nums]
        return self.prefix_db.tx_hash.multi_get([(tx_num,) for tx_num in tx_nums], deserialize_value=False)

    async def get_tx_hashes(self, tx_nums: List[int]) -> List[Optional[bytes]]:
        if self._cache_all_tx_hashes:
            result = []
            append_result = result.append
            for tx_num in tx_nums:
                append_result(None if tx_num > self.db_tx_count else self.total_transactions[tx_num])
                await asyncio.sleep(0)
            return result

        def _get_tx_hashes():
            return self.prefix_db.tx_hash.multi_get([(tx_num,) for tx_num in tx_nums], deserialize_value=False)

        return await asyncio.get_event_loop().run_in_executor(self._executor, _get_tx_hashes)

    def get_raw_mempool_tx(self, tx_hash: bytes) -> Optional[bytes]:
        return self.prefix_db.mempool_tx.get(tx_hash, deserialize_value=False)

    def get_raw_confirmed_tx(self, tx_hash: bytes) -> Optional[bytes]:
        return self.prefix_db.tx.get(tx_hash, deserialize_value=False)

    def get_raw_tx(self, tx_hash: bytes) -> Optional[bytes]:
        return self.get_raw_mempool_tx(tx_hash) or self.get_raw_confirmed_tx(tx_hash)

    def get_tx_num(self, tx_hash: bytes) -> int:
        if self._cache_all_tx_hashes:
            return self.tx_num_mapping[tx_hash]
        return self.prefix_db.tx_num.get(tx_hash).tx_num

    def get_cached_claim_txo(self, claim_hash: bytes) -> Optional[ClaimToTXOValue]:
        if self._cache_all_claim_txos:
            return self.claim_to_txo.get(claim_hash)
        return self.prefix_db.claim_to_txo.get_pending(claim_hash)

    def get_cached_claim_hash(self, tx_num: int, position: int) -> Optional[bytes]:
        if self._cache_all_claim_txos:
            if tx_num not in self.txo_to_claim:
                return
            return self.txo_to_claim[tx_num].get(position, None)
        v = self.prefix_db.txo_to_claim.get_pending(tx_num, position)
        return None if not v else v.claim_hash

    def get_cached_claim_exists(self, tx_num: int, position: int) -> bool:
        return self.get_cached_claim_hash(tx_num, position) is not None

    # Header merkle cache

    async def populate_header_merkle_cache(self):
        self.logger.info('populating header merkle cache...')
        length = max(1, self.db_height - self._reorg_limit)
        start = time.time()
        await self.header_mc.initialize(length)
        elapsed = time.time() - start
        self.logger.info(f'header merkle cache populated in {elapsed:.1f}s')

    async def header_branch_and_root(self, length, height):
        return await self.header_mc.branch_and_root(length, height)

    async def raw_header(self, height):
        """Return the binary header at the given height."""
        header, n = await self.read_headers(height, 1)
        if n != 1:
            raise IndexError(f'height {height:,d} out of range')
        return header

    def encode_headers(self, start_height, count, headers):
        key = (start_height, count)
        if not self.encoded_headers.get(key):
            compressobj = zlib.compressobj(wbits=-15, level=1, memLevel=9)
            headers = base64.b64encode(compressobj.compress(headers) + compressobj.flush()).decode()
            if start_height % 1000 != 0:
                return headers
            self.encoded_headers[key] = headers
        return self.encoded_headers.get(key)

    async def read_headers(self, start_height, count) -> typing.Tuple[bytes, int]:
        """Requires start_height >= 0, count >= 0.  Reads as many headers as
        are available starting at start_height up to count.  This
        would be zero if start_height is beyond self.db_height, for
        example.

        Returns a (binary, n) pair where binary is the concatenated
        binary headers, and n is the count of headers returned.
        """

        if start_height < 0 or count < 0:
            raise DBError(f'{count:,d} headers starting at {start_height:,d} not on disk')

        disk_count = max(0, min(count, self.db_height + 1 - start_height))

        def read_headers():
            x = b''.join(
                self.prefix_db.header.iterate(
                    start=(start_height,), stop=(start_height+disk_count,), include_key=False, deserialize_value=False
                )
            )
            return x

        if disk_count:
            return await asyncio.get_event_loop().run_in_executor(self._executor, read_headers), disk_count
        return b'', 0

    def fs_tx_hash(self, tx_num):
        """Return a par (tx_hash, tx_height) for the given tx number.

        If the tx_height is not on disk, returns (None, tx_height)."""
        tx_height = bisect_right(self.tx_counts, tx_num)
        if tx_height > self.db_height:
            return None, tx_height
        try:
            return self.get_tx_hash(tx_num), tx_height
        except IndexError:
            self.logger.exception(
                "Failed to access a cached transaction, known bug #3142 "
                "should be fixed in #3205"
            )
            return None, tx_height

    def get_block_txs(self, height: int) -> List[bytes]:
        return self.prefix_db.block_txs.get(height).tx_hashes

    async def get_transactions_and_merkles(self, txids: List[str]):
        tx_infos = {}
        needed_tx_nums = []
        needed_confirmed = []
        needed_mempool = []
        cached_mempool = []
        needed_heights = set()
        tx_heights_and_positions = defaultdict(list)

        run_in_executor = asyncio.get_event_loop().run_in_executor

        for txid in txids:
            tx_hash_bytes = bytes.fromhex(txid)[::-1]
            cached_tx = self.tx_cache.get(tx_hash_bytes)
            if cached_tx:
                tx, tx_num, tx_pos, tx_height = cached_tx
                if tx_height > 0:
                    needed_heights.add(tx_height)
                    tx_heights_and_positions[tx_height].append((tx_hash_bytes, tx, tx_num, tx_pos))
                else:
                    cached_mempool.append((tx_hash_bytes, tx))
            else:
                if self._cache_all_tx_hashes and tx_hash_bytes in self.tx_num_mapping:
                    needed_confirmed.append((tx_hash_bytes, self.tx_num_mapping[tx_hash_bytes]))
                else:
                    needed_tx_nums.append(tx_hash_bytes)

        if needed_tx_nums:
            for tx_hash_bytes, v in zip(needed_tx_nums, await run_in_executor(
                    self._executor, self.prefix_db.tx_num.multi_get, [(tx_hash,) for tx_hash in needed_tx_nums],
                    True, True)):
                tx_num = None if v is None else v.tx_num
                if tx_num is not None:
                    needed_confirmed.append((tx_hash_bytes, tx_num))
                else:
                    needed_mempool.append(tx_hash_bytes)
                await asyncio.sleep(0)

        if needed_confirmed:
            for (tx_hash_bytes, tx_num), tx in zip(needed_confirmed, await run_in_executor(
                    self._executor, self.prefix_db.tx.multi_get, [(tx_hash,) for tx_hash, _ in needed_confirmed],
                    True, False)):
                tx_height = bisect_right(self.tx_counts, tx_num)
                needed_heights.add(tx_height)
                tx_pos = tx_num - self.tx_counts[tx_height - 1]
                tx_heights_and_positions[tx_height].append((tx_hash_bytes, tx, tx_num, tx_pos))
                self.tx_cache[tx_hash_bytes] = tx, tx_num, tx_pos, tx_height

        sorted_heights = list(sorted(needed_heights))
        merkles: Dict[int, FastMerkleCacheItem] = {}   # uses existing cached merkle trees when they're available
        needed_for_merkle_cache = []
        for height in sorted_heights:
            merkle = self.merkle_cache.get(height)
            if merkle:
                merkles[height] = merkle
            else:
                needed_for_merkle_cache.append(height)
        if needed_for_merkle_cache:
            block_txs = await run_in_executor(
                self._executor, self.prefix_db.block_txs.multi_get,
                [(height,) for height in needed_for_merkle_cache]
            )
            for height, v in zip(needed_for_merkle_cache, block_txs):
                merkles[height] = self.merkle_cache[height] = FastMerkleCacheItem(v.tx_hashes)
                await asyncio.sleep(0)
        for tx_height, v in tx_heights_and_positions.items():
            get_merkle_branch = merkles[tx_height].branch
            for (tx_hash_bytes, tx, tx_num, tx_pos) in v:
                tx_infos[tx_hash_bytes[::-1].hex()] = None if not tx else tx.hex(), {
                    'block_height': tx_height,
                    'merkle': get_merkle_branch(tx_pos),
                    'pos': tx_pos
                }
        for tx_hash_bytes, tx in cached_mempool:
            tx_infos[tx_hash_bytes[::-1].hex()] = None if not tx else tx.hex(), {'block_height': -1}
        if needed_mempool:
            for tx_hash_bytes, tx in zip(needed_mempool, await run_in_executor(
                    self._executor, self.prefix_db.mempool_tx.multi_get, [(tx_hash,) for tx_hash in needed_mempool],
                    True, False)):
                self.tx_cache[tx_hash_bytes] = tx, None, None, -1
                tx_infos[tx_hash_bytes[::-1].hex()] = None if not tx else tx.hex(), {'block_height': -1}
                await asyncio.sleep(0)
        return {txid: tx_infos.get(txid) for txid in txids}  # match ordering of the txs in the request

    async def fs_block_hashes(self, height, count):
        if height + count > self.db_height + 1:
            raise DBError(f'only got {len(self.block_hashes) - height:,d} headers starting at {height:,d}, not {count:,d}')
        return self.block_hashes[height:height + count]

    def _read_history(self, hashX: bytes, limit: Optional[int] = 1000) -> List[int]:
        txs = []
        txs_extend = txs.extend
        for hist in self.prefix_db.hashX_history.iterate(prefix=(hashX,), include_key=False):
            txs_extend(hist)
            if limit and len(txs) >= limit:
                break
        return txs

    async def read_history(self, hashX: bytes, limit: Optional[int] = 1000) -> List[int]:
        return await asyncio.get_event_loop().run_in_executor(self._executor, self._read_history, hashX, limit)

    async def limited_history(self, hashX, *, limit=1000):
        """Return an unpruned, sorted list of (tx_hash, height) tuples of
        confirmed transactions that touched the address, earliest in
        the blockchain first.  Includes both spending and receiving
        transactions.  By default returns at most 1000 entries.  Set
        limit to None to get them all.
        """
        run_in_executor = asyncio.get_event_loop().run_in_executor
        tx_nums = await run_in_executor(self._executor, self._read_history, hashX, limit)
        history = []
        append_history = history.append
        while tx_nums:
            batch, tx_nums = tx_nums[:100], tx_nums[100:]
            for tx_num, tx_hash in zip(batch, await self.get_tx_hashes(batch)):
                append_history((tx_hash, bisect_right(self.tx_counts, tx_num)))
            await asyncio.sleep(0)
        return history

    # -- Undo information

    def min_undo_height(self, max_height):
        """Returns a height from which we should store undo info."""
        return max_height - self._reorg_limit + 1

    def read_db_state(self):
        state = self.prefix_db.db_state.get()

        if not state:
            self.db_height = -1
            self.db_tx_count = 0
            self.db_tip = b'\0' * 32
            self.db_version = max(self.DB_VERSIONS)
            self.utxo_flush_count = 0
            self.wall_time = 0
            self.catching_up = True
            self.hist_flush_count = 0
            self.hist_comp_flush_count = -1
            self.hist_comp_cursor = -1
            self.hist_db_version = max(self.DB_VERSIONS)
            self.es_sync_height = 0
            self.last_indexed_address_status_height = 0
        else:
            self.db_version = state.db_version
            if self.db_version not in self.DB_VERSIONS:
                raise DBError(f'your DB version is {self.db_version} but this '
                                   f'software only handles versions {self.DB_VERSIONS}')
            # backwards compat
            genesis_hash = state.genesis
            if genesis_hash.hex() != self.coin.GENESIS_HASH:
                raise DBError(f'DB genesis hash {genesis_hash} does not '
                                   f'match coin {self.coin.GENESIS_HASH}')
            self.db_height = state.height
            self.db_tx_count = state.tx_count
            self.db_tip = state.tip
            self.utxo_flush_count = state.utxo_flush_count
            self.wall_time = state.wall_time
            self.catching_up = state.catching_up
            self.hist_flush_count = state.hist_flush_count
            self.hist_comp_flush_count = state.comp_flush_count
            self.hist_comp_cursor = state.comp_cursor
            self.hist_db_version = state.db_version
            self.es_sync_height = state.es_sync_height
            self.last_indexed_address_status_height = state.hashX_status_last_indexed_height
        return state

    def assert_db_state(self):
        state = self.prefix_db.db_state.get()
        assert self.db_version == state.db_version, f"{self.db_version} != {state.db_version}"
        assert self.db_height == state.height, f"{self.db_height} != {state.height}"
        assert self.db_tx_count == state.tx_count, f"{self.db_tx_count} != {state.tx_count}"
        assert self.db_tip == state.tip, f"{self.db_tip} != {state.tip}"
        assert self.catching_up == state.catching_up, f"{self.catching_up} != {state.catching_up}"
        assert self.es_sync_height == state.es_sync_height, f"{self.es_sync_height} != {state.es_sync_height}"

    async def all_utxos(self, hashX):
        """Return all UTXOs for an address sorted in no particular order."""
        def read_utxos():
            fs_tx_hash = self.fs_tx_hash
            utxo_info = [
                (k.tx_num, k.nout, v.amount) for k, v in self.prefix_db.utxo.iterate(prefix=(hashX, ))
            ]
            return [UTXO(tx_num, nout, *fs_tx_hash(tx_num), value=value) for (tx_num, nout, value) in utxo_info]

        while True:
            utxos = await asyncio.get_event_loop().run_in_executor(self._executor, read_utxos)
            if all(utxo.tx_hash is not None for utxo in utxos):
                return utxos
            self.logger.warning(f'all_utxos: tx hash not '
                                f'found (reorg?), retrying...')
            await asyncio.sleep(0.25)
