from typing import Optional, Set, Dict, List
from concurrent.futures.thread import ThreadPoolExecutor
from hub.schema.claim import guess_stream_type
from hub.schema.result import Censor
from hub.common import hash160, STREAM_TYPES, CLAIM_TYPES, LRUCache
from hub.db import SecondaryDB
from hub.db.common import ResolveResult


class ElasticSyncDB(SecondaryDB):
    def __init__(self, coin, db_dir: str, secondary_name: str, max_open_files: int = -1, reorg_limit: int = 200,
                 cache_all_claim_txos: bool = False, cache_all_tx_hashes: bool = False,
                 blocking_channel_ids: List[str] = None,
                 filtering_channel_ids: List[str] = None, executor: ThreadPoolExecutor = None,
                 index_address_status=False):
        super().__init__(coin, db_dir, secondary_name, max_open_files, reorg_limit, cache_all_claim_txos,
                         cache_all_tx_hashes, blocking_channel_ids, filtering_channel_ids, executor,
                         index_address_status)
        self.block_timestamp_cache = LRUCache(1024)

    def estimate_timestamp(self, height: int) -> int:
        if height in self.block_timestamp_cache:
            return self.block_timestamp_cache[height]
        header = self.prefix_db.header.get(height, deserialize_value=False)
        timestamp = int(self.coin.genesisTime + (self.coin.averageBlockOffset * height)) \
            if not header else int.from_bytes(header[100:104], byteorder='little')
        self.block_timestamp_cache[height] = timestamp
        return timestamp

    async def prepare_claim_metadata_batch(self, claims: Dict[bytes, ResolveResult], extras):
        metadatas = {}
        needed_txos = set()

        for claim_hash, claim in claims.items():
            reposted_claim_hash = claim.reposted_claim_hash
            needed_txos.add((claim.tx_hash, claim.position))
            if reposted_claim_hash:
                if not reposted_claim_hash not in extras:
                    continue
                reposted_claim = extras.get((reposted_claim_hash))
                if reposted_claim:
                    needed_txos.add((reposted_claim.tx_hash, reposted_claim.position))
        metadatas.update(await self.get_claim_metadatas(list(needed_txos)))

        for claim_hash, claim in claims.items():
            metadata = metadatas.get((claim.tx_hash, claim.position))
            if not metadata:
                continue
            if not metadata.is_stream or not metadata.stream.has_fee:
                fee_amount = 0
            else:
                fee_amount = int(max(metadata.stream.fee.amount or 0, 0) * 1000)
                if fee_amount >= 9223372036854775807:
                    continue
            reposted_claim_hash = claim.reposted_claim_hash
            reposted_metadata = None
            if reposted_claim_hash:
                if reposted_claim_hash in extras:
                    reposted_claim = extras[reposted_claim_hash]
                    reposted_metadata = metadatas.get((reposted_claim.tx_hash, reposted_claim.position))

            reposted_tags = []
            reposted_languages = []
            reposted_has_source = False
            reposted_claim_type = None
            reposted_stream_type = None
            reposted_media_type = None
            reposted_fee_amount = None
            reposted_fee_currency = None
            reposted_duration = None

            if reposted_metadata:
                if reposted_metadata.is_stream:
                    meta = reposted_metadata.stream
                elif reposted_metadata.is_channel:
                    meta = reposted_metadata.channel
                elif reposted_metadata.is_collection:
                    meta = reposted_metadata.collection
                elif reposted_metadata.is_repost:
                    meta = reposted_metadata.repost
                else:
                    continue
                reposted_tags = [tag for tag in meta.tags]
                reposted_languages = [lang.language or 'none' for lang in meta.languages] or ['none']
                reposted_has_source = False if not reposted_metadata.is_stream else reposted_metadata.stream.has_source
                reposted_claim_type = CLAIM_TYPES[reposted_metadata.claim_type]
                reposted_stream_type = STREAM_TYPES[guess_stream_type(reposted_metadata.stream.source.media_type)] \
                    if reposted_has_source else 0
                reposted_media_type = reposted_metadata.stream.source.media_type if reposted_metadata.is_stream else 0
                if not reposted_metadata.is_stream or not reposted_metadata.stream.has_fee:
                    reposted_fee_amount = 0
                else:
                    reposted_fee_amount = int(max(reposted_metadata.stream.fee.amount or 0, 0) * 1000)
                    if reposted_fee_amount >= 9223372036854775807:
                        continue
                reposted_fee_currency = None if not reposted_metadata.is_stream else reposted_metadata.stream.fee.currency
                reposted_duration = None
                if reposted_metadata.is_stream and \
                        (reposted_metadata.stream.video.duration or reposted_metadata.stream.audio.duration):
                    reposted_duration = reposted_metadata.stream.video.duration or reposted_metadata.stream.audio.duration
            if metadata.is_stream:
                meta = metadata.stream
            elif metadata.is_channel:
                meta = metadata.channel
            elif metadata.is_collection:
                meta = metadata.collection
            elif metadata.is_repost:
                meta = metadata.repost
            else:
                continue
            claim_tags = [tag for tag in meta.tags]
            claim_languages = [lang.language or 'none' for lang in meta.languages] or ['none']
            tags = list(set(claim_tags).union(set(reposted_tags)))
            languages = list(set(claim_languages).union(set(reposted_languages)))
            blocking_channel = None
            blocked_hash = self.blocked_streams.get(claim_hash) or self.blocked_streams.get(
                reposted_claim_hash) or self.blocked_channels.get(claim_hash) or self.blocked_channels.get(
                reposted_claim_hash) or self.blocked_channels.get(claim.channel_hash)
            if blocked_hash:
                blocking_channel, blocked_hash = blocked_hash
            filtered_channel = None
            filtered_hash = self.filtered_streams.get(claim_hash) or self.filtered_streams.get(
                reposted_claim_hash) or self.filtered_channels.get(claim_hash) or self.filtered_channels.get(
                reposted_claim_hash) or self.filtered_channels.get(claim.channel_hash)
            if filtered_hash:
                filtered_channel, filtered_hash = filtered_hash
            value = {
                'claim_id': claim_hash.hex(),
                'claim_name': claim.name,
                'normalized_name': claim.normalized_name,
                'tx_id': claim.tx_hash[::-1].hex(),
                'tx_num': claim.tx_num,
                'tx_nout': claim.position,
                'amount': claim.amount,
                'timestamp': self.estimate_timestamp(claim.height),
                'creation_timestamp': self.estimate_timestamp(claim.creation_height),
                'height': claim.height,
                'creation_height': claim.creation_height,
                'activation_height': claim.activation_height,
                'expiration_height': claim.expiration_height,
                'effective_amount': claim.effective_amount,
                'support_amount': claim.support_amount,
                'is_controlling': bool(claim.is_controlling),
                'last_take_over_height': claim.last_takeover_height,
                'short_url': claim.short_url,
                'canonical_url': claim.canonical_url,
                'title': None if not metadata.is_stream else metadata.stream.title,
                'author': None if not metadata.is_stream else metadata.stream.author,
                'description': None if not metadata.is_stream else metadata.stream.description,
                'claim_type': CLAIM_TYPES[metadata.claim_type],
                'has_source': reposted_has_source if metadata.is_repost else (
                    False if not metadata.is_stream else metadata.stream.has_source),
                'sd_hash': metadata.stream.source.sd_hash if metadata.is_stream and metadata.stream.has_source else None,
                'stream_type': STREAM_TYPES[guess_stream_type(metadata.stream.source.media_type)]
                if metadata.is_stream and metadata.stream.has_source
                else reposted_stream_type if metadata.is_repost else 0,
                'media_type': metadata.stream.source.media_type
                if metadata.is_stream else reposted_media_type if metadata.is_repost else None,
                'fee_amount': fee_amount if not metadata.is_repost else reposted_fee_amount,
                'fee_currency': metadata.stream.fee.currency
                if metadata.is_stream else reposted_fee_currency if metadata.is_repost else None,
                'repost_count': self.get_reposted_count(claim_hash),
                'reposted_claim_id': None if not reposted_claim_hash else reposted_claim_hash.hex(),
                'reposted_claim_type': reposted_claim_type,
                'reposted_has_source': reposted_has_source,
                'channel_id': None if not metadata.is_signed else metadata.signing_channel_hash[::-1].hex(),
                'public_key_id': None if not metadata.is_channel else
                self.coin.P2PKH_address_from_hash160(hash160(metadata.channel.public_key_bytes)),
                'signature': (metadata.signature or b'').hex() or None,
                # 'signature_digest': metadata.signature,
                'is_signature_valid': bool(claim.signature_valid),
                'tags': tags,
                'languages': languages,
                'censor_type': Censor.RESOLVE if blocked_hash else Censor.SEARCH if filtered_hash else Censor.NOT_CENSORED,
                'censoring_channel_id': (blocking_channel or filtered_channel or b'').hex() or None,
                'censoring_claim_id': (blocked_hash or filtered_hash or b'').hex() or None,
                'claims_in_channel': None if not metadata.is_channel else self.get_claims_in_channel_count(claim_hash),
                'reposted_tx_id': None if not claim.reposted_tx_hash else claim.reposted_tx_hash[::-1].hex(),
                'reposted_tx_position': claim.reposted_tx_position,
                'reposted_height': claim.reposted_height,
                'channel_tx_id': None if not claim.channel_tx_hash else claim.channel_tx_hash[::-1].hex(),
                'channel_tx_position': claim.channel_tx_position,
                'channel_height': claim.channel_height,
            }

            if metadata.is_repost and reposted_duration is not None:
                value['duration'] = reposted_duration
            elif metadata.is_stream and (metadata.stream.video.duration or metadata.stream.audio.duration):
                value['duration'] = metadata.stream.video.duration or metadata.stream.audio.duration
            if metadata.is_stream:
                value['release_time'] = metadata.stream.release_time or value['creation_timestamp']
            elif metadata.is_repost or metadata.is_collection:
                value['release_time'] = value['creation_timestamp']
            yield value

    async def all_claims_producer(self, batch_size: int):
        batch = []
        for k in self.prefix_db.claim_to_txo.iterate(include_value=False):
            batch.append(k.claim_hash)
            if len(batch) == batch_size:
                claims = {}
                total_extras = {}
                async for claim_hash, claim, extras in self._prepare_resolve_results(batch, include_extra=False,
                                                                                     apply_blocking=False,
                                                                                     apply_filtering=False):
                    if not claim:
                        self.logger.warning("wat")
                        continue
                    claims[claim_hash] = claim
                    total_extras[claim_hash] = claim
                    total_extras.update(extras)
                async for claim in self.prepare_claim_metadata_batch(claims, total_extras):
                    if claim:
                        yield claim
                batch.clear()
        if batch:
            claims = {}
            total_extras = {}
            async for claim_hash, claim, extras in self._prepare_resolve_results(batch, include_extra=False,
                                                                               apply_blocking=False,
                                                                               apply_filtering=False):
                if not claim:
                    self.logger.warning("wat")
                    continue
                claims[claim_hash] = claim
                total_extras[claim_hash] = claim
                total_extras.update(extras)
            async for claim in self.prepare_claim_metadata_batch(claims, total_extras):
                if claim:
                    yield claim
