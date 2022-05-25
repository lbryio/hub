from typing import Optional, Set, Dict
from hub.schema.claim import guess_stream_type
from hub.schema.result import Censor
from hub.common import hash160, STREAM_TYPES, CLAIM_TYPES
from hub.db import SecondaryDB
from hub.db.common import ResolveResult


class ElasticSyncDB(SecondaryDB):
    def estimate_timestamp(self, height: int) -> int:
        header = self.prefix_db.header.get(height, deserialize_value=False)
        if header:
            return int.from_bytes(header[100:104], byteorder='little')
        return int(160.6855883050695 * height)

    def _prepare_claim_metadata(self, claim_hash: bytes, claim: ResolveResult):
        metadata = self.get_claim_metadata(claim.tx_hash, claim.position)
        if not metadata:
            return
        metadata = metadata
        if not metadata.is_stream or not metadata.stream.has_fee:
            fee_amount = 0
        else:
            fee_amount = int(max(metadata.stream.fee.amount or 0, 0) * 1000)
            if fee_amount >= 9223372036854775807:
                return
        reposted_claim_hash = claim.reposted_claim_hash
        reposted_claim = None
        reposted_metadata = None
        if reposted_claim_hash:
            reposted_claim = self.get_cached_claim_txo(reposted_claim_hash)
            if not reposted_claim:
                return
            reposted_metadata = self.get_claim_metadata(
                self.get_tx_hash(reposted_claim.tx_num), reposted_claim.position
            )
            if not reposted_metadata:
                return
        reposted_tags = []
        reposted_languages = []
        reposted_has_source = False
        reposted_claim_type = None
        reposted_stream_type = None
        reposted_media_type = None
        reposted_fee_amount = None
        reposted_fee_currency = None
        reposted_duration = None
        if reposted_claim:
            raw_reposted_claim_tx = self.prefix_db.tx.get(claim.reposted_tx_hash, deserialize_value=False)
            try:
                reposted_metadata = self.coin.transaction(
                    raw_reposted_claim_tx
                ).outputs[reposted_claim.position].metadata
            except:
                self.logger.error("failed to parse reposted claim in tx %s that was reposted by %s",
                                  claim.reposted_claim_hash.hex(), claim_hash.hex())
                return
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
                return
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
                    return
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
            return
        claim_tags = [tag for tag in meta.tags]
        claim_languages = [lang.language or 'none' for lang in meta.languages] or ['none']

        tags = list(set(claim_tags).union(set(reposted_tags)))
        languages = list(set(claim_languages).union(set(reposted_languages)))
        blocked_hash = self.blocked_streams.get(claim_hash) or self.blocked_streams.get(
            reposted_claim_hash) or self.blocked_channels.get(claim_hash) or self.blocked_channels.get(
            reposted_claim_hash) or self.blocked_channels.get(claim.channel_hash)
        filtered_hash = self.filtered_streams.get(claim_hash) or self.filtered_streams.get(
            reposted_claim_hash) or self.filtered_channels.get(claim_hash) or self.filtered_channels.get(
            reposted_claim_hash) or self.filtered_channels.get(claim.channel_hash)
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
            'censoring_channel_id': (blocked_hash or filtered_hash or b'').hex() or None,
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
        return value

    async def all_claims_producer(self, batch_size=500_000):
        batch = []
        if self._cache_all_claim_txos:
            claim_iterator = self.claim_to_txo.items()
        else:
            claim_iterator = map(lambda item: (item[0].claim_hash, item[1]), self.prefix_db.claim_to_txo.iterate())

        for claim_hash, claim_txo in claim_iterator:
            # TODO: fix the couple of claim txos that dont have controlling names
            if not self.prefix_db.claim_takeover.get(claim_txo.normalized_name):
                continue
            activation = self.get_activation(claim_txo.tx_num, claim_txo.position)
            claim = self._prepare_resolve_result(
                claim_txo.tx_num, claim_txo.position, claim_hash, claim_txo.name, claim_txo.root_tx_num,
                claim_txo.root_position, activation, claim_txo.channel_signature_is_valid
            )
            if claim:
                batch.append(claim)
            if len(batch) == batch_size:
                batch.sort(key=lambda x: x.tx_hash)  # sort is to improve read-ahead hits
                for claim in batch:
                    meta = self._prepare_claim_metadata(claim.claim_hash, claim)
                    if meta:
                        yield meta
                batch.clear()
        batch.sort(key=lambda x: x.tx_hash)
        for claim in batch:
            meta = self._prepare_claim_metadata(claim.claim_hash, claim)
            if meta:
                yield meta
        batch.clear()

    def claim_producer(self, claim_hash: bytes) -> Optional[Dict]:
        claim_txo = self.get_cached_claim_txo(claim_hash)
        if not claim_txo:
            self.logger.warning("can't sync non existent claim to ES: %s", claim_hash.hex())
            return
        if not self.prefix_db.claim_takeover.get(claim_txo.normalized_name):
            self.logger.warning("can't sync non existent claim to ES: %s", claim_hash.hex())
            return
        activation = self.get_activation(claim_txo.tx_num, claim_txo.position)
        claim = self._prepare_resolve_result(
            claim_txo.tx_num, claim_txo.position, claim_hash, claim_txo.name, claim_txo.root_tx_num,
            claim_txo.root_position, activation, claim_txo.channel_signature_is_valid
        )
        if not claim:
            self.logger.warning("wat")
            return
        return self._prepare_claim_metadata(claim.claim_hash, claim)

    def claims_producer(self, claim_hashes: Set[bytes]):
        batch = []
        results = []

        for claim_hash in claim_hashes:
            claim_txo = self.get_cached_claim_txo(claim_hash)
            if not claim_txo:
                self.logger.warning("can't sync non existent claim to ES: %s", claim_hash.hex())
                continue
            if not self.prefix_db.claim_takeover.get(claim_txo.normalized_name):
                self.logger.warning("can't sync non existent claim to ES: %s", claim_hash.hex())
                continue

            activation = self.get_activation(claim_txo.tx_num, claim_txo.position)
            claim = self._prepare_resolve_result(
                claim_txo.tx_num, claim_txo.position, claim_hash, claim_txo.name, claim_txo.root_tx_num,
                claim_txo.root_position, activation, claim_txo.channel_signature_is_valid
            )
            if claim:
                batch.append(claim)

        batch.sort(key=lambda x: x.tx_hash)

        for claim in batch:
            _meta = self._prepare_claim_metadata(claim.claim_hash, claim)
            if _meta:
                results.append(_meta)
        return results
