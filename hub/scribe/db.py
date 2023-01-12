import hashlib
import asyncio
import array
import time
from typing import List
from concurrent.futures.thread import ThreadPoolExecutor
from bisect import bisect_right
from hub.common import ResumableSHA256
from hub.db import SecondaryDB


class PrimaryDB(SecondaryDB):
    def __init__(self, coin, db_dir: str, reorg_limit: int = 200,
                 cache_all_tx_hashes: bool = False,
                 max_open_files: int = 64, blocking_channel_ids: List[str] = None,
                 filtering_channel_ids: List[str] = None, executor: ThreadPoolExecutor = None,
                 index_address_status=False, enforce_integrity=True):
        super().__init__(coin, db_dir, '', max_open_files, reorg_limit, cache_all_tx_hashes,
                         blocking_channel_ids, filtering_channel_ids, executor, index_address_status,
                         enforce_integrity=enforce_integrity)

    def _rebuild_hashX_status_index(self, start_height: int):
        self.logger.warning("rebuilding the address status index...")
        prefix_db = self.prefix_db

        def hashX_iterator():
            last_hashX = None
            for k in prefix_db.hashX_history.iterate(deserialize_key=False, include_value=False):
                hashX = k[1:12]
                if last_hashX is None:
                    last_hashX = hashX
                if last_hashX != hashX:
                    yield hashX
                    last_hashX = hashX
            if last_hashX:
                yield last_hashX

        def hashX_status_from_history(history: bytes) -> ResumableSHA256:
            tx_counts = self.tx_counts
            hist_tx_nums = array.array('I')
            hist_tx_nums.frombytes(history)
            digest = ResumableSHA256()
            digest.update(
                b''.join(f'{tx_hash[::-1].hex()}:{bisect_right(tx_counts, tx_num)}:'.encode()
                for tx_num, tx_hash in zip(
                    hist_tx_nums,
                    self.prefix_db.tx_hash.multi_get([(tx_num,) for tx_num in hist_tx_nums], deserialize_value=False)
                ))
            )
            return digest

        start = time.perf_counter()

        if start_height <= 0:
            self.logger.info("loading all blockchain addresses, this will take a little while...")
            hashXs = list({hashX for hashX in hashX_iterator()})
        else:
            self.logger.info("loading addresses since block %i...", start_height)
            hashXs = set()
            for touched in prefix_db.touched_hashX.iterate(start=(start_height,), stop=(self.db_height + 1,),
                                                           include_key=False):
                hashXs.update(touched.touched_hashXs)
            hashXs = list(hashXs)

        self.logger.info(f"loaded {len(hashXs)} hashXs in {round(time.perf_counter() - start, 2)}s, "
                         f"now building the status index...")
        op_cnt = 0
        hashX_cnt = 0
        for hashX in hashXs:
            hashX_cnt += 1
            key = prefix_db.hashX_status.pack_key(hashX)
            history = b''.join(prefix_db.hashX_history.iterate(prefix=(hashX,), deserialize_value=False, include_key=False))
            digester = hashX_status_from_history(history)
            status = digester.digest()
            existing_status = prefix_db.hashX_status.get(hashX, deserialize_value=False)
            existing_digester = prefix_db.hashX_history_hasher.get(hashX)
            if not existing_status:
                prefix_db.stash_raw_put(key, status)
                op_cnt += 1
            else:
                prefix_db.stash_raw_delete(key, existing_status)
                prefix_db.stash_raw_put(key, status)
                op_cnt += 2
            if not existing_digester:
                prefix_db.hashX_history_hasher.stash_put((hashX,), (digester,))
                op_cnt += 1
            else:
                prefix_db.hashX_history_hasher.stash_delete((hashX,), existing_digester)
                prefix_db.hashX_history_hasher.stash_put((hashX,), (digester,))
                op_cnt += 2
            if op_cnt > 100000:
                prefix_db.unsafe_commit()
                self.logger.info(f"wrote {hashX_cnt}/{len(hashXs)} hashXs statuses...")
                op_cnt = 0
        if op_cnt:
            prefix_db.unsafe_commit()
            self.logger.info(f"wrote {hashX_cnt}/{len(hashXs)} hashXs statuses...")
        self._index_address_status = True
        self.last_indexed_address_status_height = self.db_height
        self.write_db_state()
        self.prefix_db.unsafe_commit()
        self.logger.info("finished indexing address statuses")

    def rebuild_hashX_status_index(self, start_height: int):
        return asyncio.get_event_loop().run_in_executor(self._executor, self._rebuild_hashX_status_index, start_height)

    def apply_expiration_extension_fork(self):
        # TODO: this can't be reorged
        for k, v in self.prefix_db.claim_expiration.iterate():
            self.prefix_db.claim_expiration.stash_delete(k, v)
            self.prefix_db.claim_expiration.stash_put(
                (bisect_right(self.tx_counts, k.tx_num) + self.coin.nExtendedClaimExpirationTime,
                 k.tx_num, k.position), v
            )
        self.prefix_db.unsafe_commit()

    def write_db_state(self):
        """Write (UTXO) state to the batch."""
        if self.db_height > 0:
            existing = self.prefix_db.db_state.get()
            self.prefix_db.db_state.stash_delete((), existing.expanded)
        self.prefix_db.db_state.stash_put((), (
            self.genesis_bytes, self.db_height, self.db_tx_count, self.db_tip,
            self.utxo_flush_count, int(self.wall_time), self.catching_up, self._index_address_status, self.db_version,
            self.hist_flush_count, self.hist_comp_flush_count, self.hist_comp_cursor,
            self.es_sync_height, self.last_indexed_address_status_height
            )
        )
