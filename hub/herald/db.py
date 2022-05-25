import asyncio
from typing import List
from concurrent.futures.thread import ThreadPoolExecutor
from hub.db import SecondaryDB


class HeraldDB(SecondaryDB):
    def __init__(self, coin, db_dir: str, secondary_name: str, max_open_files: int = -1, reorg_limit: int = 200,
                 cache_all_claim_txos: bool = False, cache_all_tx_hashes: bool = False,
                 blocking_channel_ids: List[str] = None,
                 filtering_channel_ids: List[str] = None, executor: ThreadPoolExecutor = None,
                 index_address_status=False):
        super().__init__(coin, db_dir, secondary_name, max_open_files, reorg_limit, cache_all_claim_txos,
                         cache_all_tx_hashes, blocking_channel_ids, filtering_channel_ids, executor,
                         index_address_status)
        # self.headers = None

    # async def _read_headers(self):
    #     def get_headers():
    #         return [
    #             header for header in self.prefix_db.header.iterate(
    #                 start=(0, ), stop=(self.db_height + 1, ), include_key=False, fill_cache=False,
    #                 deserialize_value=False
    #             )
    #         ]
    #
    #     headers = await asyncio.get_event_loop().run_in_executor(self._executor, get_headers)
    #     assert len(headers) - 1 == self.db_height, f"{len(headers)} vs {self.db_height}"
    #     self.headers = headers

    # async def initialize_caches(self):
    #     await super().initialize_caches()
    #     await self._read_headers()
