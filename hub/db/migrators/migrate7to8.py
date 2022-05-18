import logging
import time
import array
import typing
from bisect import bisect_right
from hub.common import sha256
if typing.TYPE_CHECKING:
    from hub.db.db import HubDB

FROM_VERSION = 7
TO_VERSION = 8


def get_all_hashXs(db):
    def iterator():
        last_hashX = None
        for k in db.prefix_db.hashX_history.iterate(deserialize_key=False, include_value=False):
            hashX = k[1:12]
            if last_hashX is None:
                last_hashX = hashX
            if last_hashX != hashX:
                yield hashX
                last_hashX = hashX
        if last_hashX:
            yield last_hashX
    return [hashX for hashX in iterator()]


def hashX_history(db: 'HubDB', hashX: bytes):
    history = b''
    to_delete = []
    for k, v in db.prefix_db.hashX_history.iterate(prefix=(hashX,), deserialize_value=False, deserialize_key=False):
        to_delete.append((k, v))
        history += v
    return history, to_delete


def hashX_status_from_history(db: 'HubDB', history: bytes) -> bytes:
    tx_counts = db.tx_counts
    hist_tx_nums = array.array('I')
    hist_tx_nums.frombytes(history)
    hist = ''
    for tx_num in hist_tx_nums:
        hist += f'{db.get_tx_hash(tx_num)[::-1].hex()}:{bisect_right(tx_counts, tx_num)}:'
    return sha256(hist.encode())


def migrate(db):
    log = logging.getLogger(__name__)
    start = time.perf_counter()
    prefix_db = db.prefix_db
    hashXs = get_all_hashXs(db)
    log.info(f"loaded {len(hashXs)} hashXs in {round(time.perf_counter() - start, 2)}s, "
             f"now building the status index...")
    op_cnt = 0
    hashX_cnt = 0
    for hashX in hashXs:
        hashX_cnt += 1
        key = prefix_db.hashX_status.pack_key(hashX)
        history, to_delete = hashX_history(db, hashX)
        status = hashX_status_from_history(db, history)
        existing_status = prefix_db.hashX_status.get(hashX, deserialize_value=False)
        if existing_status and existing_status != status:
            prefix_db.stage_raw_delete(key, existing_status)
            op_cnt += 1
        elif existing_status == status:
            pass
        else:
            prefix_db.stage_raw_put(key, status)
            op_cnt += 1
        if len(to_delete) > 1:
            for k, v in to_delete:
                prefix_db.stage_raw_delete(k, v)
                op_cnt += 1
            if history:
                prefix_db.stage_raw_put(prefix_db.hashX_history.pack_key(hashX, 0), history)
                op_cnt += 1
        if op_cnt > 100000:
            prefix_db.unsafe_commit()
            log.info(f"wrote {hashX_cnt}/{len(hashXs)} hashXs statuses")
            op_cnt = 0
    if op_cnt:
        prefix_db.unsafe_commit()
        log.info(f"wrote {hashX_cnt}/{len(hashXs)} hashXs statuses")
    db.db_version = 8
    db.write_db_state()
    db.prefix_db.unsafe_commit()
    log.info("finished migration")
