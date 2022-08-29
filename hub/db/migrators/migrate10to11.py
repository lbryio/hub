import logging
from collections import defaultdict
from hub.db.prefixes import ACTIVATED_SUPPORT_TXO_TYPE

FROM_VERSION = 10
TO_VERSION = 11


def migrate(db):
    log = logging.getLogger(__name__)
    prefix_db = db.prefix_db

    log.info("migrating the db to version 11")

    effective_amounts = defaultdict(int)
    support_amounts = defaultdict(int)

    log.info("deleting any existing effective amounts")

    to_delete = list(prefix_db.effective_amount.iterate(deserialize_key=False, deserialize_value=False))
    while to_delete:
        batch, to_delete = to_delete[:100000], to_delete[100000:]
        if batch:
            prefix_db.multi_delete(batch)
            prefix_db.unsafe_commit()

    log.info("calculating claim effective amounts for the new index at block %i", db.db_height)

    height = db.db_height

    cnt = 0
    for k, v in prefix_db.active_amount.iterate():
        cnt += 1
        claim_hash, activation_height, amount = k.claim_hash, k.activation_height, v.amount
        if activation_height <= height:
            effective_amounts[claim_hash] += amount
            if k.txo_type == ACTIVATED_SUPPORT_TXO_TYPE:
                support_amounts[claim_hash] += amount
        if cnt % 1000000 == 0:
            log.info("scanned %i amounts for %i claims", cnt, len(effective_amounts))

    log.info("preparing to insert effective amounts")

    effective_amounts_to_put = [
        prefix_db.effective_amount.pack_item(claim_hash, effective_amount, support_amounts[claim_hash])
        for claim_hash, effective_amount in effective_amounts.items()
    ]

    log.info("inserting %i effective amounts", len(effective_amounts_to_put))

    cnt = 0

    while effective_amounts_to_put:
        batch, effective_amounts_to_put = effective_amounts_to_put[:100000], effective_amounts_to_put[100000:]
        if batch:
            prefix_db.multi_put(batch)
            prefix_db.unsafe_commit()
            cnt += len(batch)
            if cnt % 1000000 == 0:
                log.info("inserted effective amounts for %i claims", cnt)

    log.info("finished building the effective amount index")

    db.db_version = 11
    db.write_db_state()
    db.prefix_db.unsafe_commit()
    log.info("finished migration to version 11")
