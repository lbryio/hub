import logging
from collections import defaultdict

FROM_VERSION = 11
TO_VERSION = 12


def migrate(db):
    log = logging.getLogger(__name__)
    prefix_db = db.prefix_db

    log.info("migrating the db to version 12")

    effective_amounts = defaultdict(int)

    log.info("deleting any existing future effective amounts")

    to_delete = list(prefix_db.future_effective_amount.iterate(deserialize_key=False, deserialize_value=False))
    while to_delete:
        batch, to_delete = to_delete[:100000], to_delete[100000:]
        if batch:
            prefix_db.multi_delete(batch)
            prefix_db.unsafe_commit()

    log.info("calculating future claim effective amounts for the new index at block %i", db.db_height)
    cnt = 0
    for k, v in prefix_db.active_amount.iterate():
        cnt += 1
        effective_amounts[k.claim_hash] += v.amount
        if cnt % 1000000 == 0:
            log.info("scanned %i amounts for %i claims", cnt, len(effective_amounts))
    log.info("preparing to insert future effective amounts")

    effective_amounts_to_put = [
        prefix_db.future_effective_amount.pack_item(claim_hash, effective_amount)
        for claim_hash, effective_amount in effective_amounts.items()
    ]

    log.info("inserting %i future effective amounts", len(effective_amounts_to_put))

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

    db.db_version = 12
    db.write_db_state()
    db.prefix_db.unsafe_commit()
    log.info("finished migration to version 12")
