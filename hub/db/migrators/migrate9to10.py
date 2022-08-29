import logging
from collections import defaultdict
from hub.db.revertable import RevertablePut

FROM_VERSION = 9
TO_VERSION = 10


def migrate(db):
    log = logging.getLogger(__name__)
    prefix_db = db.prefix_db

    log.info("migrating the db to version 10")

    repost_counts = defaultdict(int)
    log.info("deleting any existing repost counts")

    to_delete = list(prefix_db.reposted_count.iterate(deserialize_key=False, deserialize_value=False))
    while to_delete:
        batch, to_delete = to_delete[:10000], to_delete[10000:]
        if batch:
            prefix_db.multi_delete(batch)
            prefix_db.unsafe_commit()

    log.info("counting reposts to build the new index")

    for reposted_claim_hash in prefix_db.repost.iterate(include_key=False, deserialize_value=False):
        repost_counts[reposted_claim_hash] += 1

    log.info("inserting repost counts")

    reposted_counts_to_put = [
        prefix_db.reposted_count.pack_item(claim_hash, count)
        for claim_hash, count in repost_counts.items()
    ]

    while reposted_counts_to_put:
        batch, reposted_counts_to_put = reposted_counts_to_put[:10000], reposted_counts_to_put[10000:]
        if batch:
            prefix_db.multi_put(batch)
            prefix_db.unsafe_commit()

    log.info("finished building the repost count index")

    db.db_version = 10
    db.write_db_state()
    db.prefix_db.unsafe_commit()
    log.info("finished migration to version 10")
