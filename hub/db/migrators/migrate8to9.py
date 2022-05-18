import logging

FROM_VERSION = 8
TO_VERSION = 9


def migrate(db):
    log = logging.getLogger(__name__)
    prefix_db = db.prefix_db
    index_address_status = db._index_address_status

    log.info("migrating the db to version 9")

    if not index_address_status:
        log.info("deleting the existing address status index")
        to_delete = list(prefix_db.hashX_status.iterate(deserialize_key=False, deserialize_value=False))
        while to_delete:
            batch, to_delete = to_delete[:10000], to_delete[10000:]
            if batch:
                prefix_db.multi_delete(batch)
                prefix_db.unsafe_commit()

    db.db_version = 9
    db.write_db_state()
    db.prefix_db.unsafe_commit()
    log.info("finished migration")
