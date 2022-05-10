import struct
import typing
import rocksdb
from typing import Optional
from scribe.db.common import DB_PREFIXES, COLUMN_SETTINGS
from scribe.db.revertable import RevertableOpStack, RevertablePut, RevertableDelete


ROW_TYPES = {}


class PrefixRowType(type):
    def __new__(cls, name, bases, kwargs):
        klass = super().__new__(cls, name, bases, kwargs)
        if name != "PrefixRow":
            ROW_TYPES[klass.prefix] = klass
            cache_size = klass.cache_size
            COLUMN_SETTINGS[klass.prefix] = {
                'cache_size': cache_size,
            }
        return klass


class PrefixRow(metaclass=PrefixRowType):
    prefix: bytes
    key_struct: struct.Struct
    value_struct: struct.Struct
    key_part_lambdas = []
    cache_size: int = 1024 * 1024 * 16

    def __init__(self, db: 'rocksdb.DB', op_stack: RevertableOpStack):
        self._db = db
        self._op_stack = op_stack
        self._column_family = self._db.get_column_family(self.prefix)
        if not self._column_family.is_valid:
            raise RuntimeError('column family is not valid')

    def iterate(self, prefix=None, start=None, stop=None, reverse: bool = False, include_key: bool = True,
                include_value: bool = True, fill_cache: bool = True, deserialize_key: bool = True,
                deserialize_value: bool = True):
        if not prefix and not start and not stop:
            prefix = ()
        if prefix is not None:
            prefix = self.pack_partial_key(*prefix)
            if stop is None:
                try:
                   stop = (int.from_bytes(prefix, byteorder='big') + 1).to_bytes(len(prefix), byteorder='big')
                except OverflowError:
                    stop = (int.from_bytes(prefix, byteorder='big') + 1).to_bytes(len(prefix) + 1, byteorder='big')
            else:
                stop = self.pack_partial_key(*stop)
        else:
            if start is not None:
                start = self.pack_partial_key(*start)
            if stop is not None:
                stop = self.pack_partial_key(*stop)

        if deserialize_key:
            key_getter = lambda _k: self.unpack_key(_k)
        else:
            key_getter = lambda _k: _k
        if deserialize_value:
            value_getter = lambda _v: self.unpack_value(_v)
        else:
            value_getter = lambda _v: _v

        it = self._db.iterator(
            start or prefix, self._column_family, iterate_lower_bound=(start or prefix),
            iterate_upper_bound=stop, reverse=reverse, include_key=include_key,
            include_value=include_value, fill_cache=fill_cache, prefix_same_as_start=False
        )

        if include_key and include_value:
            for k, v in it:
                yield key_getter(k[1]), value_getter(v)
        elif include_key:
            for k in it:
                yield key_getter(k[1])
        elif include_value:
            for v in it:
                yield value_getter(v)
        else:
            for _ in it:
                yield None

    def get(self, *key_args, fill_cache=True, deserialize_value=True):
        v = self._db.get((self._column_family, self.pack_key(*key_args)), fill_cache=fill_cache)
        if v:
            return v if not deserialize_value else self.unpack_value(v)

    def multi_get(self, key_args: typing.List[typing.Tuple], fill_cache=True, deserialize_value=True):
        packed_keys = {tuple(args): self.pack_key(*args) for args in key_args}
        db_result = self._db.multi_get([(self._column_family, packed_keys[tuple(args)]) for args in key_args],
                                   fill_cache=fill_cache)
        result = {k[-1]: v for k, v in (db_result or {}).items()}

        def handle_value(v):
            return None if v is None else v if not deserialize_value else self.unpack_value(v)

        return [
            handle_value(result[packed_keys[tuple(k_args)]]) for k_args in key_args
        ]

    def stage_multi_put(self, items):
        self._op_stack.multi_put([RevertablePut(self.pack_key(*k), self.pack_value(*v)) for k, v in items])

    def get_pending(self, *key_args, fill_cache=True, deserialize_value=True):
        packed_key = self.pack_key(*key_args)
        last_op = self._op_stack.get_last_op_for_key(packed_key)
        if last_op:
            if last_op.is_put:
                return last_op.value if not deserialize_value else self.unpack_value(last_op.value)
            else:  # it's a delete
                return
        v = self._db.get((self._column_family, packed_key), fill_cache=fill_cache)
        if v:
            return v if not deserialize_value else self.unpack_value(v)

    def stage_put(self, key_args=(), value_args=()):
        self._op_stack.append_op(RevertablePut(self.pack_key(*key_args), self.pack_value(*value_args)))

    def stage_delete(self, key_args=(), value_args=()):
        self._op_stack.append_op(RevertableDelete(self.pack_key(*key_args), self.pack_value(*value_args)))

    @classmethod
    def pack_partial_key(cls, *args) -> bytes:
        return cls.prefix + cls.key_part_lambdas[len(args)](*args)

    @classmethod
    def pack_key(cls, *args) -> bytes:
        return cls.prefix + cls.key_struct.pack(*args)

    @classmethod
    def pack_value(cls, *args) -> bytes:
        return cls.value_struct.pack(*args)

    @classmethod
    def unpack_key(cls, key: bytes):
        assert key[:1] == cls.prefix, f"prefix should be {cls.prefix}, got {key[:1]}"
        return cls.key_struct.unpack(key[1:])

    @classmethod
    def unpack_value(cls, data: bytes):
        return cls.value_struct.unpack(data)

    @classmethod
    def unpack_item(cls, key: bytes, value: bytes):
        return cls.unpack_key(key), cls.unpack_value(value)

    def estimate_num_keys(self) -> int:
        return int(self._db.get_property(b'rocksdb.estimate-num-keys', self._column_family).decode())


class BasePrefixDB:
    """
    Base class for a revertable rocksdb database (a rocksdb db where each set of applied changes can be undone)
    """
    UNDO_KEY_STRUCT = struct.Struct(b'>Q32s')
    PARTIAL_UNDO_KEY_STRUCT = struct.Struct(b'>Q')

    def __init__(self, path, max_open_files=64, secondary_path='', max_undo_depth: int = 200, unsafe_prefixes=None):
        column_family_options = {}
        for prefix in DB_PREFIXES:
            settings = COLUMN_SETTINGS[prefix.value]
            column_family_options[prefix.value] = rocksdb.ColumnFamilyOptions()
            column_family_options[prefix.value].table_factory = rocksdb.BlockBasedTableFactory(
                block_cache=rocksdb.LRUCache(settings['cache_size']),
            )
        self.column_families: typing.Dict[bytes, 'rocksdb.ColumnFamilyHandle'] = {}
        options = rocksdb.Options(
            create_if_missing=True, use_fsync=False, target_file_size_base=33554432,
            max_open_files=max_open_files if not secondary_path else -1, create_missing_column_families=True
        )
        self._db = rocksdb.DB(
            path, options, secondary_name=secondary_path, column_families=column_family_options
        )
        for prefix in DB_PREFIXES:
            cf = self._db.get_column_family(prefix.value)
            if cf is None and not secondary_path:
                self._db.create_column_family(prefix.value, column_family_options[prefix.value])
                cf = self._db.get_column_family(prefix.value)
            self.column_families[prefix.value] = cf

        self._op_stack = RevertableOpStack(self.get, self.multi_get, unsafe_prefixes=unsafe_prefixes)
        self._max_undo_depth = max_undo_depth

    def unsafe_commit(self):
        """
        Write staged changes to the database without keeping undo information
        Changes written cannot be undone
        """
        try:
            if not len(self._op_stack):
                return
            with self._db.write_batch(sync=True) as batch:
                batch_put = batch.put
                batch_delete = batch.delete
                get_column_family = self.column_families.__getitem__
                for staged_change in self._op_stack:
                    column_family = get_column_family(DB_PREFIXES(staged_change.key[:1]).value)
                    if staged_change.is_put:
                        batch_put((column_family, staged_change.key), staged_change.value)
                    else:
                        batch_delete((column_family, staged_change.key))
        finally:
            self._op_stack.clear()

    def commit(self, height: int, block_hash: bytes):
        """
        Write changes for a block height to the database and keep undo information so that the changes can be reverted
        """
        undo_ops = self._op_stack.get_undo_ops()
        delete_undos = []
        if height > self._max_undo_depth:
            delete_undos.extend(self._db.iterator(
                start=DB_PREFIXES.undo.value + self.PARTIAL_UNDO_KEY_STRUCT.pack(0),
                iterate_upper_bound=DB_PREFIXES.undo.value + self.PARTIAL_UNDO_KEY_STRUCT.pack(height - self._max_undo_depth),
                include_value=False
            ))
        try:
            undo_c_f = self.column_families[DB_PREFIXES.undo.value]
            with self._db.write_batch(sync=True) as batch:
                batch_put = batch.put
                batch_delete = batch.delete
                get_column_family = self.column_families.__getitem__
                for staged_change in self._op_stack:
                    column_family = get_column_family(DB_PREFIXES(staged_change.key[:1]).value)
                    if staged_change.is_put:
                        batch_put((column_family, staged_change.key), staged_change.value)
                    else:
                        batch_delete((column_family, staged_change.key))
                for undo_to_delete in delete_undos:
                    batch_delete((undo_c_f, undo_to_delete))
                batch_put((undo_c_f, DB_PREFIXES.undo.value + self.UNDO_KEY_STRUCT.pack(height, block_hash)), undo_ops)
        finally:
            self._op_stack.clear()

    def rollback(self, height: int, block_hash: bytes):
        """
        Revert changes for a block height
        """
        undo_key = DB_PREFIXES.undo.value + self.UNDO_KEY_STRUCT.pack(height, block_hash)
        undo_c_f = self.column_families[DB_PREFIXES.undo.value]
        undo_info = self._db.get((undo_c_f, undo_key))
        self._op_stack.apply_packed_undo_ops(undo_info)
        try:
            with self._db.write_batch(sync=True) as batch:
                batch_put = batch.put
                batch_delete = batch.delete
                get_column_family = self.column_families.__getitem__
                for staged_change in self._op_stack:
                    column_family = get_column_family(DB_PREFIXES(staged_change.key[:1]).value)
                    if staged_change.is_put:
                        batch_put((column_family, staged_change.key), staged_change.value)
                    else:
                        batch_delete((column_family, staged_change.key))
                # batch_delete(undo_key)
        finally:
            self._op_stack.clear()

    def get(self, key: bytes, fill_cache: bool = True) -> Optional[bytes]:
        cf = self.column_families[key[:1]]
        return self._db.get((cf, key), fill_cache=fill_cache)

    def multi_get(self, keys: typing.List[bytes], fill_cache=True):
        first_key = keys[0]
        if not all(first_key[0] == key[0] for key in keys):
            raise ValueError('cannot multi-delete across column families')
        cf = self.column_families[first_key[:1]]
        db_result = self._db.multi_get([(cf, k) for k in keys], fill_cache=fill_cache)
        return list(db_result.values())

    def multi_delete(self, items: typing.List[typing.Tuple[bytes, bytes]]):
        self._op_stack.multi_delete([RevertableDelete(k, v) for k, v in items])

    def iterator(self, start: bytes, column_family: 'rocksdb.ColumnFamilyHandle' = None,
                 iterate_lower_bound: bytes = None, iterate_upper_bound: bytes = None,
                 reverse: bool = False, include_key: bool = True, include_value: bool = True,
                 fill_cache: bool = True, prefix_same_as_start: bool = False, auto_prefix_mode: bool = True):
        return self._db.iterator(
            start=start, column_family=column_family, iterate_lower_bound=iterate_lower_bound,
            iterate_upper_bound=iterate_upper_bound, reverse=reverse, include_key=include_key,
            include_value=include_value, fill_cache=fill_cache, prefix_same_as_start=prefix_same_as_start,
            auto_prefix_mode=auto_prefix_mode
        )

    def close(self):
        self._db.close()

    def try_catch_up_with_primary(self):
        self._db.try_catch_up_with_primary()

    def stage_raw_put(self, key: bytes, value: bytes):
        self._op_stack.append_op(RevertablePut(key, value))

    def stage_raw_delete(self, key: bytes, value: bytes):
        self._op_stack.append_op(RevertableDelete(key, value))

    def estimate_num_keys(self, column_family: 'rocksdb.ColumnFamilyHandle' = None):
        return int(self._db.get_property(b'rocksdb.estimate-num-keys', column_family).decode())
