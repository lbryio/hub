import struct
import logging
from string import printable
from collections import defaultdict, deque
from typing import Tuple, Iterable, Callable, Optional, List, Deque
from hub.db.common import DB_PREFIXES

_OP_STRUCT = struct.Struct('>BLL')
log = logging.getLogger(__name__)


class RevertableOp:
    __slots__ = [
        'key',
        'value',
    ]
    is_put = 0

    def __init__(self, key: bytes, value: bytes):
        self.key = key
        self.value = value

    @property
    def is_delete(self) -> bool:
        return not self.is_put

    def invert(self) -> 'RevertableOp':
        raise NotImplementedError()

    def pack(self) -> bytes:
        """
        Serialize to bytes
        """
        return struct.pack(
            f'>BLL{len(self.key)}s{len(self.value)}s', int(self.is_put), len(self.key), len(self.value), self.key,
            self.value
        )

    @classmethod
    def unpack(cls, packed: bytes) -> Tuple['RevertableOp', bytes]:
        """
        Deserialize from bytes

        :param packed: bytes containing at least one packed revertable op
        :return: tuple of the deserialized op (a put or a delete) and the remaining serialized bytes
        """
        is_put, key_len, val_len = _OP_STRUCT.unpack(packed[:9])
        key = packed[9:9 + key_len]
        value = packed[9 + key_len:9 + key_len + val_len]
        if is_put == 1:
            return RevertablePut(key, value), packed[9 + key_len + val_len:]
        return RevertableDelete(key, value), packed[9 + key_len + val_len:]

    def __eq__(self, other: 'RevertableOp') -> bool:
        return (self.is_put, self.key, self.value) == (other.is_put, other.key, other.value)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        from hub.db.prefixes import auto_decode_item
        k, v = auto_decode_item(self.key, self.value)
        key = ''.join(c if c in printable else '.' for c in str(k))
        val = ''.join(c if c in printable else '.' for c in str(v))
        return f"{'PUT' if self.is_put else 'DELETE'} {DB_PREFIXES(self.key[:1]).name}: {key} | {val}"


class RevertableDelete(RevertableOp):
    def invert(self):
        return RevertablePut(self.key, self.value)


class RevertablePut(RevertableOp):
    is_put = True

    def invert(self):
        return RevertableDelete(self.key, self.value)


class OpStackIntegrity(Exception):
    pass


class RevertableOpStack:
    def __init__(self, get_fn: Callable[[bytes], Optional[bytes]],
                 multi_get_fn: Callable[[List[bytes]], Iterable[Optional[bytes]]], unsafe_prefixes=None,
                 enforce_integrity=True):
        """
        This represents a sequence of revertable puts and deletes to a key-value database that checks for integrity
        violations when applying the puts and deletes. The integrity checks assure that keys that do not exist
        are not deleted, and that when keys are deleted the current value is correctly known so that the delete
        may be undone. When putting values, the integrity checks assure that existing values are not overwritten
        without first being deleted. Updates are performed by applying a delete op for the old value and a put op
        for the new value.

        :param get_fn: getter function from an object implementing `KeyValueStorage`
        :param unsafe_prefixes: optional set of prefixes to ignore integrity errors for, violations are still logged
        """
        self._get = get_fn
        self._multi_get = multi_get_fn
        # a defaultdict of verified ops ready to be applied
        self._items = defaultdict(list)
        # a faster deque of ops that have not yet been checked for integrity errors
        self._stash: Deque[RevertableOp] = deque()
        self._stashed_last_op_for_key = {}
        self._unsafe_prefixes = unsafe_prefixes or set()
        self._enforce_integrity = enforce_integrity

    def stash_ops(self, ops: Iterable[RevertableOp]):
        self._stash.extend(ops)
        for op in ops:
            self._stashed_last_op_for_key[op.key] = op

    def validate_and_apply_stashed_ops(self):
        if not self._stash:
            return

        ops_to_apply = []
        append_op_needed = ops_to_apply.append
        pop_staged_op = self._stash.popleft
        unique_keys = set()

        # nullify the ops that cancel against the most recent staged for a key
        while self._stash:
            op = pop_staged_op()
            if self._items[op.key] and op.invert() == self._items[op.key][-1]:
                self._items[op.key].pop()  # if the new op is the inverse of the last op, we can safely null both
                continue
            elif self._items[op.key] and self._items[op.key][-1] == op:  # duplicate of last op
                continue  # raise an error?
            else:
                append_op_needed(op)
                unique_keys.add(op.key)

        existing = {}
        if self._enforce_integrity and unique_keys:
            unique_keys = list(unique_keys)
            for idx in range(0, len(unique_keys), 10000):
                batch = unique_keys[idx:idx+10000]
                existing.update({
                    k: v for k, v in zip(batch, self._multi_get(batch))
                })

        for op in ops_to_apply:
            if op.key in self._items and len(self._items[op.key]) and self._items[op.key][-1] == op.invert():
                self._items[op.key].pop()
                if not self._items[op.key]:
                    self._items.pop(op.key)
                continue
            if not self._enforce_integrity:
                self._items[op.key].append(op)
                continue
            stored_val = existing[op.key]
            has_stored_val = stored_val is not None
            delete_stored_op = None if not has_stored_val else RevertableDelete(op.key, stored_val)
            will_delete_existing_stored = False if delete_stored_op is None else (delete_stored_op in self._items[op.key])
            try:
                if op.is_delete:
                    if has_stored_val and stored_val != op.value and not will_delete_existing_stored:
                        # there is a value and we're not deleting it in this op
                        # check that a delete for the stored value is in the stack
                        raise OpStackIntegrity(f"db op tries to delete with incorrect existing value {op}\nvs\n{stored_val}")
                    elif not stored_val:
                        raise OpStackIntegrity(f"db op tries to delete nonexistent key: {op}")
                    elif stored_val != op.value:
                        raise OpStackIntegrity(f"db op tries to delete with incorrect value: {op}")
                else:
                    if has_stored_val and not will_delete_existing_stored:
                        raise OpStackIntegrity(f"db op tries to overwrite before deleting existing: {op}")
                    if op.key in self._items and len(self._items[op.key]) and self._items[op.key][-1].is_put:
                        raise OpStackIntegrity(f"db op tries to overwrite with {op} before deleting pending "
                                               f"{self._items[op.key][-1]}")
            except OpStackIntegrity as err:
                if op.key[:1] in self._unsafe_prefixes:
                    log.debug(f"skipping over integrity error: {err}")
                else:
                    raise err
            self._items[op.key].append(op)

        self._stashed_last_op_for_key.clear()

    def append_op(self, op: RevertableOp):
        """
        Apply a put or delete op, checking that it introduces no integrity errors
        """

        inverted = op.invert()
        if self._items[op.key] and inverted == self._items[op.key][-1]:
            self._items[op.key].pop()  # if the new op is the inverse of the last op, we can safely null both
            return
        elif self._items[op.key] and self._items[op.key][-1] == op:  # duplicate of last op
            return  # raise an error?
        stored_val = self._get(op.key)
        has_stored_val = stored_val is not None
        delete_stored_op = None if not has_stored_val else RevertableDelete(op.key, stored_val)
        will_delete_existing_stored = False if delete_stored_op is None else (delete_stored_op in self._items[op.key])
        try:
            if op.is_put and has_stored_val and not will_delete_existing_stored:
                raise OpStackIntegrity(
                    f"db op tries to add on top of existing key without deleting first: {op}"
                )
            elif op.is_delete and has_stored_val and stored_val != op.value and not will_delete_existing_stored:
                # there is a value and we're not deleting it in this op
                # check that a delete for the stored value is in the stack
                raise OpStackIntegrity(f"db op tries to delete with incorrect existing value {op}")
            elif op.is_delete and not has_stored_val:
                raise OpStackIntegrity(f"db op tries to delete nonexistent key: {op}")
            elif op.is_delete and stored_val != op.value:
                raise OpStackIntegrity(f"db op tries to delete with incorrect value: {op}")
        except OpStackIntegrity as err:
            if op.key[:1] in self._unsafe_prefixes:
                log.debug(f"skipping over integrity error: {err}")
            else:
                raise err
        self._items[op.key].append(op)

    def multi_put(self, ops: List[RevertablePut]):
        """
        Apply a put or delete op, checking that it introduces no integrity errors
        """

        if not ops:
            return

        need_put = []

        if not all(op.is_put for op in ops):
            raise ValueError(f"list must contain only puts")
        if not len(set(map(lambda op: op.key, ops))) == len(ops):
            raise ValueError(f"list must contain unique keys")

        for op in ops:
            if self._items[op.key] and op.invert() == self._items[op.key][-1]:
                self._items[op.key].pop()  # if the new op is the inverse of the last op, we can safely null both
                continue
            elif self._items[op.key] and self._items[op.key][-1] == op:  # duplicate of last op
                continue  # raise an error?
            else:
                need_put.append(op)

        for op, stored_val in zip(need_put, self._multi_get(list(map(lambda item: item.key, need_put)))):
            has_stored_val = stored_val is not None
            delete_stored_op = None if not has_stored_val else RevertableDelete(op.key, stored_val)
            will_delete_existing_stored = False if delete_stored_op is None else (delete_stored_op in self._items[op.key])
            try:
                if has_stored_val and not will_delete_existing_stored:
                    raise OpStackIntegrity(f"db op tries to overwrite before deleting existing: {op}")
            except OpStackIntegrity as err:
                if op.key[:1] in self._unsafe_prefixes:
                    log.debug(f"skipping over integrity error: {err}")
                else:
                    raise err
            self._items[op.key].append(op)

    def multi_delete(self, ops: List[RevertableDelete]):
        """
        Apply a put or delete op, checking that it introduces no integrity errors
        """

        if not ops:
            return

        need_delete = []

        if not all(op.is_delete for op in ops):
            raise ValueError(f"list must contain only deletes")
        if not len(set(map(lambda op: op.key, ops))) == len(ops):
            raise ValueError(f"list must contain unique keys")

        for op in ops:
            if self._items[op.key] and op.invert() == self._items[op.key][-1]:
                self._items[op.key].pop()  # if the new op is the inverse of the last op, we can safely null both
                continue
            elif self._items[op.key] and self._items[op.key][-1] == op:  # duplicate of last op
                continue  # raise an error?
            else:
                need_delete.append(op)

        for op, stored_val in zip(need_delete, self._multi_get(list(map(lambda item: item.key, need_delete)))):
            has_stored_val = stored_val is not None
            delete_stored_op = None if not has_stored_val else RevertableDelete(op.key, stored_val)
            will_delete_existing_stored = False if delete_stored_op is None else (delete_stored_op in self._items[op.key])
            try:
                if op.is_delete and has_stored_val and stored_val != op.value and not will_delete_existing_stored:
                    # there is a value and we're not deleting it in this op
                    # check that a delete for the stored value is in the stack
                    raise OpStackIntegrity(f"db op tries to delete with incorrect existing value {op}")
                elif not stored_val:
                    raise OpStackIntegrity(f"db op tries to delete nonexistent key: {op}")
                elif op.is_delete and stored_val != op.value:
                    raise OpStackIntegrity(f"db op tries to delete with incorrect value: {op}")
            except OpStackIntegrity as err:
                if op.key[:1] in self._unsafe_prefixes:
                    log.debug(f"skipping over integrity error: {err}")
                else:
                    raise err
            self._items[op.key].append(op)

    def clear(self):
        self._items.clear()
        self._stash.clear()
        self._stashed_last_op_for_key.clear()

    def __len__(self):
        return sum(map(len, self._items.values()))

    def __iter__(self):
        for key, ops in self._items.items():
            for op in ops:
                yield op

    def __reversed__(self):
        for key, ops in self._items.items():
            for op in reversed(ops):
                yield op

    def get_undo_ops(self) -> bytes:
        """
        Get the serialized bytes to undo all of the changes made by the pending ops
        """
        return b''.join(op.invert().pack() for op in reversed(self))

    def apply_packed_undo_ops(self, packed: bytes):
        """
        Unpack and apply a sequence of undo ops from serialized undo bytes
        """
        while packed:
            op, packed = RevertableOp.unpack(packed)
            self.append_op(op)

    def get_pending_op(self, key: bytes) -> Optional[RevertableOp]:
        if key in self._stashed_last_op_for_key:
            return self._stashed_last_op_for_key[key]
        if key in self._items and self._items[key]:
            return self._items[key][-1]
