from hub.common import double_sha256
from hub.blockchain.transaction import (
    unpack_le_int32_from, unpack_le_int64_from, unpack_le_uint16_from,
    unpack_le_uint32_from, unpack_le_uint64_from, Tx, TxInput, TxOutput
)
from hub.blockchain.transaction.script import txo_script_parser


class Deserializer:
    """Deserializes blocks into transactions.

    External entry points are read_tx(), read_tx_and_hash(),
    read_tx_and_vsize() and read_block().

    This code is performance sensitive as it is executed 100s of
    millions of times during sync.
    """

    TX_HASH_FN = staticmethod(double_sha256)

    def __init__(self, binary, start=0):
        assert isinstance(binary, bytes), f"type {type(binary)} is not 'bytes'"
        self.binary = binary
        self.binary_length = len(binary)
        self.cursor = start
        self.flags = 0

    def _read_witness(self, fields):
        read_witness_field = self._read_witness_field
        return [read_witness_field() for i in range(fields)]

    def _read_witness_field(self):
        read_varbytes = self._read_varbytes
        return [read_varbytes() for i in range(self._read_varint())]

    def _read_tx_parts(self):
        """Return a (deserialized TX, tx_hash, vsize) tuple."""
        start = self.cursor
        marker = self.binary[self.cursor + 4]
        if marker:
            tx = Tx(
                self._read_le_int32(),  # version
                self._read_inputs(),  # inputs
                self._read_outputs(),  # outputs
                self._read_le_uint32(),  # locktime
                self.binary[start:self.cursor],
            )
            tx_hash = self.TX_HASH_FN(self.binary[start:self.cursor])
            return tx, tx_hash, self.binary_length

        # Ugh, this is nasty.
        version = self._read_le_int32()
        orig_ser = self.binary[start:self.cursor]

        marker = self._read_byte()
        flag = self._read_byte()

        start = self.cursor
        inputs = self._read_inputs()
        outputs = self._read_outputs()
        orig_ser += self.binary[start:self.cursor]

        base_size = self.cursor - start
        witness = self._read_witness(len(inputs))

        start = self.cursor
        locktime = self._read_le_uint32()
        orig_ser += self.binary[start:self.cursor]
        vsize = (3 * base_size + self.binary_length) // 4

        return Tx(version, inputs, outputs, locktime, orig_ser, marker, flag, witness), self.TX_HASH_FN(orig_ser), vsize

    def read_tx(self):
        return self._read_tx_parts()[0]

    def read_tx_and_hash(self):
        tx, tx_hash, vsize = self._read_tx_parts()
        return tx, tx_hash

    def read_tx_and_vsize(self):
        tx, tx_hash, vsize = self._read_tx_parts()
        return tx, vsize

    def read_tx_block(self):
        """Returns a list of (deserialized_tx, tx_hash) pairs."""
        read = self.read_tx_and_hash
        # Some coins have excess data beyond the end of the transactions
        return [read() for _ in range(self._read_varint())]

    def _read_inputs(self):
        read_input = self._read_input
        return [read_input() for i in range(self._read_varint())]

    def _read_input(self):
        return TxInput(
            self._read_nbytes(32),   # prev_hash
            self._read_le_uint32(),  # prev_idx
            self._read_varbytes(),   # script
            self._read_le_uint32()   # sequence
        )

    def _read_outputs(self):
        read_output = self._read_output
        return [read_output(n) for n in range(self._read_varint())]

    def _read_output(self, n):
        value = self._read_le_int64()
        script = self._read_varbytes()  # pk_script
        decoded = txo_script_parser(script)
        claim = support = pubkey_hash = script_hash = pubkey = None
        if decoded:
            claim, support, pubkey_hash, script_hash, pubkey = decoded
        return TxOutput(n, value, script, claim, support, pubkey_hash, script_hash, pubkey)

    def _read_byte(self):
        cursor = self.cursor
        self.cursor += 1
        return self.binary[cursor]

    def _read_nbytes(self, n):
        cursor = self.cursor
        self.cursor = end = cursor + n
        assert self.binary_length >= end
        return self.binary[cursor:end]

    def _read_varbytes(self):
        return self._read_nbytes(self._read_varint())

    def _read_varint(self):
        n = self.binary[self.cursor]
        self.cursor += 1
        if n < 253:
            return n
        if n == 253:
            return self._read_le_uint16()
        if n == 254:
            return self._read_le_uint32()
        return self._read_le_uint64()

    def _read_le_int32(self):
        result, = unpack_le_int32_from(self.binary, self.cursor)
        self.cursor += 4
        return result

    def _read_le_int64(self):
        result, = unpack_le_int64_from(self.binary, self.cursor)
        self.cursor += 8
        return result

    def _read_le_uint16(self):
        result, = unpack_le_uint16_from(self.binary, self.cursor)
        self.cursor += 2
        return result

    def _read_le_uint32(self):
        result, = unpack_le_uint32_from(self.binary, self.cursor)
        self.cursor += 4
        return result

    def _read_le_uint64(self):
        result, = unpack_le_uint64_from(self.binary, self.cursor)
        self.cursor += 8
        return result
