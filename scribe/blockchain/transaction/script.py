import typing
from scribe.blockchain.transaction import NameClaim, ClaimUpdate, ClaimSupport
from scribe.blockchain.transaction import unpack_le_uint16_from, unpack_le_uint32_from, pack_le_uint16, pack_le_uint32


class _OpCodes(typing.NamedTuple):
    def whatis(self, value: int):
        try:
            return self._fields[self.index(value)]
        except (ValueError, IndexError):
            return -1

    OP_PUSHDATA1: int = 0x4c
    OP_PUSHDATA2: int = 0x4d
    OP_PUSHDATA4: int = 0x4e
    OP_1NEGATE: int = 0x4f
    OP_RESERVED: int = 0x50
    OP_1: int = 0x51
    OP_2: int = 0x52
    OP_3: int = 0x53
    OP_4: int = 0x54
    OP_5: int = 0x55
    OP_6: int = 0x56
    OP_7: int = 0x57
    OP_8: int = 0x58
    OP_9: int = 0x59
    OP_10: int = 0x5a
    OP_11: int = 0x5b
    OP_12: int = 0x5c
    OP_13: int = 0x5d
    OP_14: int = 0x5e
    OP_15: int = 0x5f
    OP_16: int = 0x60
    OP_NOP: int = 0x61
    OP_VER: int = 0x62
    OP_IF: int = 0x63
    OP_NOTIF: int = 0x64
    OP_VERIF: int = 0x65
    OP_VERNOTIF: int = 0x66
    OP_ELSE: int = 0x67
    OP_ENDIF: int = 0x68
    OP_VERIFY: int = 0x69
    OP_RETURN: int = 0x6a
    OP_TOALTSTACK: int = 0x6b
    OP_FROMALTSTACK: int = 0x6c
    OP_2DROP: int = 0x6d
    OP_2DUP: int = 0x6e
    OP_3DUP: int = 0x6f
    OP_2OVER: int = 0x70
    OP_2ROT: int = 0x71
    OP_2SWAP: int = 0x72
    OP_IFDUP: int = 0x73
    OP_DEPTH: int = 0x74
    OP_DROP: int = 0x75
    OP_DUP: int = 0x76
    OP_NIP: int = 0x77
    OP_OVER: int = 0x78
    OP_PICK: int = 0x79
    OP_ROLL: int = 0x7a
    OP_ROT: int = 0x7b
    OP_SWAP: int = 0x7c
    OP_TUCK: int = 0x7d
    OP_CAT: int = 0x7e
    OP_SUBSTR: int = 0x7f
    OP_LEFT: int = 0x80
    OP_RIGHT: int = 0x81
    OP_SIZE: int = 0x82
    OP_INVERT: int = 0x83
    OP_AND: int = 0x84
    OP_OR: int = 0x85
    OP_XOR: int = 0x86
    OP_EQUAL: int = 0x87
    OP_EQUALVERIFY: int = 0x88
    OP_RESERVED1: int = 0x89
    OP_RESERVED2: int = 0x8a
    OP_1ADD: int = 0x8b
    OP_1SUB: int = 0x8c
    OP_2MUL: int = 0x8d
    OP_2DIV: int = 0x8e
    OP_NEGATE: int = 0x8f
    OP_ABS: int = 0x90
    OP_NOT: int = 0x91
    OP_0NOTEQUAL: int = 0x92
    OP_ADD: int = 0x93
    OP_SUB: int = 0x94
    OP_MUL: int = 0x95
    OP_DIV: int = 0x96
    OP_MOD: int = 0x97
    OP_LSHIFT: int = 0x98
    OP_RSHIFT: int = 0x99
    OP_BOOLAND: int = 0x9a
    OP_BOOLOR: int = 0x9b
    OP_NUMEQUAL: int = 0x9c
    OP_NUMEQUALVERIFY: int = 0x9d
    OP_NUMNOTEQUAL: int = 0x9e
    OP_LESSTHAN: int = 0x9f
    OP_GREATERTHAN: int = 0xa0
    OP_LESSTHANOREQUAL: int = 0xa1
    OP_GREATERTHANOREQUAL: int = 0xa2
    OP_MIN: int = 0xa3
    OP_MAX: int = 0xa4
    OP_WITHIN: int = 0xa5
    OP_RIPEMD160: int = 0xa6
    OP_SHA1: int = 0xa7
    OP_SHA256: int = 0xa8
    OP_HASH160: int = 0xa9
    OP_HASH256: int = 0xaa
    OP_CODESEPARATOR: int = 0xab
    OP_CHECKSIG: int = 0xac
    OP_CHECKSIGVERIFY: int = 0xad
    OP_CHECKMULTISIG: int = 0xae
    OP_CHECKMULTISIGVERIFY: int = 0xaf
    OP_NOP1: int = 0xb0
    OP_CHECKLOCKTIMEVERIFY: int = 0xb1
    OP_CHECKSEQUENCEVERIFY: int = 0xb2
    OP_NOP4: int = 0xb3
    OP_NOP5: int = 0xb4
    OP_CLAIM_NAME: int = 0xb5
    OP_SUPPORT_CLAIM: int = 0xb6
    OP_UPDATE_CLAIM: int = 0xb7
    OP_NOP9: int = 0xb8
    OP_NOP10: int = 0xb9


OpCodes = _OpCodes()


# Paranoia to make it hard to create bad scripts
assert OpCodes.OP_DUP == 0x76
assert OpCodes.OP_HASH160 == 0xa9
assert OpCodes.OP_EQUAL == 0x87
assert OpCodes.OP_EQUALVERIFY == 0x88
assert OpCodes.OP_CHECKSIG == 0xac
assert OpCodes.OP_CHECKMULTISIG == 0xae

assert OpCodes.OP_CLAIM_NAME == 0xb5
assert OpCodes.OP_SUPPORT_CLAIM == 0xb6
assert OpCodes.OP_UPDATE_CLAIM == 0xb7


def P2SH_script(hash160_bytes: bytes):
    return bytes([OpCodes.OP_HASH160]) + script_push_data(hash160_bytes) + bytes([OpCodes.OP_EQUAL])


def P2PKH_script(hash160_bytes: bytes):
    return (bytes([OpCodes.OP_DUP, OpCodes.OP_HASH160])
            + script_push_data(hash160_bytes)
            + bytes([OpCodes.OP_EQUALVERIFY, OpCodes.OP_CHECKSIG]))


def script_push_data(data: bytes):
    n = len(data)
    if n < OpCodes.OP_PUSHDATA1:
        return bytes([n]) + data
    if n < 256:
        return bytes([OpCodes.OP_PUSHDATA1, n]) + data
    if n < 65536:
        return bytes([OpCodes.OP_PUSHDATA2]) + pack_le_uint16(n) + data
    return bytes([OpCodes.OP_PUSHDATA4]) + pack_le_uint32(n) + data


def script_GetOp(script_bytes: bytes):
    i = 0
    while i < len(script_bytes):
        vch = None
        opcode = script_bytes[i]
        i += 1
        if opcode <= OpCodes.OP_PUSHDATA4:
            n_size = opcode
            if opcode == OpCodes.OP_PUSHDATA1:
                n_size = script_bytes[i]
                i += 1
            elif opcode == OpCodes.OP_PUSHDATA2:
                (n_size,) = unpack_le_uint16_from(script_bytes, i)
                i += 2
            elif opcode == OpCodes.OP_PUSHDATA4:
                (n_size,) = unpack_le_uint32_from(script_bytes, i)
                i += 4
            if i + n_size > len(script_bytes):
                vch = b"_INVALID_" + script_bytes[i:]
                i = len(script_bytes)
            else:
                vch = script_bytes[i:i + n_size]
                i += n_size
        yield opcode, vch, i


_SCRIPT_TEMPLATES = (
    # claim related templates
    (OpCodes.OP_CLAIM_NAME, -1, -1, OpCodes.OP_2DROP, OpCodes.OP_DROP),
    (OpCodes.OP_UPDATE_CLAIM, -1, -1, OpCodes.OP_2DROP, OpCodes.OP_DROP),
    (OpCodes.OP_UPDATE_CLAIM, -1, -1, -1, OpCodes.OP_2DROP, OpCodes.OP_2DROP),
    (OpCodes.OP_SUPPORT_CLAIM, -1, -1, OpCodes.OP_2DROP, OpCodes.OP_DROP),
    (OpCodes.OP_SUPPORT_CLAIM, -1, -1, -1, OpCodes.OP_2DROP, OpCodes.OP_2DROP),

    # receive script templates
    (OpCodes.OP_DUP, OpCodes.OP_HASH160, -1, OpCodes.OP_EQUALVERIFY, OpCodes.OP_CHECKSIG),
    (OpCodes.OP_HASH160, -1, OpCodes.OP_EQUAL),
    (-1, OpCodes.OP_CHECKSIG)
)
_CLAIM_TEMPLATE = 0
_UPDATE_NO_DATA_TEMPLATE = 1
_UPDATE_TEMPLATE = 2
_SUPPORT_TEMPLATE = 3
_SUPPORT_WITH_DATA_TEMPLATE = 4

_TO_ADDRESS_TEMPLATE = 5
_TO_P2SH_TEMPLATE = 6
_TO_PUBKEY_TEMPLATE = 7


def txo_script_parser(script: bytes):
    template = None
    template_idx = None
    values = []
    receive_values = []
    finished_decoding_claim = False
    receive_cur = 0
    claim, support, pubkey_hash, pubkey, script_hash = None, None, None, None, None
    for cur, (op, data, _) in enumerate(script_GetOp(script)):
        if finished_decoding_claim:  # we're decoding the receiving part of the script (the last part)
            if receive_cur == 0:
                if op == OpCodes.OP_DUP:
                    template_idx = _TO_ADDRESS_TEMPLATE
                elif op == OpCodes.OP_HASH160:
                    template_idx = _TO_P2SH_TEMPLATE
                elif op == -1:
                    template_idx = _TO_PUBKEY_TEMPLATE
                else:
                    break  # return the decoded part
                template = _SCRIPT_TEMPLATES[template_idx]
            expected = template[receive_cur]
            if expected == -1 and data is None:  # if data data is expected make sure it's there
                # print("\texpected data", OpCodes.whatis(op), data)
                return
            elif expected == -1 and data:
                receive_values.append(data)
            elif op != expected:
                # print("\top mismatch")
                return
            receive_cur += 1
            continue

        if cur == 0:  # initialize the template
            if op == OpCodes.OP_CLAIM_NAME:
                template_idx = _CLAIM_TEMPLATE
            elif op == OpCodes.OP_UPDATE_CLAIM:
                template_idx = _UPDATE_NO_DATA_TEMPLATE
            elif op == OpCodes.OP_SUPPORT_CLAIM:
                template_idx = _SUPPORT_TEMPLATE  # could be a support w/ data
            elif op == OpCodes.OP_DUP:
                template_idx = _TO_ADDRESS_TEMPLATE
            elif op == OpCodes.OP_HASH160:
                template_idx = _TO_P2SH_TEMPLATE
            elif op == -1:
                template_idx = _TO_PUBKEY_TEMPLATE
            else:
                return
            template = _SCRIPT_TEMPLATES[template_idx]
        elif cur == 3 and template_idx == _SUPPORT_TEMPLATE and data:
            template_idx = _SUPPORT_WITH_DATA_TEMPLATE
            template = _SCRIPT_TEMPLATES[template_idx]
        elif cur == 3 and template_idx == _UPDATE_NO_DATA_TEMPLATE and data:
            template_idx = _UPDATE_TEMPLATE
            template = _SCRIPT_TEMPLATES[template_idx]

        if cur >= len(template):
            return

        expected = template[cur]

        if expected == -1 and data is None:  # if data data is expected make sure it's there
            # print("\texpected data", OpCodes.whatis(op), data)
            return
        elif expected == -1 and data:
            if template_idx in (_TO_ADDRESS_TEMPLATE, _TO_P2SH_TEMPLATE, _TO_ADDRESS_TEMPLATE):
                receive_values.append(data)
            else:
                values.append(data)
        elif op != expected:
            # print("\top mismatch")
            return
        if cur + 1 == len(template):
            finished_decoding_claim = True
            if template_idx == _CLAIM_TEMPLATE:
                claim = NameClaim(*values)
            elif template_idx in (_UPDATE_NO_DATA_TEMPLATE, _UPDATE_TEMPLATE):
                claim = ClaimUpdate(*values)
            elif template_idx in (_SUPPORT_TEMPLATE, _SUPPORT_WITH_DATA_TEMPLATE):
                support = ClaimSupport(*values)

    if template_idx == _TO_ADDRESS_TEMPLATE:
        pubkey_hash = receive_values[0]
    elif template_idx == _TO_P2SH_TEMPLATE:
        script_hash = receive_values[0]
    elif template_idx == _TO_PUBKEY_TEMPLATE:
        pubkey = receive_values[0]
    return claim, support, pubkey_hash, script_hash, pubkey
