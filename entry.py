import struct
import base58
import logging

logger = logging.getLogger(__name__)


def data_entry_from_base58_str(str_object):
    base58_str = base58.b58decode(str_object)
    return data_entries_from_bytes(base58_str)


def data_entries_from_bytes(bytes_object):
    length = struct.unpack(">H", bytes_object[0:2])[0]
    all_data = []
    pos_drift = 2
    for pos in range(length):
        [array_info, pos_drift] = parse_data_entry_array_size(bytes_object, pos_drift)
        all_data.append(array_info)
    return all_data


def parse_data_entry_array_size(bytes_object, start_position):
    if bytes_object[start_position: start_position + 1] == Type.public_key:
        return (data_entry_from_bytes(bytes_object[start_position:start_position + Type.key_length + 1]),
                start_position + Type.key_length + 1)
    elif bytes_object[start_position: start_position + 1] == Type.address:
        return (data_entry_from_bytes(bytes_object[start_position:start_position + Type.address_length + 1]),
                start_position + Type.address_length + 1)
    elif bytes_object[start_position: start_position + 1] == Type.amount:
        return (data_entry_from_bytes(bytes_object[start_position:start_position + Type.amount_length + 1]),
                start_position + Type.amount_length + 1)
    elif bytes_object[start_position: start_position + 1] == Type.int32:
        return (data_entry_from_bytes(bytes_object[start_position:start_position + Type.int32_length + 1]),
                start_position + Type.int32_length + 1)
    elif bytes_object[start_position: start_position + 1] == Type.short_text:
        return (data_entry_from_bytes(bytes_object[start_position:start_position + struct.unpack(">H", bytes_object[start_position + 1:start_position + 3])[0] + 3]),
                start_position + struct.unpack(">H", bytes_object[start_position + 1: start_position + 3])[0] + 3)
    elif bytes_object[start_position: start_position + 1] == Type.contract_account:
        return (data_entry_from_bytes(bytes_object[start_position:start_position + Type.address_length + 1]),
                start_position + Type.address_length + 1)


def data_entry_from_bytes(bytes_object):
    if len(bytes_object) == 0:
        raise ValueError("Invalid DataEntry %s" % str(bytes_object))
    elif bytes_object[0:1] == Type.public_key:
        return DataEntry(bytes2str(base58.b58encode(bytes_object[1:])), bytes_object[0:1])
    elif bytes_object[0:1] == Type.address:
        return DataEntry(bytes2str(base58.b58encode(bytes_object[1:])), bytes_object[0:1])
    elif bytes_object[0:1] == Type.amount:
        return DataEntry(struct.unpack(">Q", bytes_object[1:])[0], bytes_object[0:1])
    elif bytes_object[0:1] == Type.int32:
        return DataEntry(struct.unpack(">I", bytes_object[1:])[0], bytes_object[0:1])
    elif bytes_object[0:1] == Type.short_text:
        return DataEntry(bytes2str(bytes_object[3:]), bytes_object[0:1])
    elif bytes_object[0:1] == Type.contract_account:
        return DataEntry(bytes2str(base58.b58encode(bytes_object[1:])), bytes_object[0:1])


bytes2str = lambda b: ''.join(map(chr, b))
list2bytes = lambda s: b''.join(s)
str2bytes = lambda s: s.encode('latin-1')


def check_data_type(data, data_type):
    if data_type == Type.public_key:
        data_bytes = base58.b58decode(data)
        return len(data_bytes) == Type.key_length
    elif data_type == Type.address:
        data_bytes = base58.b58decode(data)
        return len(data_bytes) == Type.address_length
    elif data_type == Type.amount:
        data_bytes = struct.pack(">Q", data)
        return len(data_bytes) == Type.amount_length and struct.unpack(">Q", data_bytes)[0] > 0
    elif data_type == Type.int32:
        data_bytes = struct.pack(">I", data)
        return len(data_bytes) == Type.amount_length and struct.unpack(">I", data_bytes)[0] > 0
    elif data_type == Type.short_text:
        data_bytes = serialize_array(str2bytes(data))
        return struct.unpack(">H", data_bytes[0:2])[0] + 2 == len(data_bytes) and len(
            data_bytes) <= Type.max_short_text_size + 2
    else:
        return True


def serialize_array(b):
    if type(b) is list:
        b_bytes = list2bytes(b)
        return struct.pack(">H", len(b)) + b_bytes
    else:
        return struct.pack(">H", len(b)) + b


class DataEntry:
    def __init__(self, data, data_type):
        if not check_data_type(data, data_type):
            raise ValueError("Invalid DataEntry data: %s, type: %s" % (str(data), str(data_type)))
        if data_type == Type.public_key:
            self.data_bytes = base58.b58decode(data)
            self.data_type = 'public_key'
        elif data_type == Type.address:
            self.data_bytes = base58.b58decode(data)
            self.data_type = 'address'
        elif data_type == Type.amount:
            self.data_bytes = struct.pack(">Q", data)
            self.data_type = 'amount'
        elif data_type == Type.int32:
            self.data_bytes = struct.pack(">I", data)
            self.data_type = 'int32'
        elif data_type == Type.short_text:
            self.data_bytes = serialize_array(str2bytes(data))
            self.data_type = 'short_text'
        elif data_type == Type.contract_account:
            self.data_bytes = base58.b58decode(data)
            self.data_type = 'contract_account'
        self.data = data
        self.bytes = data_type + self.data_bytes


class Type:
    public_key = struct.pack(">B", 1)
    key_length = 32
    address = struct.pack(">B", 2)
    address_length = 26
    amount = struct.pack(">B", 3)
    amount_length = 8
    int32 = struct.pack(">B", 4)
    int32_length = 4
    short_text = struct.pack(">B", 5)
    max_short_text_size = 140
    contract_account = struct.pack(">B", 6)
    contract_account_length = 26
    account = struct.pack(">B", 7)
