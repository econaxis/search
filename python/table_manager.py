import codecs
import ctypes
from ctypes import Structure, POINTER, c_uint32, c_char_p, c_uint8
from typing import Union
import functools


class _TableManager(Structure):
    pass


class StrFatPtr(ctypes.Structure):
    _fields_ = [
        ("ptr", ctypes.c_void_p),
        ("len", ctypes.c_uint64)
    ]


def load_rust_lib(path):
    db = ctypes.cdll.LoadLibrary(f"{path}/libpythonlib.so")
    db.db1_store.argtypes = [POINTER(_TableManager), c_uint32, c_char_p, c_char_p]
    db.db1_get.argtypes = [POINTER(_TableManager), c_uint32, c_uint8]
    db.db1_get.restype = StrFatPtr
    db.db1_new.restype = POINTER(_TableManager)
    db.db1_new.argtypes = [c_char_p]
    return db


class TableManager:
    def __init__(self, path=b"/tmp/test.db"):
        self.tbm = DBDLL.db1_new(path)

    def store(self, id: int, name: Union[bytes, str], contents: Union[bytes, str]):
        def to_bytes(a):
            if type(a) != bytes:
                orig_len = len(a)
                a = codecs.encode(a, 'ascii', 'replace')
                assert orig_len == len(a)
            return a

        name = to_bytes(name)
        contents = to_bytes(contents)

        DBDLL.db1_store(self.tbm, id, name, contents)

    @functools.lru_cache(500, typed=True)
    def get(self, id: int, contents_offset: int = 0, len=None):
        namep = DBDLL.db1_get(self.tbm, id, 0)
        contentsp = DBDLL.db1_get(self.tbm, id, 1)

        if not namep or not contentsp:
            return None

        if not len:
            len = contentsp.len
        else:
            len = min(contentsp.len - contents_offset, len)

        print(namep.len, contentsp.len)
        name = ctypes.string_at(namep.ptr, namep.len)
        contents = ctypes.string_at(contentsp.ptr + contents_offset, size=len)
        return_type = codecs.decode(name, 'ascii'), codecs.decode(contents, 'ascii')

        return return_type

    def flush(self):
        DBDLL.db1_persist(self.tbm)


DBDLL = load_rust_lib("/home/henry/search/cmake-build-relwithdebinfo")
tbm = TableManager()
