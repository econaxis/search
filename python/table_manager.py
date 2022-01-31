import base64
import codecs
import ctypes
import dataclasses
import functools
import json
from ctypes import Structure, POINTER, c_uint32, c_char_p, c_uint8
from typing import Union


#
# def load_rust_lib(path):
#     db = ctypes.cdll.LoadLibrary(f"{path}/libpythonlib.so")
#     db.db1_store.argtypes = [POINTER(_TableManager), c_uint32, c_char_p, c_char_p]
#     db.db1_get.argtypes = [POINTER(_TableManager), c_uint32, c_uint8]
#     db.db1_get.restype = StrFatPtr
#     db.db1_new.restype = POINTER(_TableManager)
#     db.db1_new.argtypes = [c_char_p]
#     return db
# 

class DynamicTableC(Structure):
    pass


DB = None


def load_rust_lib(path="/home/henry/db1/target/release/libdb2.so"):
    global DB
    DB = ctypes.cdll.LoadLibrary(path)
    DB.sql_exec.argtypes = [POINTER(DynamicTableC), c_char_p]
    DB.sql_exec.restype = c_char_p
    DB.sql_new.argtypes = [c_char_p]
    DB.sql_new.restype = POINTER(DynamicTableC)


load_rust_lib()


@dataclasses.dataclass
class SearchResult:
    id: int
    filename: str
    data: bytes


DB = ctypes.cdll.LoadLibrary("/home/henry/db1/target/release/libdb2.so")
DB.sql_exec.argtypes = [POINTER(DynamicTableC), c_char_p]
DB.sql_exec.restype = c_char_p
DB.sql_new.argtypes = [c_char_p]
DB.sql_new.restype = POINTER(DynamicTableC)
import os


class TableManager:
    def __init__(self, path):
        exists = os.path.exists(path)
        if isinstance(path, str):
            path = path.encode('ascii')
        self.db = DB.sql_new(path)
        self.path = path
        if not exists:
            print("Inserting new table")
            DB.sql_exec(self.db, b"CREATE TABLE search (id INT, url STRING, contents STRING)")
            DB.sql_exec(self.db, b"FLUSH")

    def reload(self):
        self.flush()
        self.__init__(self.path)

    def flush(self):
        self.exec_sql("FLUSH")

    @classmethod
    def to_str(cls, a, b85):
        if b85:
            # if isinstance(a, str):
            #     a = a.encode('ascii', errors='ignore')
            a = a.replace('"', '')
            a = a.replace('\\', '')
            if isinstance(a, bytes):
                return a.decode('ascii', errors='ignore')
            else:
                return a.encode('ascii', errors='ignore').decode('ascii')
            # return base64.b85encode(a).decode('ascii')
        else:
            if type(a) == bytes:
                a = a.decode('ascii')
            return a

    def store(self, id: int, url, contents: Union[bytes, str]):
        contents = TableManager.to_str(contents, True)
        url = TableManager.to_str(url, True)

        DB.sql_exec(self.db, f'INSERT INTO search VALUES ({id}, "{url}", "{contents}")'.encode('ascii'))

    def exec_sql(self, q: str):
        ret = DB.sql_exec(self.db, q.encode('ascii'))
        if ret:
            ret = json.loads(ret, strict = False)
        return ret

    def process_list(self, li):
        retli = {}
        for i in li:
            if i[0] not in retli:
                retli[i[0]] = SearchResult(i[0], i[1], i[2])

        return list(retli.values())

    def get(self, id: int):
        print("Getting ", id)
        ret = self.exec_sql(f'SELECT id, url, contents FROM search WHERE id EQUALS {id}')
        return self.process_list(ret)[0]


tbm = TableManager(b"/tmp/wikibooks.db")

# os.remove("/tmp/tablem2.db")
# t2 = TableManager(b"/tmp/tablem2.db")
# for i in range(300):
#     t2.store(i, f"hello{i}", f"world{i}", f"fdkjsl; fvz")
# print("Result", t2.exec_sql("SELECT * FROM images"))
# exit(0)
