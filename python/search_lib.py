import ctypes
import sqlite3
from ctypes import POINTER, c_char_p, c_uint32, c_uint8

import os

os.environ["DATA_FILES_DIR"] = "/home/henry/search/data"


class SortedKeysIndexStub(ctypes.Structure):
    pass


class DocumentFrequency(ctypes.Structure):
    _fields_ = [
        ("document_id", c_uint32),
        ("document_freq", c_uint32)
    ]
    conn = sqlite3.connect("output.db")

    def resolve_id(self, cur: sqlite3.Cursor) -> str:
        cur.execute(f"SELECT url FROM data WHERE id = ?", (int(self.document_id),))
        return cur.fetchone()

    def __rich__(self):
        return f"{self.resolve_id(self.conn.cursor())} ({self.document_freq} score)"


class FoundPositions(ctypes.Structure):
    _fields_ = [
        ("terms_index", c_uint8),
        ("document_id", c_uint32),
        ("document_position", c_uint32),
    ]

    def __rich__(self):
        return f"{self.terms_index + 1}th term {self.document_id} @ {self.document_position}"


class _SearchRetType(ctypes.Structure):
    _fields_ = [
        ("topdocs", POINTER(DocumentFrequency)),
        ("topdocs_length", c_uint32),
        ("pos", POINTER(FoundPositions)),
        ("pos_len", c_uint32)
    ]

    def __rich__(self):
        td_str = "\n".join([self.topdocs[i].__rich__() for i in reversed(range(0, self.topdocs_length))])

        pos_str = "\n".join([self.pos[i].__rich__() for i in range(0, self.pos_len)])
        return td_str + pos_str


class SearchRetType:
    def __init__(self, dll, sr: _SearchRetType):
        self._sr = sr
        self.dll = dll

    def __enter__(self):
        return self._sr

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dll.free_elem_buf(self._sr)


def load(path):
    indexer = ctypes.cdll.LoadLibrary(f"{path}/libgeneral-indexer.so")
    indexer.initialize_directory_variables()
    indexer.new_index.argtypes = []
    indexer.new_index.restype = POINTER(SortedKeysIndexStub)

    indexer.append_file.argtypes = [POINTER(SortedKeysIndexStub), c_char_p, c_uint32]
    indexer.append_file.restype = None

    indexer.persist_indices.argtypes = [POINTER(SortedKeysIndexStub), c_char_p]

    indexer.search_many_terms.argtypes = [POINTER(SortedKeysIndexStub), POINTER(c_char_p), c_uint32]

    indexer.search_many_terms.restype = _SearchRetType

    indexer.create_index_stub.restype = POINTER(SortedKeysIndexStub)
    return indexer


DLL = load("/home/henry/search/cmake-build-debug")


class Searcher:
    def __init__(self, suffix: str):
        self.dll = DLL
        self.ind = self.dll.create_index_stub(bytes(suffix, 'ascii'))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dll.free_index(self.ind)

    def search_terms(self, *args):
        terms_len = len(args)

        args = list(map(lambda k: bytes(k, 'ascii').upper(), args))
        terms = (c_char_p * terms_len)(*args)

        terms = ctypes.cast(terms, POINTER(c_char_p))

        result = self.dll.search_many_terms(self.ind, terms, terms_len)

        return SearchRetType(self.dll, result)


class Indexer:
    def __init__(self):
        self.dll = DLL

    def __enter__(self):
        self.ind = self.dll.new_index()
        return self

    def append_file(self, contents: str, id: int):
        if type(contents) != bytes:
            contents = bytes(contents, 'ascii')
        self.dll.append_file(self.ind, contents, id)

    def persist(self, suffix: str):
        self.dll.persist_indices(self.ind, bytes(suffix, 'ascii'))

    def concat(self, other):
        self.dll.concat_indices(self.ind, other.ind)
        other.ind = None
        other.dll = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


from queue import SimpleQueue
from collections import deque

from threading import Thread


class ParallelIndexer:
    def thread_run(self):
        with Indexer() as index:

            while True:
                item = self.queue.get()
                if item == "exit":
                    index.persist(self.names.pop())
                    return

                index.append_file(item[0], item[1])

    def __init__(self, num_t=1, name="par-index"):
        self.queue = SimpleQueue()
        self.threads = [Thread(target=lambda: self.thread_run()) for _ in range(0, num_t)]
        [x.start() for x in self.threads]
        self.names = deque([f"{name}-{i}" for i in range(0, num_t)])

    def append_file(self, contents: str, id: int):
        self.queue.put((contents, id))

    def end(self):
        for _ in self.threads:
            self.queue.put("exit")
            self.queue.put("exit")

        for t in self.threads:
            t.join()
