import ctypes
from ctypes import POINTER, c_char_p, c_uint32, c_uint8, Structure
import os
from typing import Iterable
from queue import Queue
from threading import Thread
import json
import codecs

from table_manager import tbm

os.environ["DATA_FILES_DIR"] = "/home/henry/search/data"


class SortedKeysIndexStub(Structure):
    pass


class SortedKeysIndex(Structure):
    pass


class DocumentFrequency(Structure):
    _fields_ = [
        ("document_id", c_uint32),
        ("document_freq", c_uint32)
    ]

    def resolve_id(self) -> str:
        return tbm.get(int(self.document_id))[0]

    def __rich__(self):
        return f"{self.resolve_id()} ({self.document_freq} score)"


class FoundPositions(Structure):
    _fields_ = [
        ("terms_index", c_uint8),
        ("document_id", c_uint32),
        ("document_position", c_uint32),
    ]

    def __rich__(self):
        return f"{self.terms_index + 1}th term {self.document_id} @ {self.document_position}"


def limit5(iterator):
    limit = 5
    while limit > 0:
        limit -= 1
        yield next(iterator)


class _SearchRetType(Structure):
    _fields_ = [
        ("topdocs", POINTER(DocumentFrequency)),
        ("topdocs_length", c_uint32),
        ("pos", POINTER(FoundPositions)),
        ("pos_len", c_uint32)
    ]

    def iter_positions(self) -> Iterable[POINTER(FoundPositions)]:
        for i in range(0, self.pos_len):
            yield self.pos[i]

    def iter_td(self, limit=20) -> Iterable[POINTER(DocumentFrequency)]:
        for i in list(reversed(range(0, self.topdocs_length)))[0:limit]:
            yield self.topdocs[i]

    def __rich__(self):
        td_str = "\n".join([self.topdocs[i].__rich__() for i in limit5(reversed(range(0, self.topdocs_length)))])

        pos_str = "\n".join([self.pos[i].__rich__() for i in range(0, self.pos_len)])
        return td_str + "\n" + pos_str


class SearchRetType:
    def __init__(self, dll, sr: _SearchRetType, terms: [bytes]):
        td = {}
        scores = {}
        td_terms_count = {}
        for i in sr.iter_td():
            td[int(i.document_id)] = []
            scores[int(i.document_id)] = i.document_freq
            td_terms_count[int(i.document_id)] = [0] * len(terms)

        for i in sr.iter_positions():
            id = i.document_id
            if id not in td:
                continue

            td[id].append([i.document_position, i.terms_index])

        json = {}
        matches = {}

        for i in reversed(sorted(td, key=lambda i: scores[i])):
            td[i] = sorted(td[i])
            if len(td[i]) == 0:
                continue
            new = [td[i][0]]
            for pos, length in td[i][1:]:
                if pos - new[-1][0] - new[-1][1] < 30:
                    new[-1][1] = length + pos - new[-1][0]
                else:
                    new.append([pos, length])

            new = sorted(new, key=lambda k: k[1])
            joined_str = ""
            sr = tbm.get(i)
            url = sr.filename
            contents = sr.data

            matches[i] = new
            for pos, length in new[-6:]:
                joined_str += "..." if len(joined_str) != 0 else ""
                joined_str += f"{contents[pos:pos + length]}"

            json[i] = (url, joined_str)

        scores = list(reversed(sorted(scores.keys(), key=lambda k: scores[k])))
        self.td = {"text": json, "matches": matches, "scores": scores}

    def printable(self):
        return json.dumps(self.td)

    def __repr__(self):
        return self.printable()
def load(path):
    indexer = ctypes.cdll.LoadLibrary(f"{path}/libgeneral-indexer.so")
    indexer.initialize_directory_variables(0)
    indexer.new_index.argtypes = []
    indexer.new_index.restype = POINTER(SortedKeysIndex)

    indexer.append_file.argtypes = [POINTER(SortedKeysIndex), c_char_p, c_uint32]
    indexer.append_file.restype = None

    indexer.persist_indices.argtypes = [POINTER(SortedKeysIndex), c_char_p]

    indexer.search_many_terms.argtypes = [POINTER(SortedKeysIndexStub), POINTER(c_char_p), c_uint32]

    indexer.search_many_terms.restype = _SearchRetType

    indexer.create_index_stub.restype = POINTER(SortedKeysIndexStub)
    indexer.clean.argtypes = [POINTER(SortedKeysIndex)]
    return indexer


DLL = load("/home/henry/search/cmake-build-relwithdebinfo")


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

        args = list(map(lambda k: bytes(k, 'ascii'), args))
        terms = (c_char_p * terms_len)(*args)

        terms = ctypes.cast(terms, POINTER(c_char_p))

        result = self.dll.search_many_terms(self.ind, terms, terms_len)

        print("Len: ", result.topdocs_length)

        return SearchRetType(self.dll, result, terms[0:terms_len])


class Indexer:
    def __init__(self):
        self.dll = DLL

    def __enter__(self):
        self.ind = self.dll.new_index()
        return self

    def append_file(self, contents: str, id: int):
        if type(contents) != bytes:
            orig_len = len(contents)
            contents = codecs.encode(contents, 'ascii', 'replace')
            assert orig_len == len(contents)
        self.dll.append_file(self.ind, contents, id)

    def persist(self, suffix: str):
        self.dll.persist_indices(self.ind, bytes(suffix, 'ascii'))

    def concat(self, other):
        self.dll.concat_indices(self.ind, other.ind)
        other.ind = None
        other.dll = None

    def clean(self):
        self.dll.clean(self.ind)

    def address(self):
        return self.ind

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ParallelIndexer:
    def thread_run(self, index: Indexer):
        def inner():
            while True:
                item = self.queue.get()
                if item == "exit":
                    index.clean()
                    return

                index.append_file(item[0], item[1])

        return inner

    def __init__(self, num_t=15, name="par-index"):
        self.num_t = num_t
        self.name = name
        self.count = 0

    def __enter__(self):
        self.queue = Queue(50)
        self.indices = []
        for _ in range(0, self.num_t):
            ind = Indexer()
            ind.__enter__()
            self.indices.append(ind)

        self.threads = [Thread(target=self.thread_run(i)) for i in self.indices]
        [x.start() for x in self.threads]
        return self

    def append_file(self, contents: str, id: int):
        self.queue.put((contents, id))
        self.count += 1
        if self.count % 500 == 0:
            print(self.count)

    def end(self) -> str:
        for _ in self.threads:
            self.queue.put("exit")
            self.queue.put("exit")

        for t in self.threads:
            t.join()

        print("Merging")
        for t in self.indices[1:]:
            print("Merging")
            self.indices[0].concat(t)

        self.indices[0].persist(self.name)
        return self.name

    def __exit__(self, exc_type, exc_val, exc_tb):
        [i.__exit__(exc_type, exc_val, exc_tb) for i in self.indices]
