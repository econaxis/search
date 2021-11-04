import ctypes
from ctypes import POINTER, c_char_p, c_uint32


class SortedKeysIndexStub(ctypes.Structure):
    pass


class TDWPElem(ctypes.Structure):
    _fields_ = [("document_id", c_uint32), ("document_freq", c_uint32), ("matches", c_uint32 * 4)]

    def __repr__(self):
        return f"Document {self.document_id} ({self.document_freq} pts)"


def load(path):
    indexer = ctypes.cdll.LoadLibrary(f"{path}/libgeneral-indexer.so")
    indexer.initialize_directory_variables()
    indexer.new_index.argtypes = []
    indexer.new_index.restype = POINTER(SortedKeysIndexStub)

    indexer.append_file.argtypes = [POINTER(SortedKeysIndexStub), c_char_p, c_uint32]
    indexer.append_file.restype = None

    indexer.persist_indices.argtypes = [POINTER(SortedKeysIndexStub), c_char_p]

    indexer.search_many_terms.restype = POINTER(TDWPElem)
    return indexer


DLL = load("/home/henry/search/cmake-build-debug")

"""
void free_elem_buf(TopDocsWithPositions::Elem *ptr);
void free_index_stub(SortedKeysIndexStub *stub);
SortedKeysIndexStub *create_index_stub(const char *suffix);
"""


class TDWPArray:
    def __init__(self, arr: POINTER(TDWPElem), length: int):
        self.arr = arr
        self.length = length

    def get(self, index: int):
        assert index < self.length
        return self.arr[index]

    def iter_elems(self):
        for i in range(0, self.length):
            yield self.arr[i]

    def iter_ids(self):
        return map(lambda k: k.document_id, self.iter_elems())

    def __repr__(self):
        one = f"Array of TopDocs: \n"
        two = "".join(map(lambda k: f"\t{k}\n", self.iter_elems()))
        return one + two


class Searcher:
    def __init__(self, suffix: str):
        self.dll = DLL
        self.ind = self.dll.create_index_stub(bytes(suffix, 'ascii'))

    def search_terms(self, *args):
        terms_len = len(args)

        args = list(map(lambda k: bytes(k, 'ascii').upper(), args))
        terms = (c_char_p * terms_len)(*args)

        result_length = c_uint32(2 ** 31)
        result = self.dll.search_many_terms(self.ind, terms, terms_len, ctypes.pointer(result_length))
        assert result_length != 2 ** 31

        return TDWPArray(result, result_length.value)


class Indexer:
    def __init__(self):
        self.dll = DLL
        self.ind = self.dll.new_index()

    def append_file(self, contents: str, id: int):
        contents = bytes(contents, 'ascii')
        self.dll.append_file(self.ind, contents, id)

    def persist(self, suffix: str):
        self.dll.persist_indices(self.ind, bytes(suffix, 'ascii'))
