// C interface functions
#include "IndexStubCApi.h"
#include "PositionsSearcher.h"


std::vector<std::string> from_char_arr(const char **terms, int length) {
    auto vec = std::vector<std::string>();
    for (int i = 0; i < length; i++) {
        vec.push_back(terms[i]);
    }
    return vec;
}
template<typename T>
T* leak_vec(std::vector<T> vec) {
    auto *buf_ = operator new[](vec.size() * sizeof(T));
    auto *buf = (T*) buf_;
    std::move(vec.begin(), vec.end(), buf);
    return buf;
}


extern "C" {
using namespace DocumentsMatcher;

uint32_t elems_allocated = 0;

SortedKeysIndexStub* create_index_stub(const char* suffix) {
    return new SortedKeysIndexStub(suffix);
}

void free_elem_buf(SearchRetType elem) {
    delete[] elem.pos;
    delete[] elem.topdocs;
    elems_allocated--;
}
void free_index(SortedKeysIndexStub* stub) {
    delete stub;
}

SearchRetType
search_many_terms(SortedKeysIndexStub *index, const char **terms, int terms_length) {
    auto terms_vec = from_char_arr(terms, terms_length);
    auto output = index->search_many_terms(terms_vec);
    auto outputs_anded = DocumentsMatcher::AND_Driver(output);
    auto outputs_size = (uint32_t) outputs_anded.size();

    auto docs_hashset = outputs_anded.get_id_hashset();
    auto pos_mat = PositionsSearcher::fill_positions_from_docs(*index, terms_vec, [&](auto id) {
        return docs_hashset.contains(id);
    });
    auto pos_size = (uint32_t) pos_mat.size();

    elems_allocated++;
    return SearchRetType {
            leak_vec(TopDocs::into_docs(std::move(outputs_anded))), outputs_size, leak_vec(pos_mat), pos_size
    };
}

}