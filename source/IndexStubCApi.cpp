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
T *leak_vec(std::vector<T> vec) {
    auto *buf_ = operator new[](vec.size() * sizeof(T));
    auto *buf = (T *) buf_;
    std::move(vec.begin(), vec.end(), buf);
    return buf;
}


extern "C" {
using namespace DocumentsMatcher;

uint32_t elems_allocated = 0;

SortedKeysIndexStub *create_index_stub(const char *suffix) {
    return new SortedKeysIndexStub(suffix);
}

void free_elem_buf(SearchRetType elem) {
    delete[] elem.pos;
    delete[] elem.topdocs;
    elems_allocated--;
}
void free_index(SortedKeysIndexStub *stub) {
    delete stub;
}

SearchRetType
search_many_terms(SortedKeysIndexStub *index, const char **terms, int terms_length, bool load_positions, bool and_query) {
    if (terms_length <= 0) {
        throw std::runtime_error("Searched zero terms");
    }

    auto terms_vec = from_char_arr(terms, terms_length);
    auto output = index->search_many_terms(terms_vec);

    TopDocs combined;
    if(and_query) {
        combined = DocumentsMatcher::AND_Driver(output);
    } else {
        combined = DocumentsMatcher::OR(output);
    }
    auto outputs_size = (uint32_t) combined.size();

    FoundPositions* pos = nullptr;
    uint32_t pos_len = 0;
    if (load_positions) {
        auto docs_hashset = combined.get_id_hashset();
        auto pos_mat = PositionsSearcher::fill_positions_from_docs(*index, output, [&](auto id) {
            return docs_hashset.contains(id);
        });
        pos_len = (uint32_t) pos_mat.size();
        pos = leak_vec(pos_mat);
    }

    elems_allocated++;
    return SearchRetType{
            leak_vec(TopDocs::into_docs(std::move(combined))), outputs_size, pos, pos_len
    };
}

}