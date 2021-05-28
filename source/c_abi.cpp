#include "c_abi.h"
#include "Serializer.h"
#include "Constants.h"
#include "rust-interface.h"
#include "Tokenizer.h"

namespace ffi = Serializer::ffi;
namespace sr = Serializer;

void abi_error(std::string message) {
    std::cout << "error: " << message;
    throw std::runtime_error(message);
}

ifstream *create_ifstream_from_path(const char *path) {
    return ffi::create_ifstream_from_path(path);
};

void deallocate_ifstream(ifstream *stream) {
    return ffi::deallocate(stream);
};

void deallocate_ofstream(ofstream *stream) {
    return ffi::deallocate(stream);
}

void read_from_ifstream(ifstream *stream, char *buffer, uint32_t max_len) {
    stream->read(buffer, max_len);
}

uint32_t read_str(ifstream *stream, char *buf) {
    std::string str = sr::read_str(*stream);
    if (str.size() >= 500) {
        throw std::runtime_error("String too big");
    }
    str.copy(buf, str.size());
    return str.size();
}

uint32_t read_vnum(ifstream *stream) {
    return sr::read_vnum(*stream);
}


void read_filepairs(ifstream *stream, std::vector<DocIDFilePair> **vecpointer, uint32_t *length) {
    auto *vec = new std::vector<DocIDFilePair>();
    *vec = sr::read_filepairs(*stream);
    *vecpointer = vec;
    // 4 bytes for docid, 8 bytes for char*
    *length = vec->size();
}

struct RustDIFP {
    uint32_t docid;
    char *name;
};

void copy_filepairs_to_buf(std::vector<DocIDFilePair> *vec, RustDIFP *buf, uint32_t max_length) {
    if (max_length != vec->size()) abi_error("Incorrect size");
    for (int i = 0; i < vec->size(); i++) {
        buf[i].docid = vec->at(i).document_id;
        buf[i].name = vec->at(i).file_name.data();
    }
}

void deallocate_vec(std::vector<DocIDFilePair> *ptr) {
    delete ptr;
}


/*
 * Below are functions to load, search, and deallocate StubIndexes.
 */


void initialize_dir_vars() {
    initialize_directory_variables();
}

struct DocumentPositionPointer_v2_imbued {
    uint32_t document_id;
    uint32_t frequency;
    uint8_t index_num;
};

void search_multi_indices(int num_indices, SortedKeysIndexStub **indices, int num_terms, const char **query_terms,
                          RustVec *output_buffer) {

    assert(num_indices < 32);
    std::vector<std::string> query(num_terms);
    for (int i = 0; i < num_terms; i++) {
        auto as_str = std::string(query_terms[i]);
        if (!Tokenizer::is_stop_word(as_str)) {
            Tokenizer::clean_token_to_index(as_str);
            query[i] = as_str;
        }
    }

    TopDocs joined;
    for (int i = 0; i < num_indices; i++) {
        auto temp = indices[i]->search_many_terms(query);

        uint32_t curtag = i << 27;

        // Imbue top 4 bits of docid with tag (which index we are using)
        for (auto &pair: temp) {
            assert(pair.document_id < 1 << 27);
            pair.document_id |= curtag;
        }
        if (temp.size()) joined.append_multi(temp.begin(), temp.end());
    }

    joined.sort_by_frequencies();
    std::reverse(joined.begin(), joined.end());

    std::vector<DocumentPositionPointer_v2_imbued> imbued;

    uint32_t tag_remover = (1 << 27) - 1;

    for (auto &i : joined) {
        imbued.push_back({i.document_id & tag_remover, i.frequency, static_cast<uint8_t>(i.document_id >> 27)});
    }
    if (joined.size() > 100) {
        fill_rust_vec(output_buffer, imbued.begin().base(), 100 * sizeof(DocumentPositionPointer_v2_imbued));
    } else {
        fill_rust_vec(output_buffer, imbued.begin().base(), joined.size() * sizeof(DocumentPositionPointer_v2_imbued));
    }
}


uint32_t query_for_filename(SortedKeysIndexStub *index, uint32_t docid, char *buffer, uint32_t bufferlen) {
    auto str = index->query_filemap(docid);
    auto absstrpath = data_files_dir / "data" / str;

    std::strncpy(buffer, absstrpath.c_str(), bufferlen);
    return strlen(absstrpath.c_str()) + 1;
}

void search_index_top_n(SortedKeysIndexStub *index, RustVec *output_buffer, int term_num, const char **query_terms) {
    std::vector<std::string> query(term_num);
    for (int i = 0; i < term_num; i++) query[i] = std::string(query_terms[i]);

    auto td = index->search_many_terms(query);
    td.sort_by_frequencies();

    if (td.size() > 300) {
        fill_rust_vec(output_buffer, td.begin().base(), 300 * sizeof(DocumentPositionPointer_v2));
    } else {
        fill_rust_vec(output_buffer, td.begin().base(), td.size() * sizeof(DocumentPositionPointer_v2));
    }
}

SortedKeysIndexStub *load_one_index(const char *suffix_name) {

    std::string suffix = suffix_name;
    auto ssk = new SortedKeysIndexStub(suffix);
    return ssk;
}

void delete_one_index(SortedKeysIndexStub *ssk) {
    delete ssk;
}


SortedKeysIndexStub *clone_one_index(SortedKeysIndexStub *other) {
    auto *clone = new SortedKeysIndexStub(*other);
    return clone;
}


