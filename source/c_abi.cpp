#include <iostream>
#include "c_abi.h"
#include "Serializer.h"
#include "Constants.h"
#include "rust-interface.h"
#include "Tokenizer.h"
#include "DocumentFrequency.h"
#include <fmt/core.h>
#include "DocumentsMatcher.h"
#include <fmt/ostream.h>

namespace ffi = Serializer::ffi;
namespace sr = Serializer;
namespace dm = DocumentsMatcher;

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
    for (std::size_t i = 0; i < vec->size(); i++) {
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

void
serialize_final_doc_to_json(std::ostream &out, dm::TopDocsWithPositions::Elem &entry, const std::string &filename) {
    static const auto format_string = R"({{"fn":"{0}","df":{1},"matches":{2}}})";
    static const auto matches_array_format_string = R"([{0},{1},{2},{3}])";

    std::string match_str = "";
    if (entry.matches[0])
        match_str = fmt::format(matches_array_format_string, entry.matches[0], entry.matches[1], entry.matches[2],
                                entry.matches[3]);
    else match_str = "null";
    fmt::print(out, format_string, filename, entry.document_freq, match_str);
}

void search_multi_indices(int num_indices, SortedKeysIndexStub **indices, int num_terms, const char **query_terms,
                          RustVec *output_buffer) {
    try {
        assert(num_indices < 32);
        constexpr uint32_t tag_remover = (1 << 27) - 1;
        std::vector<std::string> query(num_terms);
        for (int i = 0; i < num_terms; i++) {
            auto as_str = std::string(query_terms[i]);
            Tokenizer::clean_token_to_index(as_str);
            query[i] = as_str;
        }

        DocumentsMatcher::TopDocsWithPositions joined;
        for (std::size_t i = 0; i < num_indices; i++) {
            auto temp = indices[i]->search_many_terms(query);
            auto topdocs_with_pos = DocumentsMatcher::combiner_with_position(*indices[i], temp);

            uint32_t curtag = i << 27;

            // Imbue top 4 bits of docid with index tag (which index the doc id is associated with)
            for (auto &pair: topdocs_with_pos) {
                assert(pair.document_id < 1 << 27);
                pair.document_id |= curtag;
            }
            if (!topdocs_with_pos.docs.empty()) joined.insert(topdocs_with_pos);
        }

        joined.sort_by_frequencies();
        if (joined.docs.size() > 40) {
            auto bound = std::lower_bound(joined.docs.begin(), joined.docs.end(), 60);

            if (joined.docs.end() - bound > 40) {
                joined.docs.erase(joined.docs.begin(), bound);
            }
        }
        std::reverse(joined.docs.begin(), joined.docs.end());

        std::ostringstream out{};
        out << "[";
        for (auto &i : joined) {
            auto docid = i.document_id & tag_remover;
            auto index = i.document_id >> 27;
            serialize_final_doc_to_json(out, i, indices[index]->query_filemap(docid));

            if (i.document_id != (joined.end() - 1)->document_id) {
                out << ",";
            }
        }
        out << "]";


        auto str = out.str();

        fill_rust_vec(output_buffer, str.data(), str.size());
    } catch (std::exception &e) {
        std::cerr << e.what() << "\n";
        exit(1);
    }
}


uint32_t query_for_filename(SortedKeysIndexStub *index, uint32_t docid, char *buffer, uint32_t bufferlen) {
    auto str = index->query_filemap(docid);

    std::strncpy(buffer, str.c_str(), bufferlen);
    return strlen(str.c_str()) + 1;
}

SortedKeysIndexStub *load_one_index(const char *suffix_name) {
    try {
        std::string suffix = suffix_name;
        auto ssk = new SortedKeysIndexStub(suffix);
        return ssk;
    } catch (const std::exception &e) {
        std::cerr << "C library exception: " << e.what() << "\n";
        throw e;
    }
}

void delete_one_index(SortedKeysIndexStub *ssk) {
    delete ssk;
}


SortedKeysIndexStub *clone_one_index(SortedKeysIndexStub *other) {
    auto *clone = new SortedKeysIndexStub(*other);
    return clone;
}


