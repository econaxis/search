#include <iostream>
#include "c_abi.h"
#include "Serializer.h"
#include "Constants.h"
#include "rust-interface.h"
#include "Tokenizer.h"
#include "DocumentFrequency.h"
#include "DocumentsMatcher.h"
#include <fmt/ostream.h>
#include "TopDocsResultsJoiner.h"

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
    static constexpr auto format_string = R"({{"fn":"{0}","df":{1},"matches":{2}}})";
    static constexpr auto matches_array_format_string = R"([{0},{1},{2},{3}])";

    std::string match_str = "";
    if (entry.matches[0])
        match_str = fmt::format(matches_array_format_string, entry.matches[0], entry.matches[1], entry.matches[2],
                                entry.matches[3]);
    else match_str = "null";
    fmt::print(out, format_string, filename, entry.document_freq, match_str);
}

void
search_multi_indices(int num_indices, const SortedKeysIndexStub **indices, int num_terms, const char **query_terms_ptr,
                     RustVec *output_buffer) {
    try {
        std::vector<std::string> query(num_terms);
        for (int i = 0; i < num_terms; i++) {
            auto as_str = std::string(query_terms_ptr[i]);
            if(!Tokenizer::clean_token_to_index(as_str)) {
                log("Word ", as_str, " removed from query because it fails clean_token");
                continue;
            }
            if(Tokenizer::check_stop_words(as_str, 0, as_str.size())) {
                log("Word ", as_str, " removed from query because it is stop word");
                continue;
            }
            query[i] = as_str;
        }
        auto indices_span = std::span(*indices, num_indices);

        auto result = TopDocsResultsJoiner::query_multiple_indices(indices_span, query);

        std::ostringstream out{};
        out << "[";
        auto it = result.get_results();
        while (it.valid()) {
            serialize_final_doc_to_json(out, it->doc, indices[it->indexno]->query_filemap(it->doc.document_id));

            // If the next one is also valid, then we need a comma.
            // JSON spec forbids trailing commas, so we always need this check.
            if((it+1).valid()) out << ",";
            it.next();
        }
        out << "]";


        auto str = out.str();

        fill_rust_vec(output_buffer, str.data(), str.size());
    } catch (std::exception &e) {
        std::cerr << e.what() << "\n";
        print("Exception encountered: ", e.what());
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


