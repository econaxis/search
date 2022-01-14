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
/*
 * Below are functions to load, search, and deallocate StubIndexes.
 */
void initialize_dir_vars() {
    initialize_directory_variables(nullptr);
}


//void
//search_multi_indices(int num_indices, const SortedKeysIndexStub **indices, int num_terms, const char **query_terms_ptr,
//                     RustVec *output_buffer) {
//    try {
//        std::vector<std::string> query(num_terms);
//        for (int i = 0; i < num_terms; i++) {
//            auto as_str = std::string(query_terms_ptr[i]);
//            if(!Tokenizer::clean_token_to_index(as_str)) {
//                log("Word ", as_str, " removed from query because it fails clean_token");
//                continue;
//            }
//            if(Tokenizer::check_stop_words(as_str, 0, as_str.size())) {
//                log("Word ", as_str, " removed from query because it is stop word");
//                continue;
//            }
//            query[i] = as_str;
//        }
//        auto indices_span = std::span(*indices, num_indices);
//
//        auto result = TopDocsResultsJoiner::query_multiple_indices(indices_span, query);
//
//        std::ostringstream out{};
//        out << "[";
//        auto it = result.get_results();
//        while (it.valid()) {
//            serialize_final_doc_to_json(out, it->doc, indices[it->indexno]->query_filemap(it->doc.document_id));
//
//            // If the next one is also valid, then we need a comma.
//            // JSON spec forbids trailing commas, so we always need this check.
//            if((it+1).valid()) out << ",";
//            it.next();
//        }
//        out << "]";
//
//
//        auto str = out.str();
//
//        fill_rust_vec(output_buffer, str.data(), str.size());
//    } catch (std::exception &e) {
//        std::cerr << e.what() << "\n";
//        print("Exception encountered: ", e.what());
//        exit(1);
//    }
//}


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


