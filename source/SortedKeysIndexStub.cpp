//
// Created by henry on 2021-05-01.
//

#include <fstream>
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"

namespace fs = std::filesystem;

void SortedKeysIndexStub::fill_from_file(int interval) {
    auto num_entries = Serializer::read_num(file);

    for (int i = 0; i < num_entries; i++) {
        auto entry_start_pos = file.tellg();
        std::string key = Serializer::read_str(file);
        int doc_pointer_len = Serializer::read_num(file);
        file.seekg(doc_pointer_len * sizeof(uint32_t) * 2, std::ios_base::cur);
        if (i % interval == 0 || i == num_entries - 1) {
            // Read to file
            index.emplace_back(key, entry_start_pos);
        }
    }
    std::cout << "Short index: " << index.size() << "\n";
}

int disk_seeks = 0, searches = 0;


std::optional<SearchResult> SortedKeysIndexStub::search_key(const std::string &term) {
    searches++;
    auto[file_start, file_end] = std::equal_range(index.begin(), index.end(), term);


    if (file_start == index.end()) { return SearchResult(); }
    if (file_start > index.begin()) file_start--;

    file.seekg(file_start->doc_position, std::ios_base::beg);

    while (file.tellg() < file_end->doc_position) {
        disk_seeks++;
        auto entry = Serializer::read_work_index_entry(file);

        // This uses the normal string-string comparison rather than uint64.
        if (entry.key == term) {
            // Success. We found the key.
            return entry.files;
        }
    }
    return std::nullopt;

}

std::vector<MultiSearchResult> SortedKeysIndexStub::search_keys(std::vector<std::string> keys, std::string mode) {
    // search_key function forces us to own the SearchResult vector.
    // Hold the memory in this vector, and pass the results vector to DocumentsMatcher.
    std::vector<SearchResult> results_memory_holder;
    std::vector<const SearchResult *> results;
    std::vector<std::string> result_terms;

    SearchResult empty_result_variable; // Since we have pointers, just make an empty stack variable representing empty array.
    // This variable will never outlive the results vector.
    results_memory_holder.reserve(keys.size());
    for (auto &key : keys) {
        if (auto searchresult = search_key(key); searchresult) {
            results_memory_holder.push_back(searchresult.value());
        } else {
            results.push_back(&empty_result_variable);
        }
        result_terms.push_back(key);
    }

    for (const auto &i : results_memory_holder) {
        results.push_back(&i);
    }
    if (mode == "OR") return DocumentsMatcher::OR(results, result_terms);
    else return DocumentsMatcher::AND(results, result_terms);
}


/**
 * Used to convert a string to a 64 bit unsigned integer for quicker comparison and easier memory usage.
 * Only the first MAX_CHARS characters are included in the number. All further characters are ignored.
 * This shouldn't be a problem as these comparisons hint where in the disk to search, from which we
 * compare strings normally.
 */
constexpr uint64_t LETTER_POW1 = 27;
constexpr uint64_t LETTER_POW2 = 27 * LETTER_POW1;
constexpr uint64_t LETTER_POW3 = 27 * LETTER_POW2;
constexpr uint64_t LETTER_POW4 = 27 * LETTER_POW3;
constexpr uint64_t LETTER_POW5 = 27 * LETTER_POW4;
constexpr uint64_t LETTER_POW6 = 27 * LETTER_POW5;
constexpr uint64_t LETTER_POW7 = 27 * LETTER_POW6;
constexpr uint64_t LETTER_POW8 = 27 * LETTER_POW7;
constexpr uint64_t LETTER_POW9 = 27 * LETTER_POW8;
constexpr uint64_t LETTER_POW10 = 27 * LETTER_POW9;
constexpr uint64_t LETTER_POW11 = 27 * LETTER_POW10;
constexpr uint64_t LETTER_POW12 = 27 * LETTER_POW11;
constexpr uint64_t alphabet_pow[] = {LETTER_POW1, LETTER_POW2, LETTER_POW3, LETTER_POW4, LETTER_POW5, LETTER_POW6,
                                     LETTER_POW7, LETTER_POW8, LETTER_POW9, LETTER_POW10, LETTER_POW11, LETTER_POW12};
constexpr int MAX_CHARS = 12;

Base26Num::Base26Num(const std::string &from) {
    num = 0;
    const int max_iter = std::min((int) from.size(), MAX_CHARS);
    for (int i = 0; i < max_iter; i++) {
        num += (from[i] - 'A' + 1) * alphabet_pow[MAX_CHARS - i - 1];
    }
}
