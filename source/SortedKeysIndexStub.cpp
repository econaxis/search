//
// Created by henry on 2021-05-01.
//

#include <fstream>
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"
#include "Tokenizer.h"


namespace fs = std::filesystem;
using MultiDocsMultiSearch=robin_hood::unordered_map<uint32_t, MultiSearchResult>;

constexpr auto SCORE_CUTOFF_MAX = (unsigned long) 1 << 20;

/**
 * Similar to Serializer::read_word_index_entry, except it only reads the keys and consumes the file by an equivalent amount.
 * @param stream
 * @return the string key that corresponds to the word index entry.
 */
std::string read_key_only(std::istream &stream) {
    std::string key = Serializer::read_str(stream);
    int doc_pointer_len = Serializer::read_num(stream);
    stream.seekg(doc_pointer_len * sizeof(uint32_t) * 2, std::ios_base::cur);
    return key;
}

/**
 * Fills the current index from file specified in constructor.
 * @param interval interval to skip. Higher interval = slower but less memory. Lower interval = faster but more memory.
 *      Basically, the lower the interval, the less disk seeks we require to query a specific key.
 */
void SortedKeysIndexStub::fill_from_file(int interval) {
    auto num_entries = Serializer::read_num(file);

    for (int i = 0; i < num_entries; i++) {
        auto entry_start_pos = file.tellg();
        auto key = read_key_only(file);
        if (i % interval == 0 || i == num_entries - 1) {
            // Read to file
            index.emplace_back(key, entry_start_pos);
        }
    }
    std::cout << "Short index: " << index.size() << "\n";
}


constexpr unsigned int pow4(std::size_t a) {
    return a * a * a * a;
}
/**
 * Compares the shorter string against the longer string, checking if shorter is a prefix of longer.
 *
 * @return a score that means how well they match. A complete match (shorter == longer) will return CUTOFF_MAX;
 */
int string_prefix_compare(const std::string &shorter, const std::string &longer) {
    // Returns true if shorter is the prefix of longer.
    // e.g. shorter: "str" and longer: "string" returns true.
    auto ls = longer.size();
    auto ss = shorter.size();
    for (std::size_t i = 0; i < ss; i++) {
        if (shorter[i] != longer[i]) {
            if (ss - i >= 2) return 0;
            return pow4(i) / (ls - ss + 1);
        }
    }

    if (ss == ls) return SCORE_CUTOFF_MAX * 2;
    else return pow4(ss) * 100 / (ls - ss + 1);
}

/**
 * If key exists in map, then return the iterator to key. Else, insert default_value into the map at position key, and
 * return iterator to that inserted position.
 *
 * @param map any map type, for which to operate on
 * @param key
 * @param default_value object to insert into map if key doesn't exist.
 * @return iterator to key in map
 */
auto insert_or_get(auto &map, auto &key, auto default_value) {
    auto pos = map.find(key);
    if (pos == map.end()) {
        pos = map.emplace(key, std::move(default_value)).first;
    }
    return pos;
}

/**
 * Similar to search_key. Supports prefix matching for terms longer than/equal to 3 characters.
 *
 * @param term
 * @param before If we want to do AND query after, then before is a map of results corresponding to a different
 *      term in the larger query. This provides some optimization, as if a document doesn't exist in *before*,
 *      then it will be excluded anyways in the final AND query. We early drop out of processing a document
 *      if it doesn't exist in *before*, as that is expensive.
 */
MultiDocsMultiSearch
SortedKeysIndexStub::search_key_prefix_match(const std::string &term,
                                             MultiDocsMultiSearch &before) {
    // Merges results from this term onto previous result.
    MultiDocsMultiSearch prev_result;
    prev_result.reserve(10000);
    auto[file_start, file_end] = std::equal_range(index.begin(), index.end(), Base26Num(term));

    if (file_start == index.end()) { return prev_result; }
    if (file_start > index.begin()) file_start--;

    file.seekg(file_start->doc_position);

    const auto &cached_cutoff = [&]() {
        return pow4(prev_result.size());
    };
    while (file.tellg() < file_end->doc_position) {
        auto entry_start_pos = file.tellg();
        auto key = read_key_only(file);
        if (term.size() > key.size()) continue;

        // This uses the normal string-string comparison rather than uint64.
        // If more than 3 characters match, then we good.
        if (uint32_t score = string_prefix_compare(term, key); score >= 0.1 * cached_cutoff()) {
            // Seek back to original entry start position and actually read the file.
            file.seekg(entry_start_pos);
            auto entry = Serializer::read_work_index_entry(file);

            // Success. We found the key.
            for (auto &filepointer : entry.files) {

                if (before.find(filepointer.document_id) == before.end() && !before.empty()) {
                    // Drop out early, since this document is not contained in a previous term's search results.
                    continue;
                }

                auto pos = insert_or_get(prev_result, filepointer.document_id,
                                         MultiSearchResult(filepointer.document_id, 0, {}));
                pos->second.score += score;
                pos->second.positions.emplace_back(score, filepointer.document_position);

            }

        }
    }
    return prev_result;
}

/**
 * Searches for specific key in the index, without any special features. No prefix matching.
 * @param term the term to search for.
 * @return
 */
MultiDocsMultiSearch SortedKeysIndexStub::search_key(const std::string &term) {
    MultiDocsMultiSearch output;
    auto[file_start, file_end] = std::equal_range(index.begin(), index.end(), Base26Num(term));

    if (file_start == index.end()) { return output; }
    if (file_start > index.begin()) file_start--;

    file.seekg(file_start->doc_position, std::ios_base::beg);

    while (file.tellg() < file_end->doc_position) {

        auto prevpos = file.tellg();
        auto key = read_key_only(file);

        // This uses the normal string-string comparison rather than Base26Num comparison.
        // Since the prefix scoring function returns a score larger than SCORE_CUTOFF_MAX if the two strings completely match,
        // we can reuse this function if we want to check for complete match, but also get a score to how well it matches.
        // e.g. longer matches should score higher than shorter matches.
        if (auto score = string_prefix_compare(key, term); score >= SCORE_CUTOFF_MAX) {
            score -= SCORE_CUTOFF_MAX;
            file.seekg(prevpos);
            auto entry = Serializer::read_work_index_entry(file);
            // Success. We found the key.

            for (auto i : entry.files) {
                auto pos = insert_or_get(output, i.document_id, MultiSearchResult(i.document_id, 0, {}));
                pos->second.score += score;
                pos->second.positions.emplace_back(score, i.document_position);
            }
        }

    }
}



std::vector<MultiSearchResult> SortedKeysIndexStub::search_keys(std::vector<std::string> keys, std::string mode) {
    std::vector<MultiDocsMultiSearch> init;

#ifndef PREFIX_SEARCH
    for (auto &key : keys) {
        init.push_back(search_key(key));
    }
#else
    MultiDocsMultiSearch init1;
    auto prev_init = init.begin();
    for (auto &key : keys) {
        if(prev_init >= init.begin() + 1) {
            init.emplace_back(search_key_prefix_match(key, *(prev_init-1)));
        } else {
            init.emplace_back(search_key_prefix_match(key, init1));
        }

        prev_init++;

    }
#endif
    return DocumentsMatcher::AND(init);
}


/**
 * We're actually using Base27 because we want "AA" to be higher than "AAA."
 * Using Base26 would mean both are 0 and have no ordering. Therefore, A corresponds to 1.
 * This makes AA be 11... and AAA be 111...; 11...<111...
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
constexpr int MAX_CHARS = 10;
/**
 * Used to convert a string to a 64 bit unsigned integer for quicker comparison and easier memory usage.
 * Only the first MAX_CHARS characters are included in the number. All further characters are ignored.
 * This shouldn't be a problem as these comparisons hint where in the disk to search, from which we
 * compare strings normally.
 */
Base26Num::Base26Num(std::string from) {
    num = 0;
    Tokenizer::remove_punctuation(from);
    const int max_iter = std::min((int) from.size(), MAX_CHARS);
    for (int i = 0; i < max_iter; i++) {
        num += (from[i] - 'A' + 1) * alphabet_pow[MAX_CHARS - i - 1];
    }
}
