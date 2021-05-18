
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"
#include "SortedKeysIndex.h"
#include "Tokenizer.h"


namespace fs = std::filesystem;

constexpr unsigned long MATCHALL_BONUS = 20;
constexpr unsigned long MATCHALL_SHORT_BONUS = 8;


/**
 * Fills the current index from file specified in constructor.
 * @param interval interval to skip. Higher interval = slower but less memory. Lower interval = faster but more memory.
 *      Basically, the lower the interval, the less disk seeks we require to query a specific key.
 */
//void SortedKeysIndexStub::fill_from_file(int interval) {
//    auto num_entries = Serializer::read_num(file);
//
//    for (int i = 0; i < num_entries; i++) {
//        auto entry_start_pos = file.tellg();
//        auto key = read_key_only(file);
//        if (i % interval == 0 || i == num_entries - 1) {
//            // Read to file
//            index.emplace_back(key, entry_start_pos);
//        }
//    }
//    std::cout << "Short index: " << index.size() << "\n";
//}
//

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

    if (ls < ss) return 0;

    for (std::size_t i = 0; i < ss; i++) {
        if (shorter[i] != longer[i]) {
            return i/ (ls - ss + 1);
        }
    }

    const auto score = ss / (ls - ss + 1) + MATCHALL_SHORT_BONUS;
    if (ss == ls) return MATCHALL_BONUS + score;
    else return score;
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
#include <cmath>
#include <numeric>

constexpr std::size_t MAX_CHARS = 10;

/**
 * Used to convert a string to a 64 bit unsigned integer for quicker comparison and easier memory usage.
 * Only the first MAX_CHARS characters are included in the number. All further characters are ignored.
 * This shouldn't be a problem as these comparisons hint where in the disk to search, from which we
 * compare strings normally.
 */
Base26Num::Base26Num(std::string from) {
    num = 0;
    Tokenizer::remove_punctuation(from);
    const int max_iter = std::min(from.size(), MAX_CHARS);
    for (int i = 0; i < max_iter; i++) {
        num += (from[i] - 'A' + 1) * alphabet_pow[MAX_CHARS - i - 1];
    }
}

std::vector<DocumentPositionPointer_v2> wiebuffer;

TopDocs SortedKeysIndexStub::search_one_term(const std::string &term)const  {
    auto term_after = Base26Num(term);
    auto term_before = Base26Num(term);

    auto file_start = std::lower_bound(index.begin(), index.end(), term_before) - 1;
    auto file_end = std::upper_bound(index.begin(), index.end(), term_after) + 1;

    file_start = std::clamp(file_start, index.begin(), index.end());
    file_end = std::clamp(file_end, index.begin(), index.end());

    if (file_start == index.end()) { return TopDocs{}; }


    frequencies.seekg(file_start->doc_position);

    // Peek the term position.
    auto term_pos = Serializer::read_vnum(frequencies);
    frequencies.seekg(file_start->doc_position);
    terms.seekg(term_pos);

    auto frequencies_pos = frequencies.tellg();

    TopDocs output, shorter;
    while (frequencies_pos <= file_end->doc_position) {
        // Preview the WIE without loading everything into memory.
        auto [freq_initial_off, terms_initial_off, key] = Serializer::preview_work_index_entry(frequencies, terms);

        // The number of bytes advanced = the offset amount.
        frequencies_pos += freq_initial_off;
        // This uses the normal string-string comparison rather than uint64.
        // If more than 3 characters match, then we good.
        if (uint32_t score = string_prefix_compare(term, key); score >= std::min(output.size(), 5UL) + 5) {
            // Seek back to original previewed position.
            frequencies.seekg(-freq_initial_off, std::ios_base::cur);
            terms.seekg(-terms_initial_off, std::ios_base::cur);
            auto key = Serializer::read_work_index_entry_v2_optimized(frequencies, terms, wiebuffer);

            for (auto &f : wiebuffer) {
                float coefficient = (float) (f.frequency - 1) / 3.F + 1;
                f.frequency = coefficient * (float) score;
            }

            shorter.append_multi(wiebuffer.begin(), wiebuffer.end());

            if(shorter.size() > output.size() / 3 + 20) {
                output.append_multi(shorter.begin(), shorter.end());
                shorter.docs.clear();
            }
        }
    }
    output.append_multi(shorter.begin(), shorter.end());

    return output;
}

TopDocs SortedKeysIndexStub::search_many_terms(const std::vector<std::string> &terms) {
    std::vector<TopDocs> all_outputs;
    all_outputs.reserve(terms.size());

    for(int i = 0; i < terms.size(); i++) {
        auto result = this->search_one_term(terms[i]);
        result.sort_by_ids();
//        for(auto& j : result) j.id = i;
        all_outputs.push_back(std::move(result));
    };
    return DocumentsMatcher::AND(all_outputs);
}

SortedKeysIndexStub::SortedKeysIndexStub(std::filesystem::path frequencies, std::filesystem::path terms) : frequencies(
        frequencies, std::ios_base::binary),
                                                                                                           terms(terms,
                                                                                                                 std::ios_base::binary) {
    assert(this->frequencies && this->terms);
    buffer = std::make_unique<char[]>(2048);
    this->frequencies.rdbuf()->pubsetbuf(buffer.get(), 2048);
    index = Serializer::read_sorted_keys_index_stub_v2(this->frequencies, this->terms);
}



 TopDocs SortedKeysIndexStub::collection_merge_search(std::vector<SortedKeysIndexStub> &indices, const std::vector<std::string> &search_terms) {
    std::vector<TopDocs> results;
    int incrementing = 0;
    for (auto& index : indices) {
        auto temp = index.search_many_terms(search_terms);
        for (DocumentPositionPointer_v2 &d : temp) {
//            d.unique_identifier = incrementing;
        }
        incrementing++;
        results.push_back(temp);
    };


    TopDocs joined = std::reduce(results.begin(), results.end(), TopDocs{}, [&](auto r1, auto r2) {
        r1.append_multi(r2.begin(), r2.end());
        return r1;
    });

    joined.merge_similar_docs();

     return joined;
}
