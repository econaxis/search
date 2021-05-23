
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"
#include "SortedKeysIndex.h"
#include "Tokenizer.h"
#include "Base26Num.h"
#include <cmath>

namespace fs = std::filesystem;

constexpr unsigned int MATCHALL_BONUS = 20;
constexpr unsigned int MATCHALL_SHORT_BONUS = 8;

/**
 * Compares the shorter string against the longer string, checking if shorter is a prefix of longer.
 *
 * @return a score that means how well they match. A complete match (shorter == longer) will return CUTOFF_MAX;
 */
unsigned int string_prefix_compare(const std::string &shorter, const std::string &longer) {
    // Returns true if shorter is the prefix of longer.
    // e.g. shorter: "str" and longer: "string" returns true.
    auto ls = longer.size();
    auto ss = shorter.size();

    if (ls < ss) return 0;

    for (std::size_t i = 0; i < ss; i++) {
        if (shorter[i] != longer[i]) {
            return i / (ls - ss + 1);
        }
    }

    const auto score = ss / (ls - ss + 1) + MATCHALL_SHORT_BONUS;
    if (ss == ls) return MATCHALL_BONUS + score;
    else return score;
}

#include <chrono>

using namespace std::chrono;


#include <numeric>

std::ofstream deb("/tmp/debug");


TopDocs SortedKeysIndexStub::search_one_term(const std::string &term) const {
    auto file_start = std::lower_bound(index.begin(), index.end(), Base26Num(term)) - 1;
    auto file_end = std::upper_bound(index.begin(), index.end(), Base26Num(term)) + 1;

    file_start = std::clamp(file_start, index.begin(), index.end());
    file_end = std::clamp(file_end, index.begin(), index.end());

    if (file_start == index.end()) { return TopDocs{}; }


    auto frequencies_pos = file_start->doc_position;
    frequencies.seekg(frequencies_pos);

    // Peek the term position.
    auto term_pos = Serializer::read_vnum(frequencies);
    frequencies.seekg(frequencies_pos);

    terms.seekg(term_pos);

    TopDocs output;
    std::vector<TopDocs> outputs;
    outputs.reserve(50);

    int score_cutoff_booster = 5;
    while (frequencies_pos <= file_end->doc_position) {
        // Preview the WIE without loading everything into memory.
        auto[freq_initial_off, terms_initial_off, key] = Serializer::preview_work_index_entry(frequencies, terms);

        // The number of bytes advanced = the offset amount.
        frequencies_pos += freq_initial_off;
        if (auto score = filterfunc(term, key); score >= std::min(output.size() / 200, 5UL) + score_cutoff_booster) {
            // Seek back to original previewed position.
            frequencies.seekg(-freq_initial_off, std::ios_base::cur);
            auto size = Serializer::read_work_index_entry_v2_optimized(frequencies, alignedbuf.get());

            auto init = (DocumentPositionPointer_v2 *) alignedbuf.get();
            for (auto i = init; i < init + size; i++) {
                float coefficient = std::log10(i->frequency) + 1;
                i->frequency = coefficient * score;
            }
            outputs.emplace_back(init, init + size);

        }
    }

    for (int i = 1; i < outputs.size(); i++) {
        outputs[0].append_multi(outputs[i].begin(), outputs[i].end(), false);
    }
    return outputs[0];

}

TopDocs SortedKeysIndexStub::search_many_terms(const std::vector<std::string> &terms) {
    std::vector<TopDocs> all_outputs;
    all_outputs.reserve(terms.size());

    for (int i = 0; i < terms.size(); i++) {
        auto result = this->search_one_term(terms[i]);
        //        for(auto& j : result) j.id = i;
        all_outputs.push_back(std::move(result));
    };
    return DocumentsMatcher::AND(all_outputs);
}


int default_prefix_filter_function(const std::string &search_term, const std::string &tested_term) {
    int score = string_prefix_compare(search_term, tested_term);
    return score;
}

int default_filter_function(const std::string &search_term, const std::string &tested_term) {
    return (search_term == tested_term) * (search_term.size());
}

constexpr std::size_t BUFLEN = 100000;

#include "Constants.h"

SortedKeysIndexStub::SortedKeysIndexStub(std::string suffix) : filterfunc(default_prefix_filter_function) {
    frequencies = std::ifstream(indice_files_dir / ("frequencies-" + suffix), std::ios_base::binary);
    terms = std::ifstream(indice_files_dir / ("terms-" + suffix), std::ios_base::binary);
    assert(this->frequencies && this->terms);

    auto filemap_f = std::ifstream(indice_files_dir / ("filemap-" + suffix), std::ios_base::binary);
    filemap = Serializer::read_filepairs(filemap_f);

    // Setup read cache buffer
    buffer = std::make_unique<char[]>(BUFLEN);
    this->frequencies.rdbuf()->pubsetbuf(buffer.get(), BUFLEN);

    // Setup documents holding location buffer (aligned).
    alignedbuf = std::make_unique<__m256[]>(MAX_FILES_PER_TERM * 2 / 8);

    index = Serializer::read_sorted_keys_index_stub_v2(this->frequencies, this->terms);
}


TopDocs SortedKeysIndexStub::collection_merge_search(std::vector<SortedKeysIndexStub> &indices,
                                                     const std::vector<std::string> &search_terms) {
    TopDocs joined;
    for (auto &index : indices) {
        auto temp = index.search_many_terms(search_terms);

        if (temp.size()) joined.append_multi(temp.begin(), temp.end());
    };

    joined.merge_similar_docs();
    joined.sort_by_frequencies();

    return joined;
}
