
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"
#include "Base26Num.h"
#include "Serializer.h"
#include <cmath>
#include <numeric>
#include "Constants.h"

// TODO: implement tiered postings list for quicker retrieval of very large indexes.

namespace fs = std::filesystem;


/**
 * Compares the shorter string against the longer string, checking if shorter is a prefix of longer.
 *
 * @return a score that means how well they match. A complete match (shorter == longer) will return CUTOFF_MAX;
 */
static unsigned int string_prefix_compare(const std::string &shorter, const std::string &longer) {
    // Score multiplier in case a word matches all (vs. only a prefix match)
    constexpr float MATCHALL_BONUS = 2;
    // Returns true if shorter is the prefix of longer.
    // e.g. shorter: "str" and longer: "string" returns true.
    auto ls = longer.size();
    auto ss = shorter.size();


    if (ls < ss) return 0;

    float divider = 5.F / (ls - ss + 5);
    for (std::size_t i = 0; i < ss; i++) {
        if (shorter[i] != longer[i]) {
            return i * i * divider;
        }
    }
    const auto score = ss * ss * divider;
    if (ss == ls) return MATCHALL_BONUS * score;
    else return score;
}


template<typename Iterator>
int compute_average(Iterator begin, Iterator end) {
    if(end - begin < 6) return 8;

    unsigned int sum = 0, square = 0;

    for(auto i = begin; i < end; i++) {
        sum += *i;
        square += *i * *i;
    }
    sum += end - begin;

    return square / sum;
}


TopDocs SortedKeysIndexStub::search_one_term(const std::string &term) const {
    auto file_start = std::lower_bound(index->begin(), index->end(), Base26Num(term)) - 1;
    auto file_end = std::upper_bound(index->begin(), index->end(), Base26Num(term)) + 1;

    file_start = std::clamp(file_start, index->begin(), index->end());
    file_end = std::clamp(file_end, index->begin(), index->end());

    if (file_start == index->end()) { return TopDocs{}; }

    // We assume that the positions of `terms` and `frequencies` are indetermined.
    // Therefore, we seek to the correct location as determined by the file_start StubIndEntry,
    // read the frequencies_pos, then seek the `frequencies` stream to that location.
    // Now, we have both streams at the correct location.
    auto terms_pos = file_start->terms_pos;
    terms.seekg(terms_pos);
    Serializer::read_str(terms); // First key string
    auto frequencies_pos = Serializer::read_vnum(terms); // Frequencies position
    auto max_terms_read = (file_end - file_start) * STUB_INTERVAL;

    frequencies.seekg(frequencies_pos);

    // Seek back to original location for reading.
    terms.seekg(terms_pos);

    TopDocs output;
    std::vector<TopDocs> outputs;
    std::vector<int> output_score;
    outputs.reserve(50);

    while (max_terms_read-- || terms.tellg() < file_end->terms_pos) {
        // Preview the WIE without loading everything into memory. Since we expect to do many more previews than actual reads,
        // and since majority of keys don't fit within our criteria, previewing reduces computation and memory.
        auto preview = Serializer::preview_work_index_entry(terms);

        // If the preview fits within the score cutoff, then we seek back to the previewed position and read the whole thing into memory
        // to process it.
        auto min_cutoff_score = compute_average(output_score.begin(), output_score.end());
        if (auto score = string_prefix_compare(term, preview.key); score >= min_cutoff_score) {
            // Seek back to original previewed position.
            frequencies.seekg(preview.frequencies_pos);

            // Read the work index entry from the correct, seeked position.
            auto size = Serializer::read_work_index_entry_v2_optimized(frequencies, alignedbuf.get());

            // Do some processing with the data.
            auto init = (DocumentPositionPointer_v2 *) alignedbuf.get();
            auto tot_score = 0;
            for (auto i = init; i < init + size; i++) {
                float coefficient = std::log10(i->frequency + 0.8) + 0.8;
                i->frequency = coefficient * score;
                tot_score += i->frequency;
            }
            TopDocs td(init, init+size);
            td.add_term_str(preview.key);
            outputs.push_back(std::move(td));
            output_score.emplace_back(tot_score / size);
        }
    }

    if (outputs.empty()) return TopDocs{};

    int score_cutoff = compute_average(output_score.begin(), output_score.end());
    for (int i = 1; i < outputs.size(); i++) {

        // Append only words that are above average score, as determined by cutoff.
        if (output_score[i] >= score_cutoff) {
            outputs[0].append_multi(outputs[i]);
        }
    }
    return outputs[0];

}

TopDocs SortedKeysIndexStub::search_many_terms(const std::vector<std::string> &terms) {
    std::vector<TopDocs> all_outputs;
    all_outputs.reserve(terms.size());

    for (int i = 0; i < terms.size(); i++) {
        auto result = this->search_one_term(terms[i]);
        all_outputs.push_back(std::move(result));
    };
    return DocumentsMatcher::AND(all_outputs);
}


constexpr std::size_t BUFLEN = 100000;

SortedKeysIndexStub::SortedKeysIndexStub(std::string suffix) : suffix(suffix),
                                                               filemap((indice_files_dir / ("filemap-" + suffix))) {
    frequencies = std::ifstream(indice_files_dir / ("frequencies-" + suffix), std::ios_base::binary);
    terms = std::ifstream(indice_files_dir / ("terms-" + suffix), std::ios_base::binary);
    assert(this->frequencies && this->terms);


    // Setup read cache buffer
    buffer = std::make_unique<char[]>(BUFLEN);
    this->frequencies.rdbuf()->pubsetbuf(buffer.get(), BUFLEN);

    // Setup documents holding location buffer (aligned).
    alignedbuf = std::make_unique<__m256[]>(MAX_FILES_PER_TERM * 2 / 8);

    index = std::make_shared<const std::vector<StubIndexEntry>>(
            Serializer::read_sorted_keys_index_stub_v2(this->frequencies, this->terms));
}


TopDocs SortedKeysIndexStub::collection_merge_search(std::vector<SortedKeysIndexStub> &indices,
                                                     const std::vector<std::string> &search_terms) {
    TopDocs joined;
    for (auto &index : indices) {
        auto temp = index.search_many_terms(search_terms);

        if (temp.size()) joined.append_multi(temp);
    };

    joined.merge_similar_docs();
    joined.sort_by_frequencies();

    return joined;
}

SortedKeysIndexStub::SortedKeysIndexStub(const SortedKeysIndexStub &other) : filemap(
        indice_files_dir / ("filemap-" + other.suffix)) {
    frequencies = std::ifstream(indice_files_dir / ("frequencies-" + other.suffix), std::ios_base::binary);
    terms = std::ifstream(indice_files_dir / ("terms-" + other.suffix), std::ios_base::binary);
    assert(this->frequencies && this->terms);

    // Setup read cache buffer
    buffer = std::make_unique<char[]>(BUFLEN);
    this->frequencies.rdbuf()->pubsetbuf(buffer.get(), BUFLEN);

    // Setup documents holding location buffer (aligned).
    alignedbuf = std::make_unique<__m256[]>(MAX_FILES_PER_TERM * 2 / 8);

    index = other.index;

    // Copy other suffix to this suffix.
    suffix = other.suffix;
}
