#include "PositionsSearcher.h"
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "Base26Num.h"
#include "Serializer.h"
#include <cmath>
#include <numeric>
#include "Constants.h"
#include "DocumentFrequency.h"

namespace fs = std::filesystem;


/**
 * Compares the shorter string against the longer string, checking if shorter is a prefix of longer.
 *
 * @return a score that means how well they match. A complete match (shorter == longer) will return CUTOFF_MAX;
 */
static double string_prefix_compare(const std::string &one, const std::string &two) {
    // Score multiplier in case a word matches all (vs. only a prefix match)
    constexpr double MATCHALL_BONUS = 15;
    constexpr double MATCHSHORT_BONUS = 8.0;
    // Returns true if shorter is the prefix of longer.
    // e.g. shorter: "str" and longer: "string" returns true.
    auto ones = one.size();
    auto twos = two.size();
    auto ls = std::max(ones, twos);
    auto ss = std::min(ones, twos);


    if (ls < ss) return 0;

    double divider = 7.F / (ls - ss + 7);
    int counter = 0;
    for (; counter < ss; counter++) {
        if (one[counter] != two[counter]) {
            break;
        }
    }
    double counterd = counter;
    const auto score = counterd * divider;
    if (one == two) {
        return MATCHALL_BONUS * score;
    }
    if (counter == one.size()) {
        return MATCHSHORT_BONUS * score;
    }
    return score;
}


template<typename Iterator>
static double compute_average(Iterator begin, Iterator end) {
    if (end - begin < 4) return 1;

    unsigned int sum = 0, square = 0;

    for (auto i = begin; i < end; i++) {
        sum += *i;
    }
    sum += end - begin;
    return sum / (end - begin);
}

/*
 * 190716193099524870
 * 5994033433795872
 */

std::optional<PreviewResult> SortedKeysIndexStub::seek_to_term(const std::string &term) const {
    auto num = Base26Num(term);
    auto file_start = std::lower_bound(index.begin(), index.end(), Base26Num(term));
    if (file_start > index.begin()) file_start--;

    // If we can get a lower bound, then continue searching more precisely.
    if (file_start != index.end()) {
        // We assume that the positions of `terms` and `frequencies` are indetermined.
        // Therefore, we seek to the correct location as determined by the file_start StubIndEntry,
        // read the frequencies_pos, then seek the `frequencies` stream to that location.
        // Now, we have both streams at the correct location.
        auto terms_pos = file_start->terms_pos;
        terms.seekg(terms_pos);

        auto max_search_times = index.end() - file_start - 1;

        while (max_search_times--) {
            auto preview = Serializer::preview_work_index_entry(terms);
            if (preview.key > term || terms.bad()) {
                break;
            }

            if (preview.key == term) {
                return preview;
            }
        }
    }

    log("WARN Cannot get positions for term: ", term);
    return std::nullopt;
}

std::vector<DocumentPositionPointer>
SortedKeysIndexStub::get_positions_from_streampos(std::streampos freq, std::streampos pos) const {
    positions.seekg(pos);
    frequencies.seekg(freq);

    assert(positions.good());
    auto freq_list = MultiDocumentsTier::TierIterator(frequencies).read_all();
    return PositionsSearcher::read_positions_all(positions, freq_list);
}

// We assume that the positions of `terms` and `frequencies` are indetermined.
// Therefore, we seek to the correct location as determined by the file_start StubIndEntry,
// read the frequencies_pos, then seek the `frequencies` stream to that location.
// Now, we have both streams at the correct location.
void correct_freq_pos_locations(std::istream &terms, std::istream &frequencies) {
    auto terms_pos = terms.tellg();
    auto locations = Serializer::preview_work_index_entry(terms);

    frequencies.seekg(locations.frequencies_pos);
    // Seek back to original location for reading.
    terms.seekg(terms_pos);
}


TopDocs SortedKeysIndexStub::search_one_term(const std::string &term) const {
    auto file_start = std::lower_bound(index.begin(), index.end(), Base26Num(term).fiddle(-4));
    auto file_end = std::upper_bound(index.begin(), index.end(), Base26Num(term).fiddle(4));

    // Occurs when there's an empty index. No way to prefix-match, and we have to exit early.
    if (file_start >= index.end()) {
        log("ERROR: Cannot seek to index for search term ", term);
        return {};
    };

    if (file_end >= index.end()) file_end = index.end() - 1;

    // Want to support prefix searching, so we'll start from way before "term" is supposed to be, and look for all possible matches.
    // Looking at a whole range from before and after "term" to imitate fuzzy searching.
    // We can't decrement this when calling std::lower_bound because pointer overflow might occur, and the check
    // "file_start >= index.end()" would be faulty.
    if (file_start > index.begin()) file_start--;

    terms.seekg(file_start->terms_pos);
    correct_freq_pos_locations(terms, frequencies);

    std::vector<TopDocs> outputs;
    std::vector<double> output_score;
    outputs.reserve(50);

    while (true) {
        // Preview the WIE without loading everything into memory. Since we expect to do many more previews than actual reads,
        // and since majority of keys don't fit within our criteria, previewing reduces computation and memory.
        auto preview = Serializer::preview_work_index_entry(terms);
        // If the preview fits within the score cutoff, then we seek back to the previewed position and read the whole thing into memory
        // to process it.
        auto score = string_prefix_compare(term, preview.key);
        auto min_cutoff_score = compute_average(output_score.begin(), output_score.end());
        log("Matching " + preview.key + " with " + term);
        log("Score: ", score, "/", min_cutoff_score);
        if (score >= min_cutoff_score) {
            // Seek back to original previewed position.
            frequencies.seekg(preview.frequencies_pos);

            MultiDocumentsTier::TierIterator ti(frequencies);
            auto files = ti.read_all();

//            auto files = ti.read_next().value();


//            log("Matched term, searched term " +  preview.key + " " + term, "score:", score, "docs size:", files.size());
            auto tot_score = 0;
            for (auto &i : files) {
                i.document_freq = (std::log10(i.document_freq) + 1) * score;
                tot_score += i.document_freq;
            }

            TopDocs td(std::move(files));

            if (preview.key == term || score >= 16)
                td.add_term_str(
                        PossiblyMatchingTerm(std::move(preview.key), preview.positions_pos, preview.frequencies_pos, ti,
                                             score));

            // Early optimization -- if we find the word then just return
            // (Disable because it misses some matches).
//            if (preview.key == term) return td;

            output_score.emplace_back(score);
            outputs.push_back(std::move(td));
        }

        // Check the exit condition. Has to be placed after the comparing code because file_end is inclusive (it should also be processed).
        if (Base26Num(preview.key).num >= file_end->key.num) {
            break;
        }
    }


    if (outputs.empty()) {
        log("WARN: No terms found for ", term);
        return TopDocs{};
    };

    for (int i = 1; i < outputs.size(); i++) {
        outputs[0].append_multi(outputs[i]);
    }
    return outputs[0];
}


std::vector<TopDocs> SortedKeysIndexStub::search_many_terms(const std::vector<std::string> &terms) const {
    std::vector<TopDocs> all_outputs;
    all_outputs.reserve(terms.size());
    for (auto &term: terms) {
        auto result = this->search_one_term(term);

        for (auto &r : result) {
            if (r.document_id == 19669) {
                log("Document ", r.document_id, "contains " + term);
            }
        }

        all_outputs.push_back(std::move(result));


    };
    return all_outputs;
}


SortedKeysIndexStub::SortedKeysIndexStub(const std::string &suffix) :
        frequencies(std::ifstream(indice_files_dir / ("frequencies-" + suffix), std::ios_base::binary)),
        terms(std::ifstream(indice_files_dir / ("terms-" + suffix), std::ios_base::binary)),
        positions(std::ifstream(indice_files_dir / ("positions-" + suffix), std::ios_base::binary)),
        index(Serializer::read_sorted_keys_index_stub_v2(
                this->frequencies, this->terms)) {
    assert(this->frequencies && this->terms);
}
