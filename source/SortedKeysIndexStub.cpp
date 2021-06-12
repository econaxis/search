#include "PositionsSearcher.h"
#include <iostream>
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"
#include "Base26Num.h"
#include "Serializer.h"
#include <cmath>
#include <numeric>
#include "Constants.h"
#include "DocumentFrequency.h"

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
            return 0;
        }
    }
    const auto score = ss * ss * divider;
    if (ss == ls) return MATCHALL_BONUS * score;
    else return score;
}


template<typename Iterator>
int compute_average(Iterator begin, Iterator end) {
    if (end - begin < 6) return 8;

    unsigned int sum = 0, square = 0;

    for (auto i = begin; i < end; i++) {
        sum += *i;
        square += *i * *i;
    }
    sum += end - begin;

    return square / sum;
}


std::optional<PreviewResult> SortedKeysIndexStub::seek_to_term(const std::string &term) const {
    auto file_start = std::lower_bound(index->begin(), index->end(), Base26Num(term));
    auto file_end = std::upper_bound(index->begin(), index->end(), Base26Num(term));

    if (file_start == index->end()) { return std::nullopt; }

    // We assume that the positions of `terms` and `frequencies` are indetermined.
    // Therefore, we seek to the correct location as determined by the file_start StubIndEntry,
    // read the frequencies_pos, then seek the `frequencies` stream to that location.
    // Now, we have both streams at the correct location.
    auto terms_pos = file_start->terms_pos;
    terms.seekg(terms_pos);

    while (terms.tellg() < file_end->terms_pos) {
        auto preview = Serializer::preview_work_index_entry(terms);
        if (preview.key.compare(term) == 0) {
            return preview;
        }
    }
    return std::nullopt;
}


std::vector<DocumentPositionPointer> SortedKeysIndexStub::get_positions_for_term(const std::string &term) const {
    auto loc = seek_to_term(term);
    if (!loc) {
        return {};
    } else {
        positions.seekg(loc->positions_pos);
        frequencies.seekg(loc->frequencies_pos);
        auto freq_list = MultiDocumentsTier::TierIterator(frequencies).read_all();
        return PositionsSearcher::read_positions_all(positions, freq_list);
    }
}


void SortedKeysIndexStub::rerank_by_positions(std::vector<TopDocs> &tds) {
    std::vector<std::vector<DocumentPositionPointer>> positions_list(tds.size());

    for (int i = 0; i < tds.size(); i++) {

        if (auto it = tds[i].get_first_term(); it) {
            positions_list[i] = get_positions_for_term(**it);
        } else {
            break;
        }

    }

}


TopDocs SortedKeysIndexStub::search_one_term(const std::string &term) const {
    auto file_start = std::lower_bound(index->begin(), index->end(), Base26Num(term).fiddle(-3)) - 1;
    auto file_end = std::upper_bound(index->begin(), index->end(), Base26Num(term).fiddle(3)) + 1;

    file_start = std::clamp(file_start, index->begin(), index->end() - 1);
    file_end = std::clamp(file_end, index->begin(), index->end() - 1);

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
//            auto size = Serializer::read_work_index_entry_v2_optimized(frequencies, alignedbuf.get());
//            auto wie = Serializer::read_work_index_entry_v2(frequencies, terms);

            auto wie = WordIndexEntry_v2{preview.key, static_cast<uint32_t>(terms.tellg()), {}};
            MultiDocumentsTier::TierIterator ti(frequencies);
            wie.files = ti.read_next().value();


            auto tot_score = 0;
            for (auto &i : wie.files) {
                float coefficient = std::log10(i.document_freq + 3) * 2 - 0.204;
                i.document_freq = coefficient * score;
                tot_score += i.document_freq;
            }
            TopDocs td(std::move(wie.files));

            // Only add high-ranking terms to the continue-list (for further retrieval if AND can't generate 50+ results)
            if (preview.key == term) td.add_term_str(preview.key, ti);
            output_score.emplace_back(tot_score / td.size());
            outputs.push_back(std::move(td));
        }
    }

    if (outputs.empty()) return TopDocs{};

    for (int i = 1; i < outputs.size(); i++) {
        // Append only words that are above average score, as determined by cutoff.
        outputs[0].append_multi(outputs[i]);
    }
    return outputs[0];
}


TopDocs SortedKeysIndexStub::search_many_terms(const std::vector<std::string> &terms) {
    std::vector<TopDocs> all_outputs;
    all_outputs.reserve(terms.size());

    for (auto &term: terms) {
        auto result = this->search_one_term(term);
        all_outputs.push_back(std::move(result));
    };

    auto all_outputs_backup = all_outputs;

    auto ret = DocumentsMatcher::AND(all_outputs);
    auto max_iter = 20;
    while (ret.size() < 10 && max_iter--) {
        bool has_more = false;
        for (auto &td : all_outputs) {
            if (td.extend_from_tier_iterator(3)) has_more = true;
        }

        if (!has_more) break;
        else {
            ret = DocumentsMatcher::AND(all_outputs);
        }
    }
    static int backup = 0;
    static int nobackup = 0;
    static float avgmaxiter = 10;
    if (ret.size() < 10) {
        backup++;
        avgmaxiter += max_iter;
        avgmaxiter /= 2;

        return DocumentsMatcher::backup(all_outputs_backup);
    } else {
        nobackup++;
//        rerank_by_positions(all_outputs);
        if (nobackup % 100 == 0) std::cout << backup << " " << nobackup << " " << avgmaxiter << "\n";


        return ret;
    }
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


SortedKeysIndexStub::SortedKeysIndexStub(const SortedKeysIndexStub &other) : filemap(
        indice_files_dir / ("filemap-" + other.suffix)) {
    frequencies = std::ifstream(indice_files_dir / ("frequencies-" + other.suffix), std::ios_base::binary);
    terms = std::ifstream(indice_files_dir / ("terms-" + other.suffix), std::ios_base::binary);
    positions = std::ifstream(indice_files_dir / ("positions-" + other.suffix), std::ios_base::binary);
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

std::string SortedKeysIndexStub::query_filemap(uint32_t docid) const {
    auto ret = filemap.query(docid);
    return ret;
}
