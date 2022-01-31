#include <robin_hood/robin_hood.h>
#include <cassert>
#include "DocumentsMatcher.h"
#include "DocumentFrequency.h"
#include "PositionsSearcher.h"
#include <unordered_map>
#include <bitset>

// TODO: option to disable position searching (maybe in the pipeline?)
// TODO: better testability
// TODO: option to disable prefix-based searches


#include <immintrin.h>

using namespace DocumentsMatcher;

using DPP = DocumentFrequency;

static const DPP *run_prediction(const DPP *&start, const DPP *end, const DPP *value) {
    auto prediction = end;

    auto difference = end - start - 1;

    // algorithm similar to galloping or exponential search.
    // we use galloping to find the most optimal right bound of the binary search.
    if (difference > 2 && *value < *(start + 2)) {
        prediction = start + 2;
    } else if (difference > 64 && *value < *(start + 64)) {
        prediction = start + 64;
    } else if (difference > 128 && *value < *(start + 128)) {
        prediction = start + 128;
    } else if (difference > 512 && *value < *(start + 512)) {
        prediction = start + 512;
    } else if (difference > 1024 && *value < *(start + 1024)) {
        prediction = start + 1024;
    } else if (difference > 4096 && *value < *(start + 4096)) {
        prediction = start + 4096;
    }


    return prediction;
}

bool unroll_binary_search_find(const DocumentFrequency *&begin, const DocumentFrequency *end,
                               const DocumentFrequency *value) {
    auto *optfind = begin;

    // Manually code up first and second positions, they happen 90% of the time.
    if (begin->document_id == value->document_id) {
    } else {
        auto *optimized_end = run_prediction(begin, end, value);
        optfind =
                std::upper_bound(begin, optimized_end, value, [](const auto *t1, const auto &t2) {
                    return t1->document_id < t2.document_id;
                }) - 1;


        if (optfind->document_id != value->document_id) {
            begin = optfind + 1;
            return false;
        }
    }
    begin = optfind + 1;
    return true;
}


TopDocs DocumentsMatcher::backup(std::vector<TopDocs> &results) {
    for (int i = 1; i < results.size(); i++) {
        results[0].append_multi(results[i]);
    }
    results[0].sort_by_frequencies();
    return results[0];
}

/**
 * Expects elements in sorted form.
 * @param results
 * @return
 */
TopDocs AND(std::vector<TopDocs> &results) {
    //TODO: bug! weird bug where things that still don't match all results lists still show up.
    // when [177, 300, ...] and [304, ...], 177 matches when it shouldn't. maybe because its the first term?

    if (results.empty()) return TopDocs{};
    if (results.size() == 1) return results[0];

    // Stores <set size, index>
    std::vector<std::pair<int, int>> sorted_sizes;
    int totsize = 0;
    for (int i = 0; i < results.size(); i++) {
        sorted_sizes.emplace_back(results[i].size(), i);
        totsize += results[i].size();
    }

    std::sort(sorted_sizes.begin(), sorted_sizes.end());


    std::vector<const TopDocs::value_type *> walkers, enders;
    for (auto &t: results) {
        walkers.push_back(t.begin().base());
        enders.push_back(t.end().base());
    }
    auto &min_docs = results[sorted_sizes[0].second];

    if (!min_docs.size()) return TopDocs();

    // The underlying container type of `TopDocs`, so we can manipulate it easily.
    std::vector<TopDocs::value_type> accepted_list;

    // Cutoff must be above the average score.
    for (auto pair = min_docs.begin(); pair != min_docs.end(); pair++) {
        bool exists_in_all = true;
        double acculumated_score = 0;

        for (auto &[_, idx] : sorted_sizes) {
            if (walkers[idx] >= enders[idx]) {
                // Exhausted one means exhausted all.
                // Have to use goto to exit out of two loop levels (alternative would be checking a boolean)
                goto exit_loop;
            }

            if (unroll_binary_search_find(walkers[idx], enders[idx], pair.base())) {
                // Multiply the accumulated score by pair frequency.
                // Therefore, terms are advantaged for having high scores across all queries

                // Add a bonus for document matching more than one query term.
                // Add the frequency of the found term.
                acculumated_score += (walkers[idx] - 1)->document_freq * 1.3;
            } else {
                exists_in_all = false;
                break;
            }
        }

        if (exists_in_all) {
            // Walk vector again to find the positions.
            accepted_list.emplace_back(pair->document_id, acculumated_score);
        }
    }
    exit_loop:;

    log("AND query final number of elements: ", accepted_list.size());

    return TopDocs(std::move(accepted_list));

}

TopDocs DocumentsMatcher::OR(std::vector<TopDocs>& outputs) {
    return backup(outputs);
}
TopDocs DocumentsMatcher::AND_Driver(std::vector<TopDocs> &outputs) {
    auto ret = AND(outputs);
    while (ret.size() < 50) {
        std::cout<<"Extending more\n";
        bool has_more = false;
        for (auto &td : outputs) {
            if (td.extend_from_tier_iterators()) {
                has_more = true;
                log("search extended from tier iterator once");
            }
        }
        if (!has_more) break;
        else {
            ret = AND(outputs);
        }
    }
    ret.sort_by_frequencies();
    return ret;
}

constexpr char PLACEHOLDER[] = "(null word)";

//TopDocsWithPositions
//DocumentsMatcher::combiner_with_position(const SortedKeysIndexStub &index, std::vector<TopDocs> &outputs,
//                                         const std::vector<std::string> &query_terms) {
//
//    // We'll do operations on outputs, adding to it lesser-frequencied documents.
//    // If we can't find enough documents that match an AND boolean query, then we'll
//    // switch to the backup OR boolean query. When we do OR, we only want the top documents matching.
//    // Thus, we copy this and back it up, in case we need to do an OR on the original,
//    // high-frequencied document set.
//
//    auto outputs_backup = outputs;
//    auto ret = DocumentsMatcher::AND_Driver(outputs);
//
//    if (ret.size() == 0 && outputs.size() > 1) {
//        auto str1 = outputs[0].get_first_term().value_or(PLACEHOLDER);
//        auto str2 = outputs[1].get_first_term().value_or(PLACEHOLDER);
//        log("Warning: using OR backup ", str1, str2);
//        return TopDocsWithPositions(DocumentsMatcher::backup(outputs_backup));
//    } else {
//        // We get a positions matrix of all the document positions for each term in the query
//        auto pos_mat = PositionsSearcher::fill_positions_from_docs(index, query_terms);
//
//        // Use that position matrix to rerank/boost documents if they have proximal matching terms.
//        return PositionsSearcher::rerank_by_positions(pos_mat, ret, query_terms);
//    }
//}

TopDocsWithPositions::Elem::Elem(unsigned int i, unsigned int i1) : document_id(i), document_freq(i1) {}

