//
// Created by henry on 2021-05-02.
//
#include <robin_hood/robin_hood.h>
#include <cassert>
#include "DocumentsMatcher.h"
#include <unordered_map>

constexpr int MAX_DOCUMENTS_PER_TERM = 100000;
constexpr int MAX_DOCUMENTS_RETURNED_AND = 200;


std::vector<SafeMultiSearchResult> parse_vec_from_map(std::unordered_map<uint32_t, MultiSearchResult> &&match_scores) {
    std::vector<SafeMultiSearchResult> document_search_results;

    for (auto&&[k, v] : match_scores) {
        document_search_results.emplace_back(std::move(v));
    }

    std::sort(document_search_results.begin(), document_search_results.end(), SafeMultiSearchResult::SortScore);
    return document_search_results;
}

template<typename Iterator, typename Callable>
void advance_to_next_unique_value(Iterator &it, const Callable &value_getter) {
    const auto &prev_value = value_getter(*it);
    while (value_getter(*it) == prev_value) { it++; };
}

/**
 * Returns all documents that exist in all search results.
 * @param results a vector that contains the documents matched for each term in the query.
 *         Thus, for a document to be returned in the query, it has to exist in all elements of the vector.
 * @return A condensed vector of search results present in all query terms.
 */
std::vector<SafeMultiSearchResult>
DocumentsMatcher::AND(std::vector<robin_hood::unordered_map<uint32_t, MultiSearchResult>> results) {
    // Checks for documents existing in ALL results vec.
    std::vector<SafeMultiSearchResult> linear_result;

    const auto min_set = std::min_element(results.begin(), results.end(), [](const auto &t1, const auto &t2) {
        return t1.size() < t2.size();
    });

    if (min_set == results.end()) return linear_result;

    linear_result.reserve(min_set->size() * 0.75);


    std::vector<robin_hood::unordered_map<uint32_t, MultiSearchResult>::iterator> walkers(results.size());

    for (const auto &[docid, msr] : *min_set) {
        bool exists_in_all = true;

        for (auto other = 0; other < results.size(); other++) {
            auto find = results[other].find(docid);

            if (find == results[other].end()) {
                exists_in_all = false;
                break;
            } else {
                walkers[other] = find;
            }
        }

        if (exists_in_all) {
            // Walk vector again to find the positions.
            auto &pos = linear_result.emplace_back(results.size() * 5);
            for (auto other = 0; other < results.size(); other++) {
                pos.score += walkers[other]->second.score;
                for (auto i : walkers[other]->second) {
                    pos.positions.emplace_back(i);
                }
            }
        }
    }

    if (linear_result.size() > 30) {
        std::partial_sort(linear_result.begin(), linear_result.begin() + 20, linear_result.end(),
                          [](const auto &t1, const auto &t2) {
                              return t1.score > t2.score;
                          });
    } else {
        std::sort(linear_result.begin(), linear_result.end(),
                  [](const auto &t1, const auto &t2) {
                      return t1.score > t2.score;
                  });
    }
    return linear_result;

//    return std::vector<MultiSearchResult>();
};

/**
 * Another overload for AND matching.
 * @param results vector of vector of search results. A document has to be present in all elements of the first vector to
 *      satisfy AND.
 * @param result_terms vector of words that were used for query. Length of the word used to determine score.
 */
std::vector<SafeMultiSearchResult> DocumentsMatcher::AND(const std::vector<const SearchResult *> &results,
                                                         const std::vector<std::string> &result_terms) {
    std::vector<SearchResult::const_iterator> result_idx;

    std::unordered_map<uint32_t, MultiSearchResult> match_scores;

    for (const auto &sr : results) {
        assert(std::is_sorted(sr->begin(), sr->end()));
    }
    auto must_have_term = std::min_element(results.begin(), results.end(),
                                           [](const SearchResult *t1, const SearchResult *t2) {
                                               return t1->size() < t2->size();
                                           });

    std::transform(results.begin(), results.end(), std::back_inserter(result_idx),
                   [](const auto &elem) { return elem->cbegin(); });

    if (must_have_term == results.end()) return {};

    auto startit = (**must_have_term).begin();
    while (startit < (**must_have_term).end()) {
        for (int a = 0; a < results.size(); a++) {
            auto[start, end] = std::equal_range(result_idx[a], results[a]->end(), *startit);

            result_idx[a] = end > start ? end - 1 : start;

            if (*startit < *start || start == results[a]->end()) {
                // Element doesn't exist;
                if (auto pos = match_scores.find(startit->document_id); pos != match_scores.end()) {
                    match_scores.erase(pos);
                }
                break;
            } else {
                auto score = (end - start) *
                             result_terms[a].size(); // Score is (num occurences) * (character length of term)
                auto pos = match_scores.find(startit->document_id);
                if (pos == match_scores.end()) {
                    match_scores.emplace(startit->document_id, MultiSearchResult(startit->document_id, score));
                    pos = match_scores.find(startit->document_id);
                }
                auto &scores_positions = pos->second;
                scores_positions.score += score;


                for (auto i = start; i < end; i++) {
                    scores_positions.insert_position({(uint32_t) score, i->document_position});
                }
            }
        }

        if (match_scores.size() > MAX_DOCUMENTS_RETURNED_AND) break;

        advance_to_next_unique_value(startit, [](const auto &t) { return t.document_id; });
    }

    return parse_vec_from_map(std::move(match_scores));

}


std::vector<SafeMultiSearchResult> DocumentsMatcher::OR(const std::vector<const SearchResult *> &results,
                                                        const std::vector<std::string> &result_terms) {
    std::unordered_map<uint32_t, MultiSearchResult> match_scores;
    for (int i = 0; i < results.size(); i++) {
        const auto *r = results[i];

        int cur_documents_processed = 0;

        for (const auto &dp : *r) {
            auto pos = match_scores.find(dp.document_id);
            auto score = result_terms[i].size();
            if (pos == match_scores.end()) {
                pos = match_scores.emplace(dp.document_id,
                                           MultiSearchResult(dp.document_id, score,
                                                             {(uint32_t) score, dp.document_position})).first;
            }
            pos->second.score += score;
            pos->second.insert_position({(uint32_t) score, dp.document_position});

            if (cur_documents_processed++ > MAX_DOCUMENTS_PER_TERM) break;
        };
    }

    return parse_vec_from_map(std::move(match_scores));
}

/**
 * Expects elements in sorted form.
 * @param results
 * @return
 */
TopDocs DocumentsMatcher::AND(std::vector<TopDocs> &results) {

    if (results.empty()) return TopDocs(0);

    auto min_set = std::min_element(results.begin(), results.end(), [](const auto &t1, const auto &t2) {
        return t1.size() < t2.size();
    });
    std::vector<decltype(results[0].cbegin())> walkers;
    for (auto &t: results) walkers.push_back(t.cbegin());

    for (auto &pair : *min_set) {
        bool exists_in_all = true;
        auto acculumated_score = 0;
        for (auto other = 0; other < results.size(); other++) {
            auto find = std::lower_bound(walkers[other], results[other].cend(), pair);
            if (find == results[other].cend() || find->document_id != pair.document_id) {
                exists_in_all = false;
                break;
            } else {
                walkers[other] = find;

                // Multiply the accumulated score by pair frequency.
                // Therefore, terms are advantaged for having high scores across all queries
                acculumated_score *= pair.frequency;
            }
        }

        if (exists_in_all) {
            // Walk vector again to find the positions.
            pair.frequency = acculumated_score;
        }
    }

    if (min_set->size() > 30) {
        std::partial_sort(min_set->begin(), min_set->begin() + 20, min_set->end());
    } else {
        std::sort(min_set->begin(), min_set->end());
    }
    return *min_set;

}
