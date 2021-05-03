//
// Created by henry on 2021-05-02.
//

#include <cassert>
#include "DocumentsMatcher.h"
#include <unordered_map>

constexpr int MAX_DOCUMENTS_PER_TERM = 100000;
constexpr int MAX_DOCUMENTS_RETURNED_AND = 200;


std::vector<MultiSearchResult> parse_vec_from_map(std::unordered_map<uint32_t, MultiSearchResult> &match_scores) {
    std::vector<MultiSearchResult> document_search_results(match_scores.size());

    std::transform(std::make_move_iterator(match_scores.begin()), std::make_move_iterator(match_scores.end()),
                   document_search_results.begin(),
                   [](auto &&match_scores_pair) {
                       return match_scores_pair.second;
                   });

    std::sort(document_search_results.begin(), document_search_results.end(), MultiSearchResult::SortScore);
    return document_search_results;
}

template<typename Iterator, typename Callable>
void advance_to_next_unique_value(Iterator &it, const Callable &value_getter) {
    const auto &prev_value = value_getter(*it);
    while (value_getter(*it) == prev_value) { it++; };
}

std::vector<MultiSearchResult> DocumentsMatcher::AND(const std::vector<const SearchResult *> &results,
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
                   [](const auto &elem) { return elem->begin(); });

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
                    match_scores.emplace(startit->document_id, MultiSearchResult(startit->document_id, score, {}));
                    pos = match_scores.find(startit->document_id);
                }
                auto &scores_positions = pos->second;
                scores_positions.score += score;

                auto &matching_positions_for_docid = scores_positions.positions;

                std::transform(start, end, std::back_inserter(matching_positions_for_docid),
                               [](const auto &a) {
                                   return a.document_position;
                               }); // Copy all document positions into the vector

            }
        }

        if(match_scores.size() > MAX_DOCUMENTS_RETURNED_AND) break;

        advance_to_next_unique_value(startit, [](const auto &t) { return t.document_id; });
    }

    return parse_vec_from_map(match_scores);

}


std::vector<MultiSearchResult> DocumentsMatcher::OR(const std::vector<const SearchResult *> &results,
                                                    const std::vector<std::string> &result_terms) {
    std::unordered_map<uint32_t, MultiSearchResult> match_scores;
    for (int i = 0; i < results.size(); i++) {
        const auto *r = results[i];

        int cur_documents_processed = 0;

        for(const auto& dp : *r) {
            auto pos = match_scores.find(dp.document_id);
            auto score = result_terms[i].size();
            if (pos == match_scores.end()) {
                pos = match_scores.emplace(dp.document_id,
                                           MultiSearchResult(dp.document_id, score, {dp.document_position})).first;
            }
            pos->second.score += score;
            pos->second.positions.push_back(dp.document_position);

            if(cur_documents_processed++ > MAX_DOCUMENTS_PER_TERM) break;
        };
    }

    return parse_vec_from_map(match_scores);
};
