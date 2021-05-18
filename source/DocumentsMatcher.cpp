#include <robin_hood/robin_hood.h>
#include <cassert>
#include "DocumentsMatcher.h"
#include <unordered_map>

constexpr int MAX_DOCUMENTS_PER_TERM = 100000;
constexpr int MAX_DOCUMENTS_RETURNED_AND = 200;


template<typename Iterator, typename Callable>
void advance_to_next_unique_value(Iterator &it, const Callable &value_getter) {
    const auto &prev_value = value_getter(*it);
    while (value_getter(*it) == prev_value) { it++; };
}

std::vector<robin_hood::unordered_set<uint32_t>> convert_to_hashset(std::vector<TopDocs> &results) {
    std::vector<robin_hood::unordered_set<uint32_t>> sets;

    for (TopDocs &td : results) {
        robin_hood::unordered_set<uint32_t> set;
        set.reserve(td.size());
        for(auto& [docid, freq] : td) {
            set.insert(docid);
        }
        sets.push_back(std::move(set));
    }
    return sets;
}


/**
 * Expects elements in sorted form.
 * @param results
 * @return
 */
TopDocs DocumentsMatcher::AND(std::vector<TopDocs> &results) {

    if (results.empty()) return TopDocs{};
    if (results.size() == 1) return results[0];

//    auto sets = convert_to_hashset(results);

    auto min_docs_list = std::min_element(results.begin(), results.end(), [](const auto &t1, const auto &t2) {
        return t1.size() < t2.size();
    });
    std::vector<decltype(results[0].begin())> walkers;
    for (auto &t: results) walkers.push_back(t.begin());

    for (auto &pair : *min_docs_list) {
        bool exists_in_all = true;
        auto acculumated_score = 1UL;
        for (auto other = 0; other < results.size(); other++) {

            auto find = std::upper_bound(walkers[other], results[other].end(), pair) - 1;
            if (find == results[other].end() || find->document_id != pair.document_id) {
//            auto find = sets[other].find(pair.document_id);
//            if(find == sets[other].end()) {
                exists_in_all = false;
                break;
            } else {

                // Multiply the accumulated score by pair frequency.
                // Therefore, terms are advantaged for having high scores across all queries
                acculumated_score *= pair.frequency;
            }
            walkers[other] = find + 1;
        }

        if (exists_in_all) {
            // Walk vector again to find the positions.
            pair.frequency = acculumated_score;
        } else {
            pair.frequency = 0;
            pair.document_id = 0; // Exclude from view.
        }
    }

    min_docs_list->docs.erase(std::remove_if(min_docs_list->begin(), min_docs_list->end(), [](auto& elem) {
        return (elem.frequency == 0) || (elem.document_id) == 0;
    }), min_docs_list->end());

    if (min_docs_list->size() > 30) {
        std::partial_sort(min_docs_list->begin(), min_docs_list->begin() + 30, min_docs_list->end());
    } else {
        std::sort(min_docs_list->begin(), min_docs_list->end());
    }
    return *min_docs_list;

}
