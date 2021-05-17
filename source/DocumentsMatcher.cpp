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



/**
 * Expects elements in sorted form.
 * @param results
 * @return
 */
TopDocs DocumentsMatcher::AND(std::vector<TopDocs> &results) {

    if (results.empty()) return TopDocs(0);
    if (results.size() == 1) return results[0];

    auto min_set = std::min_element(results.begin(), results.end(), [](const auto &t1, const auto &t2) {
        return t1.size() < t2.size();
    });
    std::vector<decltype(results[0].cbegin())> walkers;
    for (auto &t: results) walkers.push_back(t.cbegin());

    for (auto &pair : *min_set) {
        bool exists_in_all = true;
        auto acculumated_score = 1UL;
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
        std::partial_sort(min_set->begin(), min_set->begin() + 30, min_set->end());
    } else {
        std::sort(min_set->begin(), min_set->end());
    }
    return *min_set;

}
