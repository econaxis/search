#include <robin_hood/robin_hood.h>
#include <cassert>
#include "DocumentsMatcher.h"
#include "DocumentFrequency.h"
#include <unordered_map>
#include <fstream>
#include <bitset>

constexpr int MAX_DOCUMENTS_PER_TERM = 100000;
constexpr int MAX_DOCUMENTS_RETURNED_AND = 200;


template<typename Iterator, typename Callable>
void advance_to_next_unique_value(Iterator &it, const Callable &value_getter) {
    const auto &prev_value = value_getter(*it);
    while (value_getter(*it) == prev_value) { it++; };
}


#include <immintrin.h>

// Inspired from https://gms.tf/stdfind-and-memchr-optimizations.html#what-about-avx-512
// I adapted AVX2 code from finding bytes to finding 32 bit integers.
const uint32_t *find_avx_256(const uint32_t *start, const uint32_t *end, uint32_t value) {
    __m256i avxvalue = _mm256_set1_epi32(value);

    for (; start + 8 <= end; start += 8) {
        __m256i avxstart = _mm256_load_si256((__m256i *) start);
        __m256i comp = _mm256_cmpeq_epi32(avxstart, avxvalue);

        int movemask = _mm256_movemask_epi8(comp);

        if (movemask) {
            // __builtin_ffs returns the first bit that is 1.
            // Unfortunately, first bit is one-indexed, so we have to subtract 1.
            int firstfound = __builtin_ffs(movemask) - 1;

            // Since uint32's have 4 bytes each, and movemask is a mask of one byte, we have to divide by 4
            // to get the correct offset.
            firstfound /= 4;
            return start + firstfound;
        }
    }

    for (; start < end; start++) {
        if (*start == value) {
            return start;
        }
    }
    return nullptr;
}

#include <chrono>

using namespace std::chrono;

[[maybe_unused]] static unsigned int measure() {
    static auto lasttime = high_resolution_clock::now();
    unsigned int ret = duration_cast<nanoseconds>(high_resolution_clock::now() - lasttime).count();
    lasttime = high_resolution_clock::now();
    return ret;
}


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


TopDocs backup(std::vector<TopDocs> &results) {
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
TopDocs DocumentsMatcher::AND(std::vector<TopDocs> &results) {

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

    // Cutoff must be above the average score.
    for (auto pair = min_docs.begin(); pair != min_docs.end(); pair++) {
        bool exists_in_all = true;
        auto acculumated_score = 1UL;

        for (auto &[_result_size, idx] : sorted_sizes) {
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
                acculumated_score += (walkers[idx] - 1)->document_freq;
            } else {
                exists_in_all = false;
                break;
            }
        }

        if (exists_in_all) {
            // Walk vector again to find the positions.
            pair->document_freq = acculumated_score;
        } else {
            pair->document_freq = 0;
            pair->document_id = 0; // Exclude from view.
        }
    }

    exit_loop:;

    min_docs.docs.erase(std::remove(min_docs.begin(), min_docs.end(), DocumentFrequency{0, 0}),
                        min_docs.docs.end());

    if (min_docs.size() < 5) {
        return min_docs;
//        return backup(results);
    } else {
        return min_docs;

    }

}
