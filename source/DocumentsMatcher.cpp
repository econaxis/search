#include <robin_hood/robin_hood.h>
#include <cassert>
#include "DocumentsMatcher.h"
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

constexpr int bitsetsize = 10000;

std::bitset<bitsetsize> convert_to_hashset(TopDocs &td) {
    std::bitset<bitsetsize> bit;
    for (auto&[docid, freq] : td) {
        bit.set(docid % bitsetsize);
    }
    return bit;
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


using DPP = DocumentPositionPointer_v2;

int stats[7] = {0, 0, 0, 0, 0, 0, 0};

static const DPP *run_prediction(const DPP *&start, const DPP *end, const DPP *value) {
    auto prediction = end;

    auto difference = end - start;

    if (difference > 3 && *value < *(start + 2)) {
        prediction = start + 2;
    } else if (difference > 65 && *value < *(start + 64)) {
        prediction = start + 64;
    } else if (difference > 129 && *value < *(start + 128)) {
        prediction = start + 128;
    } else if (difference > 513 && *value < *(start + 512)) {
        prediction = start + 512;
    } else if (difference > 1025 && *value < *(start + 1024)) {
        prediction = start + 1024;
    } else if (difference > 4097 && *value < *(start + 4096)) {
        prediction = start + 4096;
    }


    return prediction;
}

bool unroll_binary_search_find(const DocumentPositionPointer_v2 *&begin, const DocumentPositionPointer_v2 *end,
                               const DocumentPositionPointer_v2 *value) {
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
        results[0].append_multi(results[i].begin(), results[i].end(), true);
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
    std::vector<__m256i> docidbuf;

    if (results.empty()) return TopDocs{};
    if (results.size() == 1) return results[0];

    // Stores <set size, index>
    std::vector<std::pair<int, int>> sorted_sizes;
    int totsize = 0;
    for (int i = 0; i < results.size(); i++) {
        sorted_sizes.emplace_back(results[i].size(), i);
        totsize += results[i].size();
    }

    docidbuf.resize(totsize / 2 / 16);
    std::sort(sorted_sizes.begin(), sorted_sizes.end());


    std::vector<const TopDocs::value_type *> walkers, enders;
    for (auto &t: results) {
//        auto beg = (uint32_t *) t.begin().base();
//        auto end = (uint32_t *) t.end().base();
//        for (auto i = beg; i < end; i += 32) {
//            __m256i first = _mm256_loadu_si256((__m256i *) i);
//            __m256i second = _mm256_loadu_si256((__m256i *) (i+8));
//            __m256i third = _mm256_loadu_si256((__m256i *) (i + 16));
//            __m256i fourth = _mm256_loadu_si256((__m256i *) (i+24));
//            __m256i packed = _mm256_packus_epi32(first, second);
//            __m256i packed1 = _mm256_packus_epi32(third, fourth);
//            __m256i permuted = _mm256_permute4x64_epi64(packed, 0b00100111);
//            __m256i permuted1 = _mm256_permute4x64_epi64(packed1, 0b00100111);
//            __m256i joined_all = _mm256_packus_epi32(permuted, permuted1);
//            __m256i reordered = _mm256_permute4x64_epi64(joined_all, 0b00100111);
//
//            _mm256_store_si256(cur_iterator, reordered);
//            cur_iterator++;
//        }

        walkers.push_back(t.begin().base());
        enders.push_back(t.end().base());
    }
    auto &min_docs = results[sorted_sizes[0].second];

    if (!min_docs.size()) return TopDocs();

    uint64_t score_cutoff = 0;
    if (min_docs.size() > 700) {
        for (auto &td: min_docs) score_cutoff += td.frequency;
        score_cutoff /= min_docs.size();
    }

    // Cutoff must be above the average score.
    for (auto pair = min_docs.begin(); pair != min_docs.end(); pair++) {
        bool exists_in_all = true;
        auto acculumated_score = 1UL;

        if (pair->frequency > score_cutoff) {
            for (auto &[_result_size, idx] : sorted_sizes) {
                if (walkers[idx] >= enders[idx]) {
                    // Exhausted one means exhausted all.
                    goto exit_loop;
                }

                if (unroll_binary_search_find(walkers[idx], enders[idx], pair.base())) {
                    // Multiply the accumulated score by pair frequency.
                    // Therefore, terms are advantaged for having high scores across all queries

                    // Add a bonus for document matching more than one query term.
                    // Add the frequency of the found term.
                    acculumated_score += (walkers[idx] - 1)->frequency + 30;
                } else {
                    exists_in_all = false;
                    break;
                }
            }
        }

        if (exists_in_all) {
            // Walk vector again to find the positions.
            pair->frequency = acculumated_score;
        } else {
            pair->frequency = 0;
            pair->document_id = 0; // Exclude from view.
        }
    }

    exit_loop:;

    min_docs.docs.erase(std::remove(min_docs.begin(), min_docs.end(), DocumentPositionPointer_v2{0, 0}),
                        min_docs.docs.end());

    if (min_docs.size() < 5) {
//        return min_docs;
        std::cout<<"Using backup\n";
        return backup(results);
    } else {
        return min_docs;

    }

}
