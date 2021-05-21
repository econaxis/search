
#ifndef GAME_TOPDOCS_H
#define GAME_TOPDOCS_H

#include <cstring>
#include <vector>
#include <cassert>
#include "DocumentPositionPointer.h"
#include "CustomAllocatedVec.h"
#include <type_traits>


class TopDocs {


    // From https://en.cppreference.com/w/cpp/algorithm/merge
    template<class InputIt1, class OutputIt>
    static OutputIt merge_combine(InputIt1 first1, InputIt1 last1,
                                  InputIt1 first2, InputIt1 last2,
                                  OutputIt d_first, bool should_limit_top30 = false) {
        std::array<uint32_t, 30> score_heap {};

        auto reverse_comp = [](auto t1, auto t2) { return t1 > t2; };

        // Make a reverse heap (min-heap)
        std::make_heap(score_heap.begin(), score_heap.end(), reverse_comp);
        for (; first1 != last1; ++d_first) {
            if (first2 == last2) {
                return std::copy(first1, last1, d_first);
            }
            if (*first2 < *first1) {

                if (should_limit_top30 && score_heap[0] < first2->frequency) {
                    std::pop_heap(score_heap.begin(), score_heap.end(), reverse_comp);
                    *d_first = *first2;
                    score_heap.back() = first2->frequency;
                    std::push_heap(score_heap.begin(), score_heap.end(), reverse_comp);
                }
                ++first2;
            } else if (*first1 < *first2) {
                if (should_limit_top30 && score_heap[0] < first1->frequency) {
                    std::pop_heap(score_heap.begin(), score_heap.end(), reverse_comp);
                    *d_first = *first1;
                    score_heap.back() = first1->frequency;
                    std::push_heap(score_heap.begin(), score_heap.end(), reverse_comp);
                }
                ++first1;
            } else {
                // They are equals.
                DocumentPositionPointer_v2 merged = *first1;
                merged.frequency += (*first2).frequency;

                if (should_limit_top30 && score_heap[0] < merged.frequency) {
                    std::pop_heap(score_heap.begin(), score_heap.end(), reverse_comp);
                    *d_first = *first2;
                    score_heap.back() = first2->frequency;
                    std::push_heap(score_heap.begin(), score_heap.end(), reverse_comp);
                }
                ++first1;
                ++first2;
            }
        }
        std::copy(first2, last2, d_first);
        return d_first;
    }

public:
    std::vector<DocumentPositionPointer_v2> docs;
    using value_type = DocumentPositionPointer_v2;

    TopDocs(int reservation = 50) {

    };


    std::vector<value_type>::const_iterator begin() const {
        return docs.begin();
    }

    template<typename Iterator>
    TopDocs(Iterator ibegin, Iterator iend) {
        append_multi(ibegin, iend);
    }

    TopDocs(value_type *ibegin, value_type *iend) {
        docs.resize(iend - ibegin);
        std::memcpy(docs.data(), ibegin, (iend - ibegin) * sizeof(value_type));
    }


    std::vector<value_type>::const_iterator end() const { return docs.end(); }

    std::vector<value_type>::iterator begin() { return docs.begin(); }

    std::vector<value_type>::iterator end() { return docs.end(); }

    std::size_t size() const { return docs.size(); }

    template<typename Iterator>
    void append_multi(Iterator ibegin, Iterator iend, bool limit = false) {
        auto prev = size();
        auto addsize = iend - ibegin;

        std::vector<value_type> merged(prev + addsize);

        auto lastelem = merge_combine(ibegin, iend, begin(), end(), merged.begin(), limit);

        // Delete all other elements.
        merged.resize(lastelem - merged.begin());

        docs = std::move(merged);
    }

    void clear() {
        docs.clear();
    }


    void merge_similar_docs() {
        if (size() == 0) return;

        auto &prev_doc = *begin();
        auto collected_score = 0;

        bool deleted_any = false;

        // Merge similar docs.
        for (auto &doc : docs) {
            if (doc.document_id != prev_doc.document_id) {
                prev_doc.frequency += collected_score;
                prev_doc = doc;
                collected_score = 0;
            } else {
                collected_score += doc.frequency;
                doc.frequency = 0;
                doc.document_id = 0;
                deleted_any = true;
            }
            prev_doc = doc;
        }
        prev_doc.frequency = collected_score;

        if (deleted_any)
            docs.erase(std::remove_if(begin(), end(), [](const auto &t) {
                return t.frequency == 0 || t.document_id == 0;
            }), end());

    }

    void sort_by_frequencies() {

        if (size() > 30) {
            std::partial_sort(begin(), begin() + 30, end(), [](auto &t, auto &t1) {
                return t.frequency < t1.frequency;
            });
        } else {
            std::sort(begin(), end(), [](auto &t, auto &t1) {
                return t.frequency < t1.frequency;
            });

        }

    }

};


#endif //GAME_TOPDOCS_H
