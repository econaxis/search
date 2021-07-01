#ifndef GAME_TOPDOCS_H
#define GAME_TOPDOCS_H

#include <cstring>
#include <vector>
#include <cassert>
#include "DocumentFrequency.h"
#include "DocumentFrequency.h"
#include <type_traits>
#include <unordered_map>
#include "DocumentsTier.h"


class TopDocs {

    std::unordered_map<std::string, MultiDocumentsTier::TierIterator> included_terms;


    // From https://en.cppreference.com/w/cpp/algorithm/merge
    template<class InputIt1, class InputIt2, class OutputIt>
    static OutputIt merge_combine(InputIt1 first1, InputIt1 last1,
                                  InputIt2 first2, InputIt2 last2,
                                  OutputIt d_first);

public:
    std::vector<DocumentFrequency> docs;
    using value_type = DocumentFrequency;

    TopDocs() = default;


    void add_term_str(const std::string &term, MultiDocumentsTier::TierIterator it) {
        included_terms.emplace(std::move(term), it);
    }

    std::optional<const char *> get_first_term() const;

    bool extend_from_tier_iterator(int how_many);

    explicit TopDocs(std::vector<DocumentFrequency> docs) : docs(std::move(docs)) {};

    TopDocs(value_type *ibegin, value_type *iend) {
        docs.resize(iend - ibegin);
        std::memcpy(docs.data(), ibegin, (iend - ibegin) * sizeof(value_type));
    }

    // Iterator implementations to match the interface of vector
    // Also allows for range based for loops, which is convenient.
    std::vector<value_type>::const_iterator begin() const { return docs.begin(); }

    std::vector<value_type>::const_iterator end() const { return docs.end(); }

    std::vector<value_type>::iterator begin() { return docs.begin(); }

    std::vector<value_type>::iterator end() { return docs.end(); }

    std::size_t size() const { return docs.size(); }

    // Merge another TopDocs with our list of TopDocs.
    // Maintains sorted order by document id
    void append_multi(TopDocs other);


    // If our list contains two same document ID's, then add their scores and merge them into one.
    // This is possible in prefix searching, when a document might have multiple words whose prefixes
    // match the same query term.
    // This also happens when we merge the TopDocs of two query terms, so documents containing both terms
    // should be bonused.
    // Note: maybe not needed since `append_multi` doesn't allow duplicates
    void merge_similar_docs();


    // Normally, we maintain sorted order by document_id.
    // However, when we want to view the results, we would rather have a frequencies-sorted list.
    void sort_by_frequencies();

};

template<class InputIt1, class InputIt2, class OutputIt>
OutputIt TopDocs::merge_combine(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, OutputIt d_first) {

    while (first1 != last1) {
        if (first2 == last2) {
            d_first = std::copy(first1, last1, d_first);
            return d_first;
        }
        if (*first2 < *first1) {

            *d_first = *first2;
            d_first++;
            ++first2;
        } else if (*first1 < *first2) {
            *d_first = *first1;
            d_first++;
            ++first1;
        } else {
            // They are equals.
            auto merged = *first1;
            merged.document_freq += (*first2).document_freq;

            *d_first = merged;
            d_first++;
            ++first1;
            ++first2;
        }
    }
    d_first = std::copy(first2, last2, d_first);
    return d_first;
}


#endif //GAME_TOPDOCS_H
