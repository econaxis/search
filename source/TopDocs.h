#ifndef GAME_TOPDOCS_H
#define GAME_TOPDOCS_H

#include <vector>
#include "DocumentFrequency.h"
#include <unordered_set>
#include "PossiblyMatchingTerm.h"

class TopDocs {
private:
    std::vector<PossiblyMatchingTerm> included_terms;
    std::vector<DocumentFrequency> docs;

public:
    using value_type = DocumentFrequency;

    std::string as_string() const;

    TopDocs() = default;

    void add_term_str(PossiblyMatchingTerm term) {
        included_terms.push_back(std::move(term));
    }

    std::unordered_set<uint32_t> get_id_hashset() {
        auto set = std::unordered_set<uint32_t>();
        for (auto& j : docs) {
            set.insert(j.document_id);
        }
        return set;
    }

    std::optional<PossiblyMatchingTerm> pop_next_term();

    // We're using tiered postings list. This function extends the current postings list to include the next tier.
    bool extend_from_tier_iterators();

    explicit TopDocs(std::vector<DocumentFrequency> docs) : docs(std::move(docs)) {};


    // Merge another TopDocs with our list of TopDocs.
    // Maintains sorted order by document id
    void append_multi(TopDocs other);


    // If our list contains two same document ID's, then add their scores and merge them into one.
    // This is possible in prefix searching, when a document might have multiple words whose prefixes
    // match the same query term.
    // This also happens when we merge the TopDocs of two query terms, so documents containing both terms
    // should be bonused.
    // Note: maybe not needed since `append_multi` doesn't allow duplicates
//    void merge_similar_docs();


    // Normally, we maintain sorted order by document_id.
    // However, when we want to view the results, we would rather have a frequencies-sorted list.
    void sort_by_frequencies();

    const std::vector<DocumentFrequency>& get_inner() const {return docs;};


    // ==========================
    // Quality-of-life iterator implementations. Needed for range-based for loops.
    std::vector<value_type>::const_iterator begin() const { return docs.begin(); }
    std::vector<value_type>::const_iterator end() const { return docs.end(); }
    std::vector<value_type>::iterator begin() { return docs.begin(); }
    std::vector<value_type>::iterator end() { return docs.end(); }
    std::size_t size() const { return docs.size(); }



    static inline std::vector<DocumentFrequency> into_docs(TopDocs td) {
        return td.docs;
    }
};



#endif //GAME_TOPDOCS_H
