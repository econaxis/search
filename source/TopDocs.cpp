#include "TopDocs.h"

void TopDocs::append_multi(TopDocs other) {
    const auto iend = other.end();
    const auto ibegin = other.begin();

    auto prev = size();
    auto addsize = iend - ibegin;

    std::vector<value_type> merged(prev + addsize);

    auto lastelem = merge_combine(ibegin, iend, begin(), end(), merged.begin());

    // Delete all other elements.
    merged.resize(lastelem - merged.begin());

    docs = std::move(merged);

    // Copy their included terms to ours.
    for(auto& [str, iter] : other.included_terms) add_term_str(str, iter);
}


// If our list contains two same document ID's, then add their scores and merge them into one.
// This is possible in prefix searching, when a document might have multiple words whose prefixes
// match the same query term.
// This also happens when we merge the TopDocs of two query terms, so documents containing both terms
// should be bonused.
void TopDocs::merge_similar_docs() {
    if (size() == 0) return;

    auto &prev_doc = *begin();
    auto collected_score = 0;

    bool deleted_any = false;

    // Merge similar docs.
    for (auto &doc : docs) {
        if (doc.document_id != prev_doc.document_id) {
            prev_doc.document_freq += collected_score;
            prev_doc = doc;
            collected_score = 0;
        } else {
            collected_score += doc.document_freq;

            // Invalidate the document's information.
            doc.document_freq = 0;
            doc.document_id = 0;
            deleted_any = true;
        }
        prev_doc = doc;
    }
    prev_doc.document_freq = collected_score;

    if (deleted_any)
        docs.erase(std::remove_if(begin(), end(), [](const auto &t) {
            return t.document_freq == 0 || t.document_id == 0;
        }), end());

}

// Partial sorts the TopDocs collection by frequency. Useful for the last stage of processing when we don't care about maintaining
// document id order, and instead want the top ranking documents.
void TopDocs::sort_by_frequencies() {
    auto partial_end = std::min(end(), begin() + 50);
    std::partial_sort(begin(), partial_end, end(), [](auto &t, auto &t1) {
        return t.document_freq < t1.document_freq;
    });
}
