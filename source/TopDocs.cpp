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
    std::move(other.included_terms.begin(), other.included_terms.end(), std::back_inserter(included_terms));
}



// Partial sorts the TopDocs collection by frequency. Useful for the last stage of processing when we don't care about maintaining
// document id order, and instead want the top ranking documents.
void TopDocs::sort_by_frequencies() {
    auto partial_end = std::min(end(), begin() + 50);
    std::partial_sort(begin(), partial_end, end(), [](auto &t, auto &t1) {
        return t.document_freq < t1.document_freq;
    });
}

bool TopDocs::extend_from_tier_iterator(int how_many) {
    std::vector<DocumentFrequency> extended;
    extended.resize(how_many * MultiDocumentsTier::BLOCKSIZE * 2 * included_terms.size());
    auto extended1 = extended;

    auto ptr = &extended;
    auto lastelem = ptr->begin();

    // Use a "double-buffering" approach to merging many sorted lists.
    // Reduces number of dynamic allocations in the loop.
    auto flip = [&]() {
        if (ptr == &extended) ptr = &extended1;
        else if (ptr == &extended1) ptr = &extended;
        else throw std::runtime_error("Ptr not extended or extended1");
        return ptr;
    };

    bool has_more = false;
    for (auto& possible_matching_term : included_terms) {
        for (int i = 0; i < how_many; i++) {
            auto n = possible_matching_term.extend(1);
            if (n) {
                auto oldrange = ptr;
                auto newrange = flip();

                assert(lastelem -oldrange->begin() + n->size() <= newrange->size());

                lastelem = std::merge(oldrange->begin(), lastelem, n->begin(), n->end(), newrange->begin());
                has_more = true;
            } else {
                break;
            }
        }
    }

    // If we encountered the last element, then the actual "valid" size will be less than buffer size
    auto size = lastelem - ptr->begin();
    ptr->resize(size);
    append_multi(TopDocs(std::move(*ptr)));
    return has_more;
}

std::optional<const char *> TopDocs::get_first_term() const {
    if(included_terms.empty()) {
        return std::nullopt;
    } else return included_terms.front().term.data();
}
