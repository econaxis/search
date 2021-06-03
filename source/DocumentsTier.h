// In an index, it represents a single tier (or document-group).
// Each term in an index has many document tiers, which are groups of documents.
// Allows tiered sorting. Document tiers at the beginning are higher scored. Each document tier is sorted by docid.

#ifndef GAME_DOCUMENTSTIER_H
#define GAME_DOCUMENTSTIER_H

#include <iterator>
#include <type_traits>
#include <vector>
#include <ostream>
#include <cassert>

struct DocumentFrequency {
    uint32_t document_id;
    uint32_t document_freq;

    DocumentFrequency(uint32_t a, uint32_t b) : document_id(a), document_freq(b) {};

    bool operator<(const DocumentFrequency &other) const {
        return document_id < other.document_id;
    }

    static bool FreqSorter(const DocumentFrequency &one, const DocumentFrequency &two) {
        return one.document_freq < two.document_freq;
    }
};

struct SingleDocumentsTier {
    std::vector<DocumentFrequency> data;

    SingleDocumentsTier() = default;

    // Iterator constructor
    // Enables construction from a slice of a range
    template<class I>
    SingleDocumentsTier(I begin, I end) {
        data = std::vector<DocumentFrequency>{begin, end};
    }
    std::size_t size() const {
        return data.size();
    }
};

class WordIndexEntry;

// Converts a long, flat list of DocumentFrequency objects to a multi-level, tiered list.
// Based on Strohman and Croft tiered-index paper.
namespace MultiDocumentsTier {
    struct TierIterator {
        int remaining;
        std::streampos read_position;
        std::istream& frequencies;

        std::optional<SingleDocumentsTier> read_next();
    };

    // Constructs a MultiDocumentsTier instance and serializes it.
    void serialize(const WordIndexEntry &wie, std::ostream &frequencies);
};


#endif //GAME_DOCUMENTSTIER_H
