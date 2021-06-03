// In an index, it represents a single tier (or document-group).
// Each term in an index has many document tiers, which are groups of documents.
// Allows tiered sorting. Document tiers at the beginning are higher scored. Each document tier is sorted by docid.

#ifndef GAME_DOCUMENTSTIER_H
#define GAME_DOCUMENTSTIER_H

#include <vector>
#include <ostream>
#include <optional>
#include "DocumentFrequency.h"
#include <cassert>

using SingleDocumentsTier = std::vector<DocumentFrequency>;


class WordIndexEntry;

// Converts a long, flat list of DocumentFrequency objects to a multi-level, tiered list.
// Based on Strohman and Croft tiered-index paper.
namespace MultiDocumentsTier {
    struct TierIterator {
        int remaining;
        std::streampos read_position;
        std::istream& frequencies;

        std::optional<SingleDocumentsTier> read_next();
        TierIterator (std::istream&);
    };

    // Constructs a MultiDocumentsTier instance and serializes it.
    void serialize(const WordIndexEntry &wie, std::ostream &frequencies);
};


#endif //GAME_DOCUMENTSTIER_H
