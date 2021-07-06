//
// Created by henry on 2021-07-01.
//

#ifndef GAME_POSSIBLYMATCHINGTERM_H
#define GAME_POSSIBLYMATCHINGTERM_H

#include "DocumentsTier.h"

struct PossiblyMatchingTerm {
    const std::string &term;
    MultiDocumentsTier::TierIterator ti;
    uint32_t score;

    PossiblyMatchingTerm(const std::string &term,
                         MultiDocumentsTier::TierIterator ti,
                         uint32_t score) : term(term), ti(ti), score(score) {};

    std::optional<SingleDocumentsTier> extend() {
        auto sdt =  ti.read_next();

        // Multiply document frequency by the score (of how well the search term matches the existing term)
        // Score is determined upstream by the index (SortedKeysIndexStub)
        if (sdt.has_value()) {
            for(auto& i : *sdt) {
                i.document_freq *= score;
            }
        }
        return sdt;
    }
};


#endif //GAME_POSSIBLYMATCHINGTERM_H
