//
// Created by henry on 2021-07-01.
//

#ifndef GAME_POSSIBLYMATCHINGTERM_H
#define GAME_POSSIBLYMATCHINGTERM_H

#include "DocumentsTier.h"

struct PossiblyMatchingTerm {
    PossiblyMatchingTerm(std::string term, std::streampos positions, std::streampos freq, MultiDocumentsTier::TierIterator ti,
                                               uint32_t score);

    std::string term;
    std::streampos positions;
    std::streampos freq;
    MultiDocumentsTier::TierIterator ti;
    uint32_t score;

    std::optional<SingleDocumentsTier> extend();

};


#endif //GAME_POSSIBLYMATCHINGTERM_H
