//
// Created by henry on 2021-07-01.
//

#include "PossiblyMatchingTerm.h"

std::optional<SingleDocumentsTier> PossiblyMatchingTerm::extend() {
    auto sdt = ti.read_next();

    // Multiply document frequency by the score (of how well the search term matches the existing term)
    // Score is determined upstream by the index (SortedKeysIndexStub)
    if (sdt.has_value()) {
        for (auto &i : *sdt) {
            i.document_freq *= score;
        }
    }
    return sdt;
}

PossiblyMatchingTerm::PossiblyMatchingTerm(std::string term, std::streampos positions, std::streampos freq, MultiDocumentsTier::TierIterator ti,
                                           uint32_t score)
        : term(term), freq(freq), ti(ti), score(score), positions(positions) {}
