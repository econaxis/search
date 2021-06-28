//
// Created by henry on 2021-06-03.
//

#ifndef GAME_POSITIONSSEARCHER_H
#define GAME_POSITIONSSEARCHER_H

#include <istream>
#include <vector>
#include "DocumentPositionPointer.h"
#include "DocumentFrequency.h"
#include "TopDocs.h"
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"

using SingleDocumentsTier = std::vector<DocumentFrequency>;

struct WordIndexEntry;

namespace PositionsSearcher {

    void serialize_positions(std::ostream &positions, const WordIndexEntry &ie);

    std::vector<DocumentPositionPointer>
    read_positions_all(std::istream &positions, const SingleDocumentsTier &freq_list);

    DocumentsMatcher::TopDocsWithPositions
    rerank_by_positions(const SortedKeysIndexStub &index, std::vector<TopDocs> &tds, const TopDocs &td);
};


#endif //GAME_POSITIONSSEARCHER_H
