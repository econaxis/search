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
using PositionsMatrix = std::vector<std::vector<DocumentPositionPointer>>;

struct WordIndexEntry;

namespace PositionsSearcher {

    void serialize_positions(std::ostream &positions, const WordIndexEntry &ie);

    std::vector<DocumentPositionPointer>
    read_positions_all(std::istream &positions, const SingleDocumentsTier &freq_list);


    DocumentsMatcher::TopDocsWithPositions
    rerank_by_positions(const PositionsMatrix &positions_list, const TopDocs &td, const std::vector<std::string>& query_terms);

    PositionsMatrix fill_positions_from_docs(const SortedKeysIndexStub &index, const std::vector<std::string>& query_terms);
};


#endif //GAME_POSITIONSSEARCHER_H
