//
// Created by henry on 2021-06-03.
//

#ifndef GAME_POSITIONSSEARCHER_H
#define GAME_POSITIONSSEARCHER_H

#include <istream>
#include "DocumentPositionPointer.h"

struct WordIndexEntry;

namespace PositionsSearcher {


    void serialize_positions(std::ostream& positions, const WordIndexEntry& ie);

    std::vector<uint32_t> read_positions(std::istream &positions, uint32_t document_id);

    std::vector<DocumentPositionPointer> read_positions_all(std::istream &positions);
};



#endif //GAME_POSITIONSSEARCHER_H
