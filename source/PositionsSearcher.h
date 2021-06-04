//
// Created by henry on 2021-06-03.
//

#ifndef GAME_POSITIONSSEARCHER_H
#define GAME_POSITIONSSEARCHER_H

#include <istream>

struct WordIndexEntry;

namespace PositionsSearcher {

    void read_index(std::istream& stream){
        // Reference serialization code
//        std::stringstream output_buf;
//        for (auto &file : ie.files) {
//            serialize_vnum(output_buf, file.document_id - prev_docid, false);
//            prev_docid = file.document_id;
//        }
//        auto prev_pos = 0;
//        for (auto &file : ie.files) {
//            uint32_t mypos;
//            if(file.document_position < prev_pos) {
//                mypos = file.document_position;
//                prev_pos = mypos;
//                mypos |= 1<<31;
//            } else {
//                mypos = file.document_position - prev_pos;
//                prev_pos = file.document_position;
//            }
//            serialize_vnum(output_buf, mypos, false);
//        }
    }

    void serialize_positions(std::ostream& positions, const WordIndexEntry& ie);
};



#endif //GAME_POSITIONSSEARCHER_H
