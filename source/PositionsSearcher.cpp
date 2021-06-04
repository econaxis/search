//
// Created by henry on 2021-06-03.
//
#include "Serializer.h"
#include "PositionsSearcher.h"


void PositionsSearcher::serialize_positions(std::ostream &positions, const WordIndexEntry &ie) {
    using namespace Serializer;
    serialize_vnum(positions, ie.files.size(), false);
    auto prev_docid = 0, prev_pos = 0;
    std::stringstream docidbuf, positionbuf;
    for (auto &file : ie.files) {

        if (file.document_id != prev_docid) {
            serialize_vnum(docidbuf, file.document_id, true);
            serialize_vnum(docidbuf, positionbuf.tellp());
            prev_docid = file.document_id;
            prev_pos = 0;
        }

        serialize_vnum(positionbuf, file.document_position - prev_pos);
        prev_pos = file.document_position;
    }

    positions << docidbuf.rdbuf();
    positions << positionbuf.rdbuf();
}
