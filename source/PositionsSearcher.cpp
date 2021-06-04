//
// Created by henry on 2021-06-03.
//
#include <iostream>
#include "Serializer.h"
#include "PositionsSearcher.h"

using namespace Serializer;


void PositionsSearcher::serialize_positions(std::ostream &positions, const WordIndexEntry &ie) {
    auto prev_docid = 0;
    auto num_docs = 0;
    std::stringstream docidbuf, positionbuf;
    for (auto &file : ie.files) {
        if (file.document_id != prev_docid) {
            num_docs++;
            serialize_vnum(docidbuf, file.document_id - prev_docid, true);
            serialize_vnum(docidbuf, positionbuf.tellp(), true);

            std::cout<<file.document_id<<" "<<positionbuf.tellp()<<"\n";
            prev_docid = file.document_id;
        }
        serialize_vnum(positionbuf, file.document_position);

    }
    serialize_vnum(positions, num_docs);
    positions << docidbuf.rdbuf();
    serialize_vnum(positions, positionbuf.tellp());
    positions << positionbuf.rdbuf();
}

std::vector<DocumentPositionPointer> PositionsSearcher::read_positions_all(std::istream &positions) {
    auto num_files = read_vnum(positions);
    std::vector<std::pair<int, int>> docids(num_files + 1);


    auto prevdocid = 0;
    for (int i = 0; i < num_files; i++) {
        auto docid = read_vnum(positions);
        auto pos = read_vnum(positions);

        prevdocid += docid;

        docids[i] = std::pair{prevdocid, pos};
    }
    auto poslength = read_vnum(positions);
    auto posstart = positions.tellg();
    docids[num_files] = std::pair{0, poslength};


    std::vector<DocumentPositionPointer> output;
    for (auto pair = docids.begin(); pair < docids.end() - 1; pair++) {
        positions.seekg(pair->second + posstart);
        auto endpos = (pair + 1)->second;

        while (positions.tellg() < endpos + posstart) {
            auto pos = read_vnum(positions);
            output.emplace_back(pair->first, pos);
        }
    }
    return output;
}


static const std::vector<DocumentPositionPointer> a = {
        {1,    2},
        {9,    123212},
        {9,    2147483648},
        {9,    12433232},
        {9,    12433232},
        {9,    123232},
        {9,    42323232},
        {9,    2123232},
        {9,    11123232},
        {9,    14},
        {11,   552},
        {91,   2533},
        {91,   25323},
        {91,   53432},
        {9112, 2231},
        {9112, 22311},
        {9112, 222311},
        {9112, 224331},
        {9112, 552231},
        {9112, 662231},
        {9112, 772231},
        {9112, 882231},

};
void Compactor_test() {
    WordIndexEntry wie {
        "test", a
    };
    std::stringstream positions("/tmp/positions");
    PositionsSearcher::serialize_positions(positions, wie);
    auto test = PositionsSearcher::read_positions_all(positions);

    std::cout<<positions.str();
    assert(test == a);
    exit(0);
}