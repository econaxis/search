//
// Created by henry on 2021-06-03.
//
#include <iostream>
#include "Serializer.h"
#include "PositionsSearcher.h"

using namespace Serializer;


// If you try to read this MAGIC_NUM with normal vnum format, you'll get an error.
constexpr uint32_t MAGIC_NUM = 0xFFADCFF0;


void PositionsSearcher::serialize_positions(std::ostream &positions, const WordIndexEntry &ie) {
    // Document ID limits are implicit from the positions file.
    // Both are sorted.

    assert(std::is_sorted(ie.files.begin(), ie.files.end()));

    std::stringstream positionbuf;
    for (auto &file : ie.files) {
        serialize_vnum(positionbuf, file.document_position);
    }

    // Serialize magic num to help in debugging, make sure we aren't reading the wrong frame.
    positions.write(reinterpret_cast<const char*>(&MAGIC_NUM), 4);
    positions << positionbuf.rdbuf();
}



std::vector<DocumentPositionPointer> PositionsSearcher::read_positions_all(std::istream &positions) {
    auto num_files = read_vnum(positions);
    std::vector<std::pair<int, int>> docids(num_files + 1);

    auto prevdocid = 0, prevposition = 0;
    for (int i = 0; i < num_files; i++) {
        auto docid = read_vnum(positions);
        auto pos = read_vnum(positions);

        prevdocid += docid;
        prevposition += pos;
        docids[i] = std::pair{prevdocid, prevposition};
    }
    auto totalposblocklen = read_vnum(positions);
    auto posstart = positions.tellg();
    docids[num_files] = std::pair{0, totalposblocklen};




    std::vector<DocumentPositionPointer> output;
    for (auto pair = docids.begin(); pair < docids.end() - 1; pair++) {
        positions.seekg(pair->second + posstart);
        auto endpos = (pair + 1)->second;

        auto prevdocpos = 0;
        while (positions.tellg() < endpos + posstart) {
            auto pos = read_vnum(positions);
            prevdocpos += pos;
            output.emplace_back(pair->first, prevdocpos);
        }
    }
    return output;
}




static const std::vector<DocumentPositionPointer> a = {
        {1,    2},
        {9,    123212},
        {9,    12433232},
        {9,    42323232},
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

#include "DocumentsTier.h"
void Compactor_test() {
    WordIndexEntry wie {
        "test", a
    };
    std::stringstream positions, frequencies;
    PositionsSearcher::serialize_positions(positions, wie);
    MultiDocumentsTier::serialize(wie, frequencies);

    MultiDocumentsTier::TierIterator ti(frequencies);
    while(true) {
        auto i = ti.read_next();
        if(!i) break;
        for(auto& [a, b] : i.value()) std::cout<<a<<" "<<b<<"\n";
    }
    auto test = PositionsSearcher::read_positions_all(positions);

    std::cout<<positions.str();
    assert(test == a);
//    exit(0);
}