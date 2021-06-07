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
    positions.write(reinterpret_cast<const char *>(&MAGIC_NUM), 4);
    positions << positionbuf.rdbuf();
}


std::vector<DocumentPositionPointer>
PositionsSearcher::read_positions_all(std::istream &positions, const std::vector<DocumentFrequency> &freq_list) {
    uint32_t magic_num;
    positions.read(reinterpret_cast<char *>(&magic_num), 4);
    assert(magic_num == MAGIC_NUM);

    std::vector<DocumentPositionPointer> out;
    for (auto &df : freq_list) {
        auto docs_left = df.document_freq;

        while (docs_left--) {
            auto pos = read_vnum(positions);
            out.emplace_back(df.document_id, pos);
        }
    }

    return out;

}


static std::vector<DocumentPositionPointer> a {};

#include "DocumentsTier.h"
#include <ctime>
#include <cstdlib>
void Push_random_test() {
    std::srand(std::time(nullptr));
    int num = 100000;
    uint maxint = 1 << 31;
    while (num-- > 10) {
        a.emplace_back(std::rand() % (num / 9) + (1 << 25), std::rand() % maxint);
    }
    std::sort(a.begin(), a.end());
}


void Compactor_test() {
    Push_random_test();

    WordIndexEntry wie{
            "test", a
    };
    std::stringstream positions, frequencies;
    PositionsSearcher::serialize_positions(positions, wie);
    MultiDocumentsTier::serialize(wie, frequencies);

    MultiDocumentsTier::TierIterator ti(frequencies);
    auto sd = ti.read_all();
    auto test = PositionsSearcher::read_positions_all(positions, sd);

    std::cout << positions.str();
    assert(test == a);
    exit(0);
}