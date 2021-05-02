#ifndef GAME_SORTEDKEYSINDEXSTUB_H
#define GAME_SORTEDKEYSINDEXSTUB_H


#include <vector>
#include <cstdint>
#include "DocumentPositionPointer.h"

class SortedKeysIndexStub {
    std::vector<uint32_t> stubindex;

public:
    SortedKeysIndexStub(std::vector<uint32_t> stubindex) : stubindex(std::move(stubindex)) {};
    SortedKeysIndexStub() = default;

    SearchResult search_keys(std::vector<std::string> terms);

    SearchResult search_key(std::string term);
};


#endif //GAME_SORTEDKEYSINDEXSTUB_H
