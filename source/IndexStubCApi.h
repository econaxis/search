
#ifndef GAME_INDEXSTUBCAPI_H
#define GAME_INDEXSTUBCAPI_H

#include "DocumentsMatcher.h"
#include "PositionsSearcher.h"

extern "C" {
using namespace DocumentsMatcher;

struct SearchRetType {
    DocumentFrequency* topdocs;
    uint32_t topdocs_length;
    FoundPositions* pos;
    uint32_t pos_len;
};

SearchRetType search_many_terms(SortedKeysIndexStub *index, const char **terms, int terms_length);
void free_index(SortedKeysIndexStub* stub);
void free_elem_buf(SearchRetType elem);
SortedKeysIndexStub *create_index_stub(const char *suffix);
}

#endif //GAME_INDEXSTUBCAPI_H
