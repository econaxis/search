//
// Created by henry on 2021-11-03.
//

#ifndef GAME_INDEXSTUBCAPI_H
#define GAME_INDEXSTUBCAPI_H
#include "DocumentsMatcher.h"

extern "C" {
    using namespace DocumentsMatcher;
TopDocsWithPositions::Elem *
search_many_terms(SortedKeysIndexStub *index, const char **terms, int terms_length, /*out */ uint32_t *length);
void free_elem_buf(TopDocsWithPositions::Elem *ptr);
void free_index_stub(SortedKeysIndexStub *stub);
SortedKeysIndexStub *create_index_stub(const char *suffix);
}

#endif //GAME_INDEXSTUBCAPI_H
