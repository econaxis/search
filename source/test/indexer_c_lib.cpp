//
// Created by henry on 2021-11-03.
//

#include <gtest/gtest.h>
#include "all_includes.h"
#include "GeneralIndexer.h"
#include "IndexStubCApi.h"

TEST(GeneralIndexer, c_lib_works) {
    auto index = new_index();
    append_file(index, "fdafdsadf fdsa hello world fsad fsa fdafda", 0);
    persist_indices(index, "python-test");
}


TEST(scratch, scratch) {
    auto index = create_index_stub("python-test");
    const char* terms[] = {"HELLO", "WORLD"};

    const char** _t = &terms[0];
    uint32_t len;
//    auto b = search_many_terms(index, _t, 2, &len);
}

TEST(scratch1, scratch1) {
    auto index = create_index_stub("par-index");
    const char* terms[] = {"abc", "WORLD"};

    const char** _t = &terms[0];
    uint32_t len;
    auto b = search_many_terms(index, _t, 2);
}
