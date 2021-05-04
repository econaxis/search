//
// Created by henry on 2021-05-02.
//

#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H


#include "SortedKeysIndex.h"
#include <vector>
#include <robin_hood/robin_hood.h>
namespace DocumentsMatcher {
    std::vector<MultiSearchResult>
    AND(const std::vector<const SearchResult *> &results,
               const std::vector<std::string> &result_terms);

    std::vector<MultiSearchResult>
    AND(const std::vector<std::vector<MultiSearchResult>>& results);

    std::vector<MultiSearchResult>
    OR(const std::vector<const SearchResult *> &results,
               const std::vector<std::string> &result_terms);


    std::vector<MultiSearchResult>
    AND(const std::vector<robin_hood::unordered_map<uint32_t, MultiSearchResult>> &results);
};

#endif //GAME_DOCUMENTSMATCHER_H
