//
// Created by henry on 2021-05-02.
//

#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H


#include "SortedKeysIndex.h"
#include <vector>
#include <robin_hood/robin_hood.h>
namespace DocumentsMatcher {
    std::vector<SafeMultiSearchResult>
    AND(const std::vector<const SearchResult *> &results,
               const std::vector<std::string> &result_terms);

    std::vector<SafeMultiSearchResult>
    OR(const std::vector<const SearchResult *> &results,
               const std::vector<std::string> &result_terms);



    std::vector<SafeMultiSearchResult> AND(std::vector<robin_hood::unordered_map<uint32_t, MultiSearchResult>> results);
};

#endif //GAME_DOCUMENTSMATCHER_H
