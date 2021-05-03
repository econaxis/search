//
// Created by henry on 2021-05-02.
//

#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H


#include "SortedKeysIndex.h"
#include <vector>

namespace DocumentsMatcher {
    std::vector<MultiSearchResult>
    AND(const std::vector<const SearchResult *> &results,
               const std::vector<std::string> &result_terms);

    std::vector<MultiSearchResult>
    OR(const std::vector<const SearchResult *> &results,
               const std::vector<std::string> &result_terms);
};

#endif //GAME_DOCUMENTSMATCHER_H
