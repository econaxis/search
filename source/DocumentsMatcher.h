//
// Created by henry on 2021-05-02.
//

#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H


#include "SortedKeysIndex.h"
#include <vector>
#include <robin_hood/robin_hood.h>
namespace DocumentsMatcher {

    TopDocs AND(std::vector<TopDocs> &results);
};

#endif //GAME_DOCUMENTSMATCHER_H
