//
// Created by henry on 2021-05-04.
//

#ifndef GAME_RESULTSPRINTER_H
#define GAME_RESULTSPRINTER_H

#include <fstream>
#include "SortedKeysIndexStub.h"
#include "Constants.h"
#include <chrono>
#include <iostream>

namespace ResultsPrinter {
    using namespace std::chrono;

    void print_results(std::vector<MultiSearchResult> &temp1, std::vector<DocIDFilePair> &filepairs);
};


#endif //GAME_RESULTSPRINTER_H
