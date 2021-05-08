//
// Created by henry on 2021-04-29.
//

#ifndef GAME_WORDINDEXENTRY_H
#define GAME_WORDINDEXENTRY_H


#include <string>
#include <vector>
#include "DocumentPositionPointer.h"
#include "CustomAllocatedVec.h"

struct WordIndexEntry_v2 {
    std::string key;
    uint32_t term_pos;
    uint32_t positions_pos;
    std::vector<DocumentPositionPointer_v2> files;
};
struct WordIndexEntry_unsafe {
    CustomAllocatedVec<DocumentPositionPointer> files;
    std::string key;

    WordIndexEntry_unsafe(std::string key, std::vector<DocumentPositionPointer> f) : key(key), files() {
        for(auto& i : f) {
            files.push_back(std::move(i));
        }
    }
    WordIndexEntry_unsafe() : key("a"), files() {};


};
struct WordIndexEntry {
    std::string key;
    std::vector<DocumentPositionPointer> files;
};

inline bool operator<(const WordIndexEntry &elem1, const WordIndexEntry &elem2) {
    return elem1.key < elem2.key;
}


inline bool operator<(const WordIndexEntry &elem1, const std::string &str) {
    return elem1.key < str;
}

inline bool operator<(const std::string &str, const WordIndexEntry &elem1) {
    return str < elem1.key;
}

#endif //GAME_WORDINDEXENTRY_H
