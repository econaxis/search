//
// Created by henry on 2021-04-29.
//

#ifndef GAME_TOKENIZER_H
#define GAME_TOKENIZER_H


#include <map>
#include "Serializer.h"
#include <istream>
#include "SortedKeysIndex.h"
#include <set>

namespace Tokenizer {
    SortedKeysIndex index_istream(std::ifstream &stream, uint32_t docid);


    void stem_english(std::string &a);

    int clean_token_to_index(std::string &token);

    void remove_punctuation(std::string &a);
};


#endif //GAME_TOKENIZER_H
