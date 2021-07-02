#ifndef GAME_TOKENIZER_H
#define GAME_TOKENIZER_H


#include <map>
#include "Serializer.h"
#include <istream>
#include "SortedKeysIndex.h"
#include <set>

namespace Tokenizer {
    std::vector<WordIndexEntry> index_string_file(std::string file, uint32_t docid);


    bool clean_token_to_index(std::string &token);

    void remove_punctuation(std::string &a);

    void remove_bad_words(std::vector<std::string> &terms);

    bool check_stop_words(const std::string &s, int bi, int ei);

}
#endif //GAME_TOKENIZER_H
