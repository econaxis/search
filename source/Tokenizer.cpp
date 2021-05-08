//
// Created by henry on 2021-04-29.
//

#include "Tokenizer.h"
#include <iostream>
#include <string>
#include <locale>
#include <fstream>

#include <porter2_stemmer/porter2_stemmer.h>
#include <unordered_map>

std::ofstream debug("/tmp/debug.txt", std::ios_base::app);

int Tokenizer::clean_token_to_index(std::string &token) {
    remove_punctuation(token);
//    stem_english(token);
//    remove_punctuation(token);
    if (token.size() <= 2) return 0; // Token shouldn't be included in index.
    else return 1;

}

SortedKeysIndex Tokenizer::index_istream(std::ifstream &stream, uint32_t docid) {
    std::unordered_map<std::string, WordIndexEntry> index_temp;
    std::string file((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    index_temp.reserve(file.length() / 20);
    int prev_pos, cur_pos = -1;
    while (prev_pos = cur_pos + 1, true) {
        cur_pos = file.find_first_of(" ,.;-{}()[]#?/;!\t\n'\"", prev_pos);
        if (cur_pos == std::string::npos) break;

        std::string temp = file.substr(prev_pos, cur_pos - prev_pos);

        if (clean_token_to_index(temp)) {
            if (auto it = index_temp.find(temp); it == index_temp.end()) {
                index_temp.insert({temp, {temp, {}}});
            }
            index_temp.at(temp).files.emplace_back(docid, prev_pos);
        }

    }
    std::vector<WordIndexEntry> final;
    final.reserve(index_temp.size());
    std::transform(index_temp.begin(), index_temp.end(), std::back_inserter(final),
                   [](auto &pair) {
                       pair.second.files.shrink_to_fit();
                       return std::move(pair.second);
                   });
    return SortedKeysIndex(std::move(final));
}

void Tokenizer::remove_punctuation(std::string &a) {
    a.erase(std::remove_if(a.begin(), a.end(), [](char c) {
        int asciicode = static_cast<int>(c);
        if (asciicode > 90) asciicode -= 32;
        return !(asciicode >= 65 && asciicode <= 90);
    }), a.end());

    for (auto &c : a) {
        c = (char) std::toupper(c);
    }
}


void Tokenizer::stem_english(std::string &a) {

    Porter2Stemmer::stem(a);
}