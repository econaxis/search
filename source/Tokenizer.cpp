//
// Created by henry on 2021-04-29.
//

#include "Tokenizer.h"
#include <string>
#include <fstream>

#include <porter2_stemmer/porter2_stemmer.h>
#include <robin_hood/robin_hood.h>

int Tokenizer::clean_token_to_index(std::string &token) {
    remove_punctuation(token);
    if (token.size() <= 2) return 0; // Token shouldn't be included in index.
    else return 1;

}

std::vector<WordIndexEntry_unsafe> Tokenizer::index_string_file(std::string file, uint32_t docid) {
    robin_hood::unordered_map<std::string, WordIndexEntry_unsafe> index_temp;
    index_temp.reserve(file.length() / 20);
    int prev_pos, cur_pos = -1;
    while (prev_pos = cur_pos + 1, true) {
        cur_pos = file.find_first_of(" ,.;-{}()[]#?/;!\t\n'\"", prev_pos);
        if (cur_pos == std::string::npos) break;

        std::string temp = file.substr(prev_pos, cur_pos - prev_pos);

        if (clean_token_to_index(temp)) {
            if (auto it = index_temp.find(temp); it == index_temp.end()) {
                index_temp.emplace(temp, WordIndexEntry_unsafe{temp, {}});
            }
            index_temp.at(temp).files.push_back(DocumentPositionPointer{docid, (uint32_t) prev_pos});
        }

    }
    std::vector<WordIndexEntry_unsafe> final;
    final.reserve(index_temp.size());
    std::transform(index_temp.begin(), index_temp.end(), std::back_inserter(final),
                   [](auto &pair) {
                       return std::move(pair.second);
                   });
    return final;
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