//
// Created by henry on 2021-04-29.
//

#include "Tokenizer.h"
#include <fstream>
#include <iostream>
#include <string>
#include <olestem/stemming/english_stem.h>
#include <codecvt>
#include <locale>

int Tokenizer::clean_token_to_index(std::string &token) {
    remove_punctuation(token);
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
            index_temp.at(temp).files.emplace_back(docid, (uint16_t) prev_pos);
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
    using convert_type = std::codecvt_utf8<wchar_t>;
    static stemming::english_stem<> StemEnglish;
    static std::wstring_convert<convert_type, wchar_t> converter;
    static auto *UnicodeTextBuffer = new wchar_t[300];

    std::mbstowcs(UnicodeTextBuffer, a.c_str(), a.length());
    std::wstring word(UnicodeTextBuffer, a.length() + 1);
    StemEnglish(word);
    a = converter.to_bytes(word.data(), word.data() + a.length());

}