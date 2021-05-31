

#include "Tokenizer.h"
#include <string>
#include <fstream>
#include "Constants.h"

#include <robin_hood/robin_hood.h>

struct WordPos {
    unsigned int start, end, position;
};

int Tokenizer::clean_token_to_index(std::string &token) {
    remove_punctuation(token);
    if (token.size() <= 2) return 0; // Token shouldn't be included in index.
    else return 1;

}


std::vector<WordPos> clean_string(std::string &file) {
    std::vector<WordPos> result;

    unsigned int last_end = -1, cur_word = 0;
    bool is_in_word = false;
    for (unsigned int i = 0; i < file.size(); i++) {
        if (std::isalpha(file[i])) {
            if (!is_in_word) {
                is_in_word = true;
                last_end = i;
            }
        }

        if (!std::isalpha(file[i])) {
            if (is_in_word) {
                result.push_back({last_end, i, cur_word++});

                last_end = -1;
                is_in_word = false;
            }
        }
    }
    return result;
}


std::vector<WordIndexEntry_unsafe> Tokenizer::index_string_file(std::string file, uint32_t docid) {
    auto positions = clean_string(file);
    robin_hood::unordered_map<std::string, WordIndexEntry_unsafe> index_temp;
    index_temp.reserve(file.length() / 2);
    for (auto[start, end, word_count] : positions) {
        auto temp = file.substr(start, end - start);

        if (clean_token_to_index(temp)) {
            if (auto it = index_temp.find(temp); it == index_temp.end()) {
                index_temp.emplace(temp, WordIndexEntry_unsafe{temp, {}});
            }
            index_temp.at(temp).files.push_back(DocumentPositionPointer{docid, word_count});
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

