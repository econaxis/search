

#include "Tokenizer.h"
#include <string>
#include <fstream>
#include "Constants.h"
#include <robin_hood/robin_hood.h>
#include <iostream>

struct WordPos {
    unsigned int start, end;
};



bool Tokenizer::clean_token_to_index(std::string &token) {
    remove_punctuation(token);
    if (token.size() <= 2) return 0; // Token shouldn't be included in index.
    else return 1;
}


void Tokenizer::remove_bad_words(std::vector<std::string> &terms) {
    terms.erase(std::remove_if(terms.begin(), terms.end(), [](auto &t) {
        return !clean_token_to_index(t);
    }), terms.end());
}


// todo: fix bugs
std::vector<WordPos> clean_string(std::string &file) {
    std::vector<WordPos> result;

    unsigned int last_end = 1 << 31;
    bool is_in_word = false;
    bool is_in_xml = false;
    unsigned int i = 0;
    while (i < file.size()) {
        if (is_in_xml) {
            if (file[i] == '>') {
                is_in_xml = false;
            }
            i++;
            continue;
        } else if (is_in_word) {
            if (!std::isalpha(file[i])) {
                // Limit word length to 50;
                if (i - last_end > 50) {
                    last_end = i - 50;
                }

                if (!Tokenizer::check_stop_words(file, last_end, i)) {
                    result.push_back({last_end, i});
                }

                last_end = -1;
                is_in_word = false;
                continue;
            }
        } else if (!is_in_word) {
            if (std::isalpha(file[i])) {
                is_in_word = true;
                last_end = i;
            }
        }
        if (file[i] == '<') {
            is_in_xml = true;
        }
        i++;
    }

    if (is_in_word) result.push_back({last_end, i});

    return result;
}


std::vector<WordIndexEntry> Tokenizer::index_string_file(std::string file, uint32_t docid) {
    auto positions = clean_string(file);
    robin_hood::unordered_map<std::string, WordIndexEntry> index_temp;
    index_temp.reserve(file.length() / 3);

    for (auto[start, end] : positions) {
        auto temp = file.substr(start, end - start);
        if (clean_token_to_index(temp)) {
            if (auto it = index_temp.find(temp); it == index_temp.end()) {
                index_temp.emplace(temp, WordIndexEntry{temp, {}});
            }
            index_temp.at(temp).files.emplace_back(docid, start);
        }
    }
    std::vector<WordIndexEntry> final;
    final.reserve(index_temp.size());
    std::transform(std::make_move_iterator(index_temp.begin()), std::make_move_iterator(index_temp.end()),
                   std::back_inserter(final),
                   [](auto pair) {
                       return std::move(pair.second);
                   });
    return final;
}

void Tokenizer::remove_punctuation(std::string &a) {
//    a.erase(std::remove_if(a.begin(), a.end(), [](char c) {
//        int asciicode = static_cast<int>(c);
//        if (asciicode > 90) asciicode -= 32;
//        return !(asciicode >= 65 && asciicode <= 90);
//    }), a.end());

    for (auto &c : a) {
        c = (char) std::toupper(c);
    }
}


bool Tokenizer::check_stop_words(const std::string &s, int bi, int ei) {
    static constexpr std::string_view stopwords[] = {"I", "ME", "MY", "MYSELF", "WE", "OUR", "OURS", "OURSELVES",
                                                     "YOU",
                                                     "YOUR", "YOURS", "HE", "HIM", "HIS", "HIMSELF", "SHE", "HER",
                                                     "HERS", "IT", "ITS", "ITSELF", "THEY", "THEM", "THEIR",
                                                     "THEIRS", "WHAT",
                                                     "WHICH", "WHO", "WHOM", "THIS", "THAT", "THESE", "THOSE", "AM",
                                                     "IS",
                                                     "ARE", "WAS", "WERE", "BE", "BEEN", "BEING", "HAVE", "HAS",
                                                     "HAD", "DO",
                                                     "DOES", "DID", "DOING", "A", "AN", "THE", "AND", "BUT", "IF",
                                                     "OR", "AS",
                                                     "UNTIL", "WHILE", "OF", "AT", "BY", "FOR", "WITH", "ABOUT",
                                                     "INTO",
                                                     "THROUGH", "ABOVE", "BELOW", "TO", "FROM", "IN", "OUT", "ON",
                                                     "OFF",
                                                     "OVER", "UNDER", "AGAIN", "ONCE", "HERE", "THERE", "WHEN",
                                                     "WHERE", "WHY",
                                                     "HOW", "ALL", "ANY", "BOTH", "EACH", "FEW", "MORE", "MOST",
                                                     "OTHER",
                                                     "SOME", "SUCH", "NO", "NOR", "NOT", "ONLY", "OWN", "SAME",
                                                     "SO", "THAN",
                                                     "TOO", "VERY", "S", "T", "CAN", "WILL", "JUST", "DON"};
    static const robin_hood::unordered_set<std::string_view> stw{stopwords, stopwords + 110};
    auto strv = std::string_view(s).substr(bi, ei - bi);
    return stw.find(strv) != stw.end();
}

