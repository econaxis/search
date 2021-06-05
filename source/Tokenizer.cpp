

#include "Tokenizer.h"
#include <string>
#include <fstream>
#include "Constants.h"
#include <robin_hood/robin_hood.h>

struct WordPos {
    unsigned int start, end;
};

static bool check_stop_words(const std::string &s, int bi, int ei);


int Tokenizer::clean_token_to_index(std::string &token) {
    remove_punctuation(token);
    if (token.size() <= 2) return 0; // Token shouldn't be included in index.
    else return 1;

}


std::vector<WordPos> clean_string(std::string &file) {
    std::vector<WordPos> result;

    unsigned int last_end = -1;
    bool is_in_word = false;
    bool is_in_xml = false;
    for (unsigned int i = 0; i < file.size(); i++) {
        if (file[i] == '<') {
            is_in_xml = true;
        } else if (file[i] == '>') {
            is_in_xml = false;
        } else if (is_in_xml) {
            continue;
        } else if (std::isalpha(file[i])) {
            if (!is_in_word) {
                is_in_word = true;
                last_end = i;
            }
        } else if (!std::isalpha(file[i])) {
            if (is_in_word) {
                // Limit word length to 10;
                if (i - last_end > 10) {
                    last_end = i - 10;
                }

                if (!check_stop_words(file, last_end, i)) {
                    result.push_back({last_end, i});
                }

                last_end = -1;
                is_in_word = false;
            }
        }


    }
    return result;
}


std::vector<WordIndexEntry> Tokenizer::index_string_file(std::string file, uint32_t docid) {
    auto positions = clean_string(file);
    robin_hood::unordered_map<std::string, WordIndexEntry> index_temp;
    robin_hood::unordered_set<unsigned int> processed;
    index_temp.reserve(file.length() / 2);

    unsigned int word_count = 0;

    for (auto[start, end] : positions) {
        auto temp = file.substr(start, end - start);

        if (clean_token_to_index(temp)) {
            if (auto it = index_temp.find(temp); it == index_temp.end()) {
                index_temp.emplace(temp, WordIndexEntry{temp, {}});
            }
            index_temp.at(temp).files.push_back(DocumentPositionPointer{docid, word_count++});
        }
    }
    std::vector<WordIndexEntry> final;
    final.reserve(index_temp.size());
    std::transform(index_temp.begin(), index_temp.end(), std::back_inserter(final),
                   [](auto &pair) {
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


bool check_stop_words(const std::string &s, int bi, int ei) {
    static constexpr std::string_view stopwords[] = {"I", "ME", "MY", "MYSELF", "WE", "OUR", "OURS", "OURSELVES", "YOU",
                                              "YOUR", "YOURS", "HE", "HIM", "HIS", "HIMSELF", "SHE", "HER",
                                              "HERS", "IT", "ITS", "ITSELF", "THEY", "THEM", "THEIR", "THEIRS", "WHAT",
                                              "WHICH", "WHO", "WHOM", "THIS", "THAT", "THESE", "THOSE", "AM", "IS",
                                              "ARE", "WAS", "WERE", "BE", "BEEN", "BEING", "HAVE", "HAS", "HAD", "DO",
                                              "DOES", "DID", "DOING", "A", "AN", "THE", "AND", "BUT", "IF", "OR", "AS",
                                              "UNTIL", "WHILE", "OF", "AT", "BY", "FOR", "WITH", "ABOUT", "INTO",
                                              "THROUGH", "ABOVE", "BELOW", "TO", "FROM", "IN", "OUT", "ON", "OFF",
                                              "OVER", "UNDER", "AGAIN", "ONCE", "HERE", "THERE", "WHEN", "WHERE", "WHY",
                                              "HOW", "ALL", "ANY", "BOTH", "EACH", "FEW", "MORE", "MOST", "OTHER",
                                              "SOME", "SUCH", "NO", "NOR", "NOT", "ONLY", "OWN", "SAME", "SO", "THAN",
                                              "TOO", "VERY", "S", "T", "CAN", "WILL", "JUST", "DON"};
    static const robin_hood::unordered_set<std::string_view> stw{stopwords, stopwords + 110};
    auto strv = std::string_view(s).substr(bi, ei - bi);
    return stw.find(strv) != stw.end();
}

