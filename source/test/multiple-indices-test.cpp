#include "all_includes.h"
#include <gtest/gtest.h>
#include <span>
#include <TopDocsResultsJoiner.h>
#include <robin_hood/robin_hood.h>

std::string get_random_word() {
    constexpr auto sz = std::size(strings);
    return std::string(strings[utils::rand() % sz]);
}


void get_or_insert(std::unordered_map<std::string, int> &map, std::string &val) {
    if (auto it = map.find(val); it != map.end()) {
        it->second++;
    } else {
        map.emplace(val, 0);
    }
}


// Tests that multiple indices actually work.
TEST(MultipleIndices, multiple_indices) {
    std::unordered_map<std::string, std::string> docs;
    std::unordered_map<std::string, int> freq;

    int num_index = 0;
    auto generator = [&](int index, std::string filename) {
        std::string file;
        while (file.size() < 200) {
            auto word = get_random_word();
            if (!Tokenizer::clean_token_to_index(word)) {
                continue;
            }

            get_or_insert(freq, word);
            file.append(word);
            file.append(" ");
        }

        docs.emplace(filename, file + " " + std::to_string(num_index));
        return file;
    };


    std::vector<SortedKeysIndexStub> indices;

    indices.emplace_back(do_index_custom(generator));
    num_index++;
    indices.emplace_back(do_index_custom(generator));
    num_index++;
    indices.emplace_back(do_index_custom(generator));
    num_index++;
    indices.emplace_back(do_index_custom(generator));
    num_index++;
    indices.emplace_back(do_index_custom(generator));
    num_index++;
    indices.emplace_back(do_index_custom(generator));


    std::span indices_span(indices);


    for (auto&[word, frequency] : freq) {
        if (frequency < 10) continue;

        if(Tokenizer::check_stop_words(word, 0, word.size())) continue;
        print("Testing ", word);

        std::vector<std::string> query{word};
        auto result = TopDocsResultsJoiner::query_multiple_indices(indices_span, query);

        auto it = result.get_results();
        int num_results = 0;
        while (it.valid()) {
            num_results++;
            auto filename = indices[it->indexno].query_filemap(it->doc.document_id);
            auto file = docs[filename];
            ASSERT_NE(file.find(query[0]), std::string::npos);
            it.next();
        }

        ASSERT_GT(num_results, 0);

    }

}
