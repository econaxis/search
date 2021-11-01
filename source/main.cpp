#include "Tokenizer.h"
#include "GeneralIndexer.h"
#include "compactor/Compactor.h"
#include "dict_strings.h"
#include "random_b64_gen.h"
#include "SortedKeysIndexStub.h"
#include <immintrin.h>
#include <chrono>
#include "DocumentsMatcher.h"
#include "Constants.h"

namespace fs = std::filesystem;


void profile_indexing(std::vector<SortedKeysIndexStub> &index, std::vector<std::vector<DocIDFilePair>> &filemap,
                      char *argv[]) {
    using namespace std::chrono;

    int NUM_SEARCHES = std::atoi(argv[1]);
    std::uniform_int_distribution<uint> dist(0, 1000); // ASCII table codes for normal characters.
    auto t1 = high_resolution_clock::now();
    int i = 0;
    while (i < NUM_SEARCHES) {

        auto temp = (std::string) strings[dist(randgen())];
        auto temp1 = (std::string) strings[dist(randgen())];

        Tokenizer::clean_token_to_index(temp);
        Tokenizer::clean_token_to_index(temp1);

        std::vector<std::string> query{temp, temp1};
        auto size = 0;
        if (temp.size() && temp1.size()) {
            Tokenizer::remove_bad_words(query);
            if (!query.empty()) {
                for (auto &j : index) {
                    auto temp = j.search_many_terms(query);
                    size += DocumentsMatcher::combiner_with_position(j, temp, query).docs.size();
                }
                i++;
            }
        }

        if (i % 3 == 0)
            std::cout << "Matched " << size << " files for " << temp1 << " " << temp << " "
                      << i * 100 / NUM_SEARCHES << "%\n" << std::flush;
    }
    auto time = high_resolution_clock::now() - t1;
    auto timedbl = duration_cast<milliseconds>(time).count();
    std::cout << "Time for " << NUM_SEARCHES << " queries: " << timedbl << "\n";

}

std::pair<std::vector<SortedKeysIndexStub>, std::vector<std::vector<DocIDFilePair>>>
load_all_indices() {
    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);

    if (!index_file) {
        std::cerr << "Index file doesn't exist at path: " << data_files_dir / "indices" / "index_files" << "\n";
        throw std::exception();
//        return {};
    }

    std::vector<std::vector<DocIDFilePair>> filepairs;
    std::vector<SortedKeysIndexStub> indices;

    while (true) {
        auto[statedb, line] = Compactor::read_line(index_file);
        if (statedb != Compactor::ReadState::GOOD) break;

        std::cout << "Used database file: " << line << "\n";
        indices.push_back(SortedKeysIndexStub(line));

        if (indices.size() >= 1) break;
    }


    return {std::move(indices), std::move(filepairs)};
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;
    initialize_directory_variables();


    if (argc == 1) {
        while (GeneralIndexer::read_some_files(queue_produce_file_contents) != "") {
        };
        return 1;
    };


    auto[indices, filemap] = load_all_indices();
    profile_indexing(indices, filemap, argv);
    std::string inp_line;
    std::cout << "Ready\n>> ";

    while (std::getline(std::cin, inp_line)) {
        if (inp_line == ".exit") break;
        std::vector<std::string> terms;
        auto ss = std::istringstream(inp_line);
        std::string word;
        while (ss >> word) {
            std::string s(word);
            if (Tokenizer::clean_token_to_index(s)) {
                std::cout << s << " ";
                terms.emplace_back(s);
            }
        }
        auto result = indices[0].search_many_terms(terms);
        for(auto& i: result) {
            std::cout<<i.as_string();
        }
    }
}
