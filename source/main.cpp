#include "ResultsPrinter.h"
#include "Tokenizer.h"
#include "GeneralIndexer.h"
#include "compactor/Compactor.h"
#include "dict_strings.h"
#include "random_b64_gen.h"
#include "SortedKeysIndexStub.h"
#include <immintrin.h>
#include <chrono>
#include "DocumentsMatcher.h"

namespace fs = std::filesystem;


void profile_indexing(std::vector<SortedKeysIndexStub> &index, std::vector<std::vector<DocIDFilePair>> &filemap,
                      char *argv[]) {
    using namespace std::chrono;

    int NUM_SEARCHES = std::atoi(argv[1]);
    std::uniform_int_distribution<uint> dist(0, 5825); // ASCII table codes for normal characters.
    auto t1 = high_resolution_clock::now();
    int i = 0;
    while(i < NUM_SEARCHES) {

        auto temp = (std::string) strings[dist(randgen())];
        auto temp1 = (std::string) strings[dist(randgen())];
        auto temp2 = (std::string) strings[dist(randgen())];
        auto temp3 = (std::string) strings[dist(randgen())];
        auto temp4 = (std::string) strings[dist(randgen())];

        Tokenizer::clean_token_to_index(temp);
        Tokenizer::clean_token_to_index(temp1);
        Tokenizer::clean_token_to_index(temp2);
        Tokenizer::clean_token_to_index(temp3);
        Tokenizer::clean_token_to_index(temp4);

        std::vector<std::string> query{temp, temp1};
        TopDocs result;
        if (temp.size() && temp1.size()) {
            Tokenizer::remove_bad_words(query);
            if(!query.empty()) {
                result = DocumentsMatcher::collection_merge_search(index, query);
                i++;
            }
        }
//        ResultsPrinter::print_results(result, filemap, query);

        if (i % 30 == 0)
            std::cout << "Matched " << result.size() << " files for " << temp1 << " " << temp << " "
                      << i * 100 / NUM_SEARCHES << "%\n" << std::flush;
    }
    auto time = high_resolution_clock::now() - t1;
    auto timedbl = duration_cast<milliseconds>(time).count();
    std::cout << "Time for " << NUM_SEARCHES << " queries: " << timedbl << "\n";

    exit(0);
}

std::pair<std::vector<SortedKeysIndexStub>, std::vector<std::vector<DocIDFilePair>>>
load_all_indices() {
    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);

    if (!index_file) {
        std::cerr << "Index file doesn't exist at path: " << data_files_dir / "indices" / "index_files" << "\n";
        return {};
    }

    std::vector<std::vector<DocIDFilePair>> filepairs;
    std::vector<SortedKeysIndexStub> indices;

    while (true) {
        auto[statedb, line] = Compactor::read_line(index_file);
        if (statedb != Compactor::ReadState::GOOD) break;

        std::cout << "Used database file: " << line << "\n";

        indices.emplace_back(line);

        if (indices.size() >= 7) break;
    }


    return {std::move(indices), std::move(filepairs)};
}



[[maybe_unused]] static unsigned long measure() {
    using namespace std::chrono;
    static auto lasttime = high_resolution_clock::now();
    unsigned long ret = duration_cast<nanoseconds>(high_resolution_clock::now() - lasttime).count();
    lasttime = high_resolution_clock::now();
    return ret;
}



int main(int argc, char *argv[]) {
    using namespace std::chrono;
    initialize_directory_variables();


    if (argc == 1 ) {
        while (true) {
            auto first = GeneralIndexer::read_some_files();
            assert(first);
            auto second = GeneralIndexer::read_some_files();
            assert(second);

            Compactor::compact_two_files(*first, *second);
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
        auto temp1 = DocumentsMatcher::collection_merge_search(indices, terms);
        std::cout<<temp1.size()<<" matches found\n";
    }
}
