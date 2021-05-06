#include "Serializer.h"
#include "ResultsPrinter.h"
#include "Tokenizer.h"
#include <iostream>
#include <mutex>
#include <cassert>
#include "DocIDFilePair.h"
#include "GeneralIndexer.h"
#include "compactor/Compactor.h"
#include "Constants.h"
#include "SortedKeysIndexStub.h"
#include "dict_strings.h"
#include "random_b64_gen.h"

void check_file_is_sorted(const std::string &fmline, std::vector<DocIDFilePair> &filepairs);

namespace fs = std::filesystem;

bool compdocid(const DocIDFilePair &t1, const DocIDFilePair &t2) {
    return t1.docid < t2.docid;
}

void setup_index(SortedKeysIndexStub &index, std::vector<DocIDFilePair> &filepairs) {
    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);

    if (!index_file) {
        std::cerr << "Index file doesn't exist at path: " << data_files_dir / "indices" / "index_files" << "\n";
        return;
    }

    auto[statedb, dbline] = Compactor::read_line(index_file);
    auto[statefm, fmline] = Compactor::read_line(index_file);


    std::ifstream fpstream(data_files_dir / fmline, std::ios_base::binary);
    std::ifstream stream(data_files_dir / dbline, std::ios_base::binary);

    std::cout << "Used database file: " << data_files_dir / dbline << "\n";
    assert(fpstream && stream);
    assert(statedb == Compactor::ReadState::GOOD && statedb == statefm);

    filepairs = Serializer::read_filepairs(fpstream);
    check_file_is_sorted(fmline, filepairs);
    //    SortedKeysIndex index = Serializer::read_sorted_keys_index(stream);
    index = SortedKeysIndexStub(data_files_dir / dbline);
}


void profile_indexing(SortedKeysIndexStub &index) {
    using namespace std::chrono;

    constexpr int NUM_SEARCHES = 100;
    std::uniform_int_distribution<uint> dist(0, 515 - 1); // ASCII table codes for normal characters.
    auto t1 = high_resolution_clock::now();
    for (int i = 0; i < NUM_SEARCHES; i++) {
        auto temp = (std::string) strings[dist(randgen())];
        auto temp1 = (std::string) strings[dist(randgen())];
        auto temp2 = (std::string) strings[dist(randgen())];

        Tokenizer::clean_token_to_index(temp);
        Tokenizer::clean_token_to_index(temp1);
        Tokenizer::clean_token_to_index(temp2);

        std::vector<std::string> query{temp, temp1};
        auto result = index.search_keys(query);

        if (i % NUM_SEARCHES / 100 == 0)
            std::cout << "Matched " << result.size() << " files for " << temp1 << " " << temp2 << " " << temp << " "
                      << i * 100 / NUM_SEARCHES << "%\n";
    }
    auto time = high_resolution_clock::now() - t1;
    auto timedbl = duration_cast<milliseconds>(time).count();
    std::cout << "Time for " << NUM_SEARCHES << " queries: " << timedbl << "\n";
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;
    initialize_directory_variables();

    if (argc == 1) {
        GeneralIndexer::register_atexit_handler();
        while (GeneralIndexer::read_some_files() != 0) {}
        return 1;
    };

    SortedKeysIndexStub index;
    std::vector<DocIDFilePair> filepairs;
    setup_index(index, filepairs);

    profile_indexing(index);
    return 1;

    std::string inp_line;
    std::cout << "Ready\n>> ";

    auto &output_stream = std::cout;
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
        auto mode = "AND";
        if (inp_line[0] == '/') mode = "OR";

        auto t1 = high_resolution_clock::now();
        auto temp1 = index.search_keys(terms, mode);
        auto time = high_resolution_clock::now() - t1;
        ResultsPrinter::print_results(temp1, filepairs);
        std::cout << "Time: " << duration_cast<milliseconds>(time).count() << "\n";


    }
}

void check_file_is_sorted(const std::string &fmline, std::vector<DocIDFilePair> &filepairs) {
    if (!std::is_sorted(filepairs.begin(), filepairs.end(), compdocid)) {
        std::cout << "Not sorted...sorting\n";
        std::sort(filepairs.begin(), filepairs.end(), compdocid);

        // Close fpstream and rewrite file.
        std::ofstream ofpstream(fmline, std::ios_base::binary);
        Serializer::serialize(ofpstream, filepairs);
    }
}
