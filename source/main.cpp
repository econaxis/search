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

void setup_index(std::vector<DocIDFilePair> &filepairs, SortedKeysIndexStub& index) {
    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);

    if (!index_file) {
        std::cerr << "Index file doesn't exist at path: " << data_files_dir / "indices" / "index_files" << "\n";
        return;
    }

    auto[statedb, line] = Compactor::read_line(index_file);
    assert(statedb == Compactor::ReadState::GOOD);

    std::cout << "Used database file: " << line << "\n";

    std::ifstream filepairstream(indice_files_dir / ("filemap-" + line), std::ios_base::binary);
    filepairs = Serializer::read_filepairs(filepairstream);
    index = SortedKeysIndexStub(indice_files_dir / ("frequencies-" + line), indice_files_dir / ("terms-" + line));
}


void profile_indexing(SortedKeysIndexStub &index) {
    using namespace std::chrono;

    constexpr int NUM_SEARCHES = 1000;
    std::uniform_int_distribution<uint> dist(0, 514); // ASCII table codes for normal characters.
    auto t1 = high_resolution_clock::now();
    for (int i = 0; i < NUM_SEARCHES; i++) {
        auto temp = (std::string) strings[dist(randgen())];
        auto temp1 = (std::string) strings[dist(randgen())];
        auto temp2 = (std::string) strings[dist(randgen())];

        Tokenizer::clean_token_to_index(temp);
        Tokenizer::clean_token_to_index(temp1);
        Tokenizer::clean_token_to_index(temp2);

        std::vector<std::string> query{temp, temp1};
        TopDocs result;
        if(temp.size() && temp1.size()) result = index.search_many_terms(query);

        if (i % (NUM_SEARCHES / 100) == 0)
            std::cout << "Matched " << result.size() << " files for " << temp1 << " " << temp << " "
                      << i * 100 / NUM_SEARCHES << "%\n";
    }
    auto time = high_resolution_clock::now() - t1;
    auto timedbl = duration_cast<milliseconds>(time).count();
    std::cout << "Time for " << NUM_SEARCHES << " queries: " << timedbl << "\n";

    exit(0);
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;
    initialize_directory_variables();


    if (argc == 1) {
        GeneralIndexer::register_atexit_handler();
        while (GeneralIndexer::read_some_files() != 0) {  };
        return 1;
    };

//    std::vector<DocIDFilePair> filepairs;
//    setup_index(index, filepairs);


//    profile_indexing(index);
//    return 1;

    std::vector<DocIDFilePair> filemap;
    SortedKeysIndexStub index;
    setup_index(filemap, index);

    profile_indexing(index);

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
        auto temp1 = index.search_many_terms(terms);
        ResultsPrinter::print_results(temp1, filemap);
    }
}
