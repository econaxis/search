#include "Serializer.h"
#include "SortedKeysIndex.h"
#include "Tokenizer.h"
#include <fstream>
#include <random>
#include <iostream>
#include <mutex>
#include <cassert>
#include "DocIDFilePair.h"
#include "GeneralIndexer.h"
#include "compactor/Compactor.h"
#include "Constants.h"
#include "SortedKeysIndexStub.h"

void check_file_is_sorted(const std::string &fmline, std::vector<DocIDFilePair> &filepairs);

namespace fs = std::filesystem;

bool compdocid(const DocIDFilePair &t1, const DocIDFilePair &t2) {
    return t1.docid < t2.docid;
}


int main(int argc, char *argv[]) {
    using namespace std::chrono;
    initialize_directory_variables();

    if (argc == 1) {
        GeneralIndexer::register_atexit_handler();
        while (GeneralIndexer::read_some_files() != 0) {}
        return 1;
    };

    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);

    if(!index_file) {
        std::cerr<<"Index file doesn't exist at path: "<<data_files_dir/"indices"/"index_files"<<"\n";
        return 1;
    }

    auto[statedb, dbline] = Compactor::read_line(index_file);
    auto[statefm, fmline] = Compactor::read_line(index_file);

    std::ifstream fpstream(data_files_dir / fmline, std::ios_base::binary);
    std::ifstream stream(data_files_dir / dbline, std::ios_base::binary);
    assert(fpstream && stream);
    assert(statedb == Compactor::ReadState::GOOD && statedb == statefm);

    std::vector<DocIDFilePair> filepairs = Serializer::read_filepairs(fpstream);
    //    SortedKeysIndex index = Serializer::read_sorted_keys_index(stream);
    SortedKeysIndexStub index(data_files_dir/dbline);
    index.fill_from_file(4);

    fpstream.close();
    stream.close();
    index_file.close();

    check_file_is_sorted(fmline, filepairs);
    auto t1 = high_resolution_clock::now();

    std::string inp_line;
    std::cout<<"Ready\n>> ";
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
        if(inp_line[0] == '/') mode= "OR";

        t1 = high_resolution_clock::now();
        auto temp1 = index.search_keys(terms, mode);
        auto time = high_resolution_clock::now() - t1;


        for (auto &v : temp1) {
            auto pos = std::lower_bound(filepairs.begin(), filepairs.end(), v.docid, [](auto &a, auto &b) {
                return a.docid < b;
            });

            if(pos!= filepairs.end()) std::cout << pos->file_name << ":";
            for (auto t0 : v.positions) { std::cout << t0 << " "; }
            std::cout << "\n";
        }
        std::cout << "Done search " << duration_cast<microseconds>(time).count()
                  << std::endl;



        std::cout << "\n>> ";

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
