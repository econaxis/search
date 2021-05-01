#include "Serializer.h"
#include "SortedKeysIndex.h"
#include "Tokenizer.h"
#include <fstream>
#include <random>
#include <iostream>
#include <mutex>
#include "DocIDFilePair.h"
#include "GeneralIndexer.h"

namespace fs = std::filesystem;


int main(int argc, char *argv[]) {
    using namespace std::chrono;
    if (argc == 1) {
        while (GeneralIndexer::read_some_files() != 0) {}
        return 1;
    };


    std::ifstream fpstream("../data-files/indices/filemap", std::ios_base::binary);
    std::vector<DocIDFilePair> filepairs = Serializer::read_filepairs(fpstream);
    std::ifstream stream("../data-files/indices/master_index", std::ios_base::binary);
    SortedKeysIndex index = Serializer::read_sorted_keys_index(stream);

    auto t1 = high_resolution_clock::now();
    std::vector<std::string> terms;

    for (int i = 1; i < argc; i++) {
        std::string s(argv[i]);
        for (auto &c: s) c = std::toupper(c);
        std::cout << s << " ";
        terms.emplace_back(s);
    }


    auto temp1 = index.search_keys(terms);
    std::cout << "Done search " << duration_cast<microseconds>(high_resolution_clock::now() - t1).count() << std::endl;
    for (const auto &p : temp1) {
        std::cout << std::find_if(filepairs.begin(), filepairs.end(),
                                  [&](const auto &elem) { return elem.docid == p.document_id; })->file_name << " "
                  << p.document_position << "\n";
    }

}
