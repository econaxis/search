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

namespace fs = std::filesystem;

bool compdocid(const DocIDFilePair &t1, const DocIDFilePair &t2) {
    return t1.docid < t2.docid;
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;
    if (argc == 1) {
        GeneralIndexer::register_atexit_handler();
        int max=20;
        while (GeneralIndexer::read_some_files() != 0 && max--) {}
        return 1;
    };

    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);


    auto[statedb, dbline] = Compactor::read_line(index_file);
    auto[statefm, fmline] = Compactor::read_line(index_file);

    std::ifstream fpstream(fmline, std::ios_base::binary);
    std::ifstream stream(dbline, std::ios_base::binary);
    assert(fpstream && stream);
    assert(statedb == Compactor::ReadState::GOOD && statedb == statefm);

    std::vector<DocIDFilePair> filepairs = Serializer::read_filepairs(fpstream);
    SortedKeysIndex index = Serializer::read_sorted_keys_index(stream);

    if (!std::is_sorted(filepairs.begin(), filepairs.end(), compdocid)) {
        std::cout << "Not sorted...sorting\n";
        std::sort(filepairs.begin(), filepairs.end(), compdocid);

        // Close fpstream and rewrite file.
        std::ofstream ofpstream(fmline, std::ios_base::binary);
        Serializer::serialize(ofpstream, filepairs);
    }
    auto t1 = high_resolution_clock::now();
    std::vector<std::string> terms;

    for (int i = 1; i < argc; i++) {
        std::string s(argv[i]);

        if (Tokenizer::clean_token_to_index(s)) {
            std::cout << s << " ";
            terms.emplace_back(s);
        }
    }


    auto temp1 = index.search_keys(terms);
    std::cout << "Done search " << duration_cast<microseconds>(high_resolution_clock::now() - t1).count() << std::endl;
    for (const auto &p : temp1) {
        const auto &file_entry = std::lower_bound(filepairs.begin(), filepairs.end(), p,
                                                  [](const auto &fp, const auto &dpp) {
                                                      return fp.docid < dpp.document_id;
                                                  });

        if (file_entry != filepairs.end()) {
            std::cout << file_entry->file_name << " "
                      << p.document_position << "\n";
        } else {
            std::cout << p.document_id << " not found!\n";
        }
    }

}
