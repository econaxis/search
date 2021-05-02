#include "GeneralIndexer.h"
#include "SortedKeysIndex.h"
#include "DocIDFilePair.h"
#include <execution>
#include "Tokenizer.h"
#include "random_b64_gen.h"
#include <mutex>
#include <fstream>
#include <iostream>


// Returns the number of files processed.

namespace fs = std::filesystem;
constexpr unsigned int MAX_FILES_PER_INDEX = 1000;
const auto data_file_path = fs::path("../data-files");
const auto indice_file_path = data_file_path / "indices";


int GeneralIndexer::read_some_files() {
    namespace chron = std::chrono;

    SortedKeysIndex master;
    std::vector<SortedKeysIndex> children;
    std::vector<DocIDFilePair> filepairs;
    filepairs.reserve(MAX_FILES_PER_INDEX);
    auto dir_it = fs::directory_iterator(data_file_path);
    uint32_t doc_id_counter = 0, files_processed = 0;

    // Consume directory iterator and push into filepairs vector
    for (const auto &i : dir_it) {
        if (i.is_regular_file()) {
            if (files_processed++ > MAX_FILES_PER_INDEX) break;
            filepairs.push_back(DocIDFilePair{++doc_id_counter, i.path()});
        }
    }

    if (filepairs.empty()) {
        return 0;
    }
    std::mutex global_merge;
    int num_merges_master = 0;
    children.resize(filepairs.size());
    std::transform(std::execution::par, filepairs.begin(), filepairs.end(),
                   children.begin(), [&](const DocIDFilePair &entry) {
                std::ifstream file(entry.file_name);
                if (!file.is_open()) {
                    std::cout << "Couldn't open file " << entry.file_name << "!\n";
                }

                SortedKeysIndex index1 = Tokenizer::index_istream(file, entry.docid);

                if (entry.docid % 5000 == 0) {
                    std::cout << "Done " << entry.docid << "\n";
                }

                if (global_merge.try_lock()) {
                    std::lock_guard<std::mutex> guard(global_merge, std::adopt_lock);
                    master.merge_into(index1);
                    if (++num_merges_master % 500 == 0) {
                        std::cout << "Sort group all master\n";
                        master.sort_and_group_all();
                    }
                    return SortedKeysIndex();
                } else {
                    return index1;
                }
            });

    std::cout << "Merging\n";

    std::size_t total_token_size = 0;

    for (const auto &c : children) total_token_size += c.index_size();

    master.reserve_more(total_token_size);
    for (auto &c : children) master.merge_into(c);

    master.sort_and_group_all();

    // Multiple indices output possible. Check them.
    std::string suffix = random_b64_str(5);
    if (std::filesystem::is_regular_file(
            std::filesystem::__cxx11::path("../data-files/indices/master_index" + suffix))) {
        // File already exists. Get a new suffix that's more random.
        suffix += random_b64_str(30);
    }

    std::cout << "Persisting files to disk\n";
    std::ofstream out_index(indice_file_path.string() + "/master_index" + suffix, std::ios_base::binary);
    std::ofstream filemapstream(indice_file_path.string() + "/filemap" + suffix, std::ios_base::binary);
    std::ofstream index_file(indice_file_path.string() + "/index_files", std::ios_base::app);
    Serializer::serialize(filemapstream, filepairs);
    Serializer::serialize(out_index, master);
    index_file << indice_file_path.string() + "/master_index" + suffix << "\n"
               << indice_file_path.string() + "/filemap" + suffix << "\n";
    // Since indexing was successful, we move the processed files to the processed folder.
    for (const auto &fp : filepairs) {
        auto p = std::filesystem::__cxx11::path(fp.file_name);
        std::filesystem::rename(p, p.parent_path() / std::filesystem::__cxx11::path("processed") / p.filename());
    }

    return filepairs.size();
}