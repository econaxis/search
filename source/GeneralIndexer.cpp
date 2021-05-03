#include "GeneralIndexer.h"
#include "SortedKeysIndex.h"
#include "DocIDFilePair.h"
#include <execution>
#include "Tokenizer.h"
#include "random_b64_gen.h"
#include <fstream>
#include <iostream>
#include <shared_mutex>
#include <mutex>
#include "Constants.h"

#include <csignal>

// Returns the number of files processed.
using FilePairs = std::vector<DocIDFilePair>;
namespace fs = std::filesystem;
constexpr unsigned int MAX_FILES_PER_INDEX = 5000;

std::shared_mutex atomic_file_operation_in_progress;
std::once_flag already_registered_atexit;


void GeneralIndexer::register_atexit_handler() {
    std::call_once(already_registered_atexit, []() {
        std::signal(SIGINT, [](int signal) {
            if (!atomic_file_operation_in_progress.try_lock()) {
                std::cout << "Stopped program in middle of atomic file operation! Unique suffix: "
                          << unique_directory_suffix
                          << "\nMove files from that directory to parent directory.\n";
                atomic_file_operation_in_progress.unlock();
            }
            std::signal(SIGINT, SIG_DFL);
            raise(SIGINT);
        });
    });
}


int GeneralIndexer::read_some_files() {
    namespace chron = std::chrono;

    SortedKeysIndex master;
    std::vector<SortedKeysIndex> children;
    FilePairs filepairs;
    filepairs.reserve(MAX_FILES_PER_INDEX);
    auto dir_it = fs::directory_iterator(data_files_dir);
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

    const auto &sortedkeys_reducer = [](SortedKeysIndex op1, SortedKeysIndex op2) {
        op1.merge_into(op2);

        return op1;
    };
    children.resize(filepairs.size());
    master = std::transform_reduce(std::execution::par_unseq, filepairs.begin(), filepairs.end(),
                                   SortedKeysIndex(), sortedkeys_reducer,
                                   [&](const DocIDFilePair &entry) {
                                       std::ifstream file(entry.file_name);
                                       if (!file.is_open()) {
                                           std::cout << "Couldn't open file " << entry.file_name << "!\n";
                                       }

                                       SortedKeysIndex index1 = Tokenizer::index_istream(file, entry.docid);

                                       if (entry.docid % (MAX_FILES_PER_INDEX / 10) == 0) {
                                           std::cout << "Done " << entry.docid << "\n";
                                       }

                                       return index1;
                                   });

    std::cout << "Merging\n";
    master.sort_and_group_shallow();
    master.sort_and_group_all();


    {
        std::shared_lock lock(atomic_file_operation_in_progress);
        persist_indices(master, filepairs);
    }


    return filepairs.size();
}

void GeneralIndexer::persist_indices(const SortedKeysIndex &master,
                                     FilePairs &filepairs) {// Multiple indices output possible. Check them.
    const auto indice_file_path = data_files_dir / "indices";

    std::string suffix = random_b64_str(5);
    if (std::filesystem::is_regular_file(
            fs::path(indice_file_path / ("master_index" + suffix)))) {
        // File already exists. Get a new suffix that's more random.
        suffix += random_b64_str(50);
    }
    // Since indexing was successful, we move the processed files to the processed folder.
    for (const auto &fp : filepairs) {
        auto p = fs::path(fp.file_name);
        fs::create_directory(data_files_dir / ("processed"));
        std::filesystem::rename(p, data_files_dir / "processed" / p.filename());
    }


    for(auto& fp : filepairs) {
        auto path = fs::path(fp.file_name);
        path = path.parent_path() / "processed"/path.filename();

        // Output relative path when serializing.
        fp.file_name = fs::relative(path.string(), data_files_dir);
    }

    std::cout << "Persisting files to disk\n";
    auto master_index_path = "master_index" + suffix;
    auto filemap_path = "filemap" + suffix;
    std::ofstream out_index(indice_file_path / master_index_path, std::ios_base::binary);
    std::ofstream filemapstream(indice_file_path / filemap_path, std::ios_base::binary);
    std::ofstream index_file(indice_file_path / "index_files", std::ios_base::app);
    Serializer::serialize(filemapstream, filepairs);
    Serializer::serialize(out_index, master);
    index_file << fs::relative(indice_file_path /  master_index_path, data_files_dir).string() << "\n"
               << fs::relative(indice_file_path /  filemap_path, data_files_dir).string() << "\n";
}
