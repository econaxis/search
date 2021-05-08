#include "GeneralIndexer.h"
#include "SortedKeysIndex.h"
#include "DocIDFilePair.h"
#include <execution>
#include "Tokenizer.h"
#include "random_b64_gen.h"
#include <fstream>
#include "Constants.h"
#include <iostream>
#include <shared_mutex>
#include <mutex>

#include <csignal>
#include <future>

// Returns the number of files processed.
using FilePairs = std::vector<DocIDFilePair>;
namespace fs = std::filesystem;
constexpr unsigned int MAX_FILES_PER_INDEX = 100000;

std::shared_mutex atomic_file_operation_in_progress;
std::once_flag already_registered_atexit;


int GeneralIndexer::read_some_files() {
    namespace chron = std::chrono;

    FilePairs filepairs;
    filepairs.reserve(MAX_FILES_PER_INDEX);
    auto dir_it = std::ifstream(data_files_dir / "total-files-list");
    uint32_t doc_id_counter = 1, files_processed = 0;

    std::string file_line;
    // Consume directory iterator and push into filepairs vector
    while(std::getline(dir_it,file_line)) {
        // Check that file doesn't exist already.
//        if (fs::exists(data_files_dir / "processed" / file_line)) continue;
        if(!fs::exists(data_files_dir / "data"/ file_line)) continue;

        if (files_processed++ > MAX_FILES_PER_INDEX) break;
        filepairs.push_back(DocIDFilePair{++doc_id_counter, file_line});
    }

    if (filepairs.empty()) {
        std::cout<<"No files to be processed\n";
        return 0;
    }
    std::atomic_int progress_counter = 0;


    const auto &sortedkeys_reducer = [](SortedKeysIndex op1, SortedKeysIndex op2) {
        op1.merge_into(op2);

        return op1;
    };
    const auto &filename_processor = [&](const DocIDFilePair &entry) {
        std::ifstream file(data_files_dir / "data"/ entry.file_name);
        if (!file.is_open()) {
            std::cout << "Couldn't open file " << entry.file_name << "!\n";
        }

        SortedKeysIndex index1 = Tokenizer::index_istream(file, entry.docid);

        if (entry.docid % (MAX_FILES_PER_INDEX / 100) == 0) {
            progress_counter++;
            std::cout << "Done " << progress_counter<< "% \n";
        }

        return index1;
    };

    const auto &start_job = [&](auto it_begin, auto it_end) {
        return std::transform_reduce(std::execution::par_unseq, it_begin, it_end,
                              SortedKeysIndex(), sortedkeys_reducer, filename_processor);
    };
    auto master_first = start_job(filepairs.begin(), filepairs.end());

    std::cout << "Merging\n";
    master_first.sort_and_group_shallow();
    master_first.sort_and_group_all();


    {
        std::shared_lock lock(atomic_file_operation_in_progress);
        persist_indices(master_first, filepairs);
    }


    return filepairs.size();
}

void GeneralIndexer::persist_indices(const SortedKeysIndex &master,
                                     FilePairs &filepairs) {// Multiple indices output possible. Check them.

    std::string suffix = random_b64_str(5);
    if (std::filesystem::is_regular_file(
            fs::path(indice_files_dir / ("master_index" + suffix)))) {
        // File already exists. Get a new suffix that's more random.
        suffix += random_b64_str(50);
    }
    // Since indexing was successful, we move the processed files to the processed folder.
    fs::create_directory(data_files_dir / ("processed"));
    for (const auto &fp : filepairs) {
        fs::rename(data_files_dir /"data"/ fp.file_name, data_files_dir / "processed" / fp.file_name);
    }



    std::cout << "Persisting files to disk\n";
    auto filemap_path = "filemap-" + suffix;
    std::ofstream filemapstream(indice_files_dir / filemap_path, std::ios_base::binary);
    std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
    Serializer::serialize(filemapstream, filepairs);
    Serializer::serialize_consume(suffix, master);
    index_file << suffix << "\n";
}


/**
 * When we're in the midst of renaming files or doing an operation with bad consequences if it fails,
 * then we inform the user of what to do.
 */
void GeneralIndexer::register_atexit_handler() {
    std::call_once(already_registered_atexit, []() {
        auto handler = [](int signal) {
            if (!atomic_file_operation_in_progress.try_lock()) {
                std::cout << "Stopped program in middle of atomic file operation! Unique suffix: "
                          << unique_directory_suffix
                          << "\nMove files from that directory to parent directory.\n";
                atomic_file_operation_in_progress.unlock();
            }
            std::signal(signal, SIG_DFL);
            raise(signal);
        };
        std::signal(SIGINT, handler);
        std::signal(SIGTERM, handler);
        std::signal(SIGHUP, handler);
        std::signal(SIGKILL, handler);
    });
}


/**
 * Various debug testing functions.
 */

void GeneralIndexer::test_serialization() {
    std::vector<WordIndexEntry> a;
    std::uniform_int_distribution<uint> dist(0, 10); // ASCII table codes for normal characters.
    for (int i = 0; i < 1000; i++) {
        std::vector<DocumentPositionPointer> t;
        int iters = 100;
        while (iters--) t.push_back({dist(randgen()), 100});
        auto s = random_b64_str(10000);
        Tokenizer::clean_token_to_index(s);
        a.push_back({s, t});
    }

    SortedKeysIndex index(a);
    Serializer::serialize_consume("test_serialization", index);

    std::ifstream frequencies (data_files_dir / "indices" / "frequencies-test_serialization");
    std::ifstream terms (data_files_dir / "indices" / "terms-test_serialization");
    auto t = Serializer::read_sorted_keys_index_stub_v2(frequencies, terms);
    exit(0);
}

void GeneralIndexer::test_searching() {
    auto stub = SortedKeysIndexStub(indice_files_dir / "frequencies-test", indice_files_dir / "terms-test");
    TopDocs t = stub.search_many_terms({"AIR", "TEST", "UNITED", "THEIR", "THEM", "THE"});

    stub.search_one_term("AIR");
}