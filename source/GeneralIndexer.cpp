#include "GeneralIndexer.h"
#include "SortedKeysIndex.h"
#include "DocIDFilePair.h"
#include "Tokenizer.h"
#include "random_b64_gen.h"
#include <fstream>
#include "Constants.h"
#include <iostream>
#include <shared_mutex>

#include <microtar/microtar.h>
#include <mutex>

#include <csignal>
#include <thread>
#include <atomic>

// Returns the number of files processed.
using FilePairs = std::vector<DocIDFilePair>;
namespace fs = std::filesystem;
constexpr unsigned int MAX_FILES_PER_INDEX = 50000;

std::shared_mutex atomic_file_operation_in_progress;
std::once_flag already_registered_atexit;

std::condition_variable cv;
std::mutex mutex;
std::atomic_bool done = false;

void queue_produce_file_contents(std::queue<std::pair<std::string, DocIDFilePair>> &contents, FilePairs &filepairs) {
    mtar_t mtar;
    mtar_header_t h;
    mtar_open(&mtar, (data_files_dir / "tarball.tar").c_str(), "r");
    for (auto &entry : filepairs) {
        std::ifstream file(data_files_dir / "data" / entry.file_name);
        if (!file.is_open()) {
            std::cout << "Couldn't open file " << entry.file_name << "!\n";
        }
        std::string filestr(10000, ' ');
        file.read(filestr.data(), 10000);

        if(!file.eof() && !file.fail()) {
            filestr.append((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        } else {
            filestr.erase(file.gcount(), 5000 - file.gcount());
        }
//    while ( (mtar_read_header(&mtar, &h)) != MTAR_ENULLRECORD ) {
//        std::string filestr(h.size, ' ');
//        mtar_read_data(&mtar, filestr.data(), h.size);
//        mtar_next(&mtar);

        {

            std::unique_lock lock(mutex);
            cv.wait(lock, [&] {
                return contents.size() < 2000;
            });

            contents.push({std::move(filestr), entry});
            cv.notify_one();
        }


    }
    done = true;
}


int GeneralIndexer::read_some_files() {
    namespace chron = std::chrono;

    FilePairs filepairs;
    filepairs.reserve(MAX_FILES_PER_INDEX);
    auto dir_it = std::fstream(data_files_dir / "total-files-list");
    uint32_t doc_id_counter = 1, files_processed = 0;

    std::string file_line;
    // Consume directory iterator and push into filepairs vector
    while (std::getline(dir_it, file_line)) {
        // Check that file doesn't exist already.
//        if (fs::exists(data_files_dir / "processed" / file_line)) continue;
//        if(!fs::exists(data_files_dir / "data"/ file_line)) continue;
        if (file_line[0] == '#') {
            // File has already / is currently processed
            continue;
        }

        dir_it.seekg(-file_line.size() - 1, std::ios_base::cur);
        dir_it.put(dir_it.peek());
        dir_it.seekg(file_line.size(), std::ios_base::cur);

        if (files_processed++ > MAX_FILES_PER_INDEX) break;
        filepairs.push_back(DocIDFilePair{++doc_id_counter, file_line});
    }

    dir_it.close(); // flush all our writes
    if (filepairs.empty()) {
        std::cout << "No files to be processed\n";
        return 0;
    }
    int progress_counter = 0;


    const auto &sortedkeys_reducer = [](std::vector<WordIndexEntry_unsafe> &op1,
                                        std::vector<WordIndexEntry_unsafe> op2) {
        for (auto &i : op2) {
            op1.push_back(std::move(i));
        }
    };

    std::queue<std::pair<std::string, DocIDFilePair>> file_contents;
    const auto &file_processor = [&](const std::string &filestr, uint32_t docid) {
        if (docid % (MAX_FILES_PER_INDEX / 100) == 0) {
            progress_counter++;
            std::cout << "Done " << progress_counter << "% \n";
        }

        return Tokenizer::index_string_file(filestr, docid);
    };

    std::vector<WordIndexEntry_unsafe> a0;
    a0.reserve(110000);
    SortedKeysIndex a1;

    std::thread filecontentproducer(queue_produce_file_contents, std::ref(file_contents), std::ref(filepairs));

    while (!done) {
        std::string file;
        uint32_t docid;
        {
            std::unique_lock lock(mutex);
            if (file_contents.empty()) {
                cv.wait(lock, [&]() {
                    return file_contents.size() > 300 || done;
                });
            }
            if(done) break;
            file = file_contents.back().first;
            docid = file_contents.back().second.docid;
            file_contents.pop();
            lock.unlock();
            cv.notify_all();
        }

        auto temp = file_processor(file, docid);
        sortedkeys_reducer(a0, std::move(temp));

        if (a0.size() > 100000) {
            std::cout << a0.size() << "\n";
            if (a0.size() % 10 == 0) a1.sort_and_group_shallow();
            a1.merge_into(SortedKeysIndex(std::move(a0)));
            a0.clear();
            a0 = std::vector<WordIndexEntry_unsafe>();
            a0.reserve(110000);
        }
    }
    a1.merge_into(SortedKeysIndex(std::move(a0)));
    filecontentproducer.join();

    auto master = a1;
    std::cout << "Merging\n";

    if(master.get_index().empty()) {
        return 0;
    }
    master.sort_and_group_shallow();
    master.sort_and_group_all();


    {
        std::shared_lock _lock(atomic_file_operation_in_progress);
        persist_indices(master, filepairs);
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
//        fs::rename(data_files_dir /"data"/ fp.file_name, data_files_dir / "processed" / fp.file_name);
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
    std::vector<WordIndexEntry_unsafe> a;
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

    std::ifstream frequencies(data_files_dir / "indices" / "frequencies-test_serialization");
    std::ifstream terms(data_files_dir / "indices" / "terms-test_serialization");
    auto t = Serializer::read_sorted_keys_index_stub_v2(frequencies, terms);
    exit(0);
}

void GeneralIndexer::test_searching() {
    auto stub = SortedKeysIndexStub(indice_files_dir / "frequencies-test", indice_files_dir / "terms-test");
    TopDocs t = stub.search_many_terms({"AIR", "TEST", "UNITED", "THEIR", "THEM", "THE"});

    stub.search_one_term("AIR");
}