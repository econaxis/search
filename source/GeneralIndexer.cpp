#include "GeneralIndexer.h"
#include "SortedKeysIndex.h"
#include "Tokenizer.h"
#include "random_b64_gen.h"
#include "Constants.h"

#include <thread>
#include "FileListGenerator.h"
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <queue>

using FilePairs = std::vector<DocIDFilePair>;
namespace fs = std::filesystem;

struct SyncedQueue {
    std::queue<std::pair<std::string, DocIDFilePair>> queue;
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::lock_guard<std::mutex> get_lock() const {
        return std::lock_guard(mutex);
    }

    uint32_t size() const {
        return queue.size();
    };

    void push(std::pair<std::string, DocIDFilePair> elem) {
        auto l = get_lock();
        queue.push(std::move(elem));
        cv.notify_one();
    }

    std::pair<std::string, DocIDFilePair> pop() {
        auto l = get_lock();
        auto b = queue.back();
        queue.pop();
        cv.notify_one();
        return b;
    }

    template<typename Callable>
    void wait_for(Callable c) {
        std::unique_lock lock(mutex);
        cv.wait(lock, c);
    }
};


void queue_produce_file_contents_tar(std::vector<std::string> tarnames, SyncedQueue &contents,
                                     std::atomic_bool &done_flag) {
    uint32_t docid = 1;
    for (const auto &tarname : tarnames) {
        mtar_t mtar;
        mtar_header_t h;
        if (mtar_open(&mtar, (data_files_dir / tarname).c_str(), "r") != MTAR_ESUCCESS) {
            throw std::runtime_error("Couldn't open file " + (data_files_dir / tarname).string());
        }
        while ((mtar_read_header(&mtar, &h)) != MTAR_ENULLRECORD && docid <= MAX_FILES_PER_INDEX) {
            std::string filestr(h.size, ' ');
            mtar_read_data(&mtar, filestr.data(), h.size);
            mtar_next(&mtar);

            contents.wait_for([&] {
                return contents.size() < 1500;
            });

            contents.push({std::move(filestr), {docid++, h.name}});

        }
    }
    done_flag = true;
    contents.cv.notify_all();
}

void queue_produce_file_contents(SyncedQueue &contents, FilePairs &filepairs,
                                 std::atomic_bool &done_flag) {
    for (auto &entry : filepairs) {
        auto abspath = data_files_dir / "data" / entry.file_name;
        if (!fs::exists(abspath) || !fs::is_regular_file(abspath)) {
            std::cerr << "Path " << abspath.c_str() << " nonexistent\n";
            continue;
        }

        auto len = fs::file_size(abspath);
        std::ifstream file(abspath);
        if (!file.is_open()) {
            std::cout << "Couldn't open file " << entry.file_name << "!\n";
        }
        std::string filestr(len, ' ');
        file.read(filestr.data(), len);

        if (!file.eof() && !file.fail()) {
            filestr.append((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        }

        if (contents.size() > 600) {
            contents.wait_for([&] {
                return contents.size() < 100;
            });
        }

        contents.push({std::move(filestr), entry});
    }
    done_flag = true;
    contents.cv.notify_all();
}


void reduce_word_index_entries(std::vector<WordIndexEntry_unsafe> &op1,
                               std::vector<WordIndexEntry_unsafe> op2) {
    for (auto &i : op2) {
        op1.push_back(std::move(i));
    }
}

std::vector<WordIndexEntry_unsafe> remake_vector() {
    auto a = std::vector<WordIndexEntry_unsafe>();
    a.reserve(11000);
    return a;
}


int GeneralIndexer::read_some_files() {
    // Vector of file paths and generated, incremental ID.
    FilePairs fp = FileListGenerator::from_file();

    int progress_counter = 0;

    // Vector of arrays with custom allocator.
    SortedKeysIndex a1;

    // Thread synchronization variables.
    // done_flag: for the file contents producer thread to signify it has pulled all file contents,
    //      or else, we don't know if we have indexed everything or are still waiting on file IO.
    // file_contents: a thread-safe queue that holds the contents + filename of each file for the Tokenizer
    //      to process.
    std::atomic_bool done_flag = false;
    SyncedQueue file_contents;

    // Start our thread to open all files and load them into memory, so we don't get stuck on file IO
    // in the processing + indexing thread.
    std::thread filecontentproducer(queue_produce_file_contents, std::ref(file_contents), std::ref(fp),
                                    std::ref(done_flag));

    auto a0 = remake_vector();
    while (file_contents.size() || !done_flag) {
        if (!file_contents.size()) {
            file_contents.wait_for([&]() {
                return file_contents.size() > 3 || done_flag;
            });
        }
        if (!file_contents.size() && done_flag) continue;
        auto[contents, docidfilepair] = file_contents.pop();

        if (progress_counter++ % (MAX_FILES_PER_INDEX / 5000 + 1) == 0) {
            std::cout << "Done " << progress_counter * 100 / MAX_FILES_PER_INDEX << "% " << progress_counter << "\r"
                      << std::flush;
        }

        auto temp = Tokenizer::index_string_file(contents, docidfilepair.document_id);
        reduce_word_index_entries(a0, std::move(temp));

        // `a0` is our "holding zone" for recently indexed files. It is a vector of all tokens and their positions in each file.
        // When a0 gets too full, we want to copy its data into the main index `a1`. Here, all similar tokens from different files
        // will be merged.
        //
        // The reason we need a holding location for a0 is because at the indexing stage, we'll make many tiny vectors (one for each term),
        // which is very inefficient. Therefore, in `a0`, we use some unsafe pool memory allocation to speed indexing up. Then, periodically,
        // we'll copy that unsafe memory into safe, STL vector-managed memory. Plus, the memory pool `ContiguousAllocator`
        // only has a fixed size which will runtime crash if we exceed it.
        if (a0.size() > 1000) {
            // Only need to sort and group (merge similar terms into the same vector) every few iterations.
            if (a1.get_index().size() > 10000 && a1.get_index().size() % 100 == 0) a1.sort_and_group_shallow();

            // Merge the unsafe, speedy holding structure into the main index.
            a1.merge_into(SortedKeysIndex(std::move(a0)));

            // Reset the holding vector to its empty state, ready for more indexing.
            a0 = remake_vector();
        }
    }
    a1.merge_into(SortedKeysIndex(std::move(a0)));

    if (a1.get_index().empty()) {
        return 0;
    }
    a1.sort_and_group_shallow();

    // Instead of sorting and grouping by terms, this also sorts each term's documents list by document ID.
    // This could take a long time (many sorts), and there's no memory-conservation advantage,
    // so we only need to do it at the end.
    a1.sort_and_group_all();

    persist_indices(a1, fp);

    filecontentproducer.join();
    return 1;
}

void GeneralIndexer::persist_indices(const SortedKeysIndex &master,
                                     const FilePairs &filepairs) {// Multiple indices output possible. Check them.

    std::string suffix = random_b64_str(5);
    if (std::filesystem::is_regular_file(
            fs::path(indice_files_dir / ("master_index" + suffix)))) {
        // File already exists. Get a new suffix that's more random.
        suffix += random_b64_str(50);
    }
    std::cout << "Persisting files to disk - " << suffix << "\n";
    auto filemap_path = "filemap-" + suffix;
    std::ofstream filemapstream(indice_files_dir / filemap_path, std::ios_base::binary);
    Serializer::serialize(filemapstream, filepairs);
    Serializer::serialize(suffix, master);

    // Put these new indices to the index_files list
    std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
    index_file << suffix << "\n";
}

// Various debug testing functions.
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
    Serializer::serialize("test_serialization", index);

    std::ifstream frequencies(data_files_dir / "indices" / "frequencies-test_serialization");
    std::ifstream terms(data_files_dir / "indices" / "terms-test_serialization");
    auto t = Serializer::read_sorted_keys_index_stub_v2(frequencies, terms);
}
