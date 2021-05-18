#include "GeneralIndexer.h"
#include "SortedKeysIndex.h"
#include "Tokenizer.h"
#include "random_b64_gen.h"
#include "Constants.h"

#include <thread>
#include "FileListGenerator.h"
#include <atomic>
//#define READ_TAR
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
        auto len = fs::file_size(data_files_dir / "data" / entry.file_name);
        std::ifstream file(data_files_dir / "data" / entry.file_name);
        if (!file.is_open()) {
            std::cout << "Couldn't open file " << entry.file_name << "!\n";
        }
        std::string filestr(len, ' ');
        file.read(filestr.data(), len);

        if (!file.eof() && !file.fail()) {
            filestr.append((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        }
        contents.wait_for([&] {
            return contents.size() < 300;
        });

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

template<typename T>
std::vector<T> remake_vector() {
    auto a = std::vector<T>();
    a.reserve(110000);
    return a;
}


int GeneralIndexer::read_some_files() {
    FilePairs fp = FileListGenerator::from_file();

    uint32_t progress_counter = 0;

    // Vector of arrays with custom allocator.
    SortedKeysIndex a1;
    std::atomic_bool done_flag = false;
    SyncedQueue file_contents;
#ifdef READ_TAR
    std::vector<std::string> tarnames = {"tar-1.tar","tar-2.tar","tar-3.tar","tar-4.tar","tar-4.tar"};
    std::thread filecontentproducer(queue_produce_file_contents, std::ref(tarnames), std::ref(file_contents),
                                    std::ref(done_flag));
#else
    std::thread filecontentproducer(queue_produce_file_contents, std::ref(file_contents), std::ref(fp),
                                    std::ref(done_flag));
#endif

    auto a0 = remake_vector<WordIndexEntry_unsafe>();
    while (file_contents.size() || !done_flag) {
        if (!file_contents.size()) {
            file_contents.wait_for([&]() {
                return file_contents.size() > 3 || done_flag;
            });
        }
        if (!file_contents.size() && done_flag) continue;
        auto[contents, docidfilepair] = file_contents.pop();

        if (progress_counter++ % (MAX_FILES_PER_INDEX / 5000 + 1) == 0) {
            std::cout << "Done " << progress_counter * 100 / MAX_FILES_PER_INDEX << "% "<<progress_counter<<"\r"<< std::flush;
        }

        auto temp = Tokenizer::index_string_file(contents, docidfilepair.document_id);
        reduce_word_index_entries(a0, std::move(temp));

        if (a0.size() > 100000) {
            if (a0.size() % 10 == 0) a1.sort_and_group_shallow();
            a1.merge_into(SortedKeysIndex(std::move(a0)));
            a0 = remake_vector<WordIndexEntry_unsafe>();
        }
    }
    a1.merge_into(SortedKeysIndex(std::move(a0)));
    filecontentproducer.join();

    if (a1.get_index().empty()) {
        return 0;
    }
    a1.sort_and_group_shallow();
    a1.sort_and_group_all();


    persist_indices(a1, fp);


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
    // Since indexing was successful, we move the processed files to the processed folder.
    fs::create_directory(data_files_dir / ("processed"));

    std::cout << "Persisting files to disk - " << suffix << "\n";
    auto filemap_path = "filemap-" + suffix;
    std::ofstream filemapstream(indice_files_dir / filemap_path, std::ios_base::binary);
    Serializer::serialize(filemapstream, filepairs);
    Serializer::serialize(suffix, master);

    // Put these new indices to the index_files list
    std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
    index_file << suffix << "\n";
}


/**
 * When we're in the midst of renaming files or doing an operation with bad consequences if it fails,
 * then we inform the user of what to do.
 */
void GeneralIndexer::register_atexit_handler() {
    // not used.
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
    Serializer::serialize("test_serialization", index);

    std::ifstream frequencies(data_files_dir / "indices" / "frequencies-test_serialization");
    std::ifstream terms(data_files_dir / "indices" / "terms-test_serialization");
    auto t = Serializer::read_sorted_keys_index_stub_v2(frequencies, terms);
}
