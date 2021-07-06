#include "GeneralIndexer.h"
#include <future>
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
#include <execution>

#include "SyncedQueue.h"

using FilePairs = std::vector<DocIDFilePair>;
namespace fs = std::filesystem;
SortedKeysIndex thread_process_files(SyncedQueue &file_contents);
std::string persist_indices(const SortedKeysIndex &master,
                            const FilePairs &filepairs);

static void queue_produce_file_contents(SyncedQueue &contents) {
    const FilePairs filepairs = FileListGenerator::from_file();

    std::vector<SyncedQueue::value_type> thread_local_holder;

    for (auto entry = filepairs.begin(); entry != filepairs.end(); entry++) {
        auto abspath = data_files_dir / "data" / entry->file_name;

        auto len = fs::file_size(abspath);
        std::ifstream file(abspath);
        if (!file.is_open()) {
            std::cout << "Couldn't open file " << entry->file_name << "!\n";
        }
        std::string filestr(len, ' ');
        file.read(filestr.data(), len);

        if (!file.eof() && !file.fail()) {
            filestr.append((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        }


        if (entry->document_id % 50 == 0) {
            std::cout << (entry - filepairs.begin() - contents.size()) * 100 / filepairs.size() << "%\r" << std::flush;
        }

//        if (contents.size() > 400) {
//            contents.wait_for([&] {
//                return contents.size() < 400;
//            });
//        }

        thread_local_holder.emplace_back(std::move(filestr), *entry);

        if (thread_local_holder.size() >= 200) {
            contents.push_multi(thread_local_holder.begin(), thread_local_holder.end());
            thread_local_holder.clear();
        }
    }
    contents.done_flag = true;
    contents.cv.notify_all();
}

static void sort_and_group_all_par(std::vector<WordIndexEntry> &index) {
    std::for_each(std::execution::par, index.begin(), index.end(), [](WordIndexEntry &elem) {
        std::sort(elem.files.begin(), elem.files.end());
    });
}

//! Reads some files from a specified func* and indexes them.
//! \param func function that produces content from a source (e.g. socket, STDIN, file) for the indexer to index.
//! \return the file name of the produced index file.
std::string GeneralIndexer::read_some_files(GeneralIndexer::ContentProducerFunc* func) {
    // Vector of arrays with custom allocator.
    SortedKeysIndex a1;

    // Thread synchronization variables.
    // file_contents: a thread-safe queue that holds the contents + filename of each file for the Tokenizer
    //      to process.
    SyncedQueue file_contents;
    file_contents.done_flag = false;

    // Start our thread to open all files and load them into memory, so we don't get stuck on file IO
    // in the processing + indexing thread.
    std::thread filecontentproducer(func, std::ref(file_contents));


    std::vector<std::future<SortedKeysIndex>> threads;
    for (int i = 0; i < 1; i++) {
        threads.emplace_back(std::async(std::launch::async | std::launch::deferred, [&]() {
            return thread_process_files(file_contents);
        }));
    }
    for (auto &fut : threads) a1.merge_into(fut.get());

    filecontentproducer.join();

    if (a1.get_index().empty()) {
        print("ERROR: Empty index");
    }

    a1.sort_and_group_shallow();

    // Instead of sorting and grouping by terms, this also sorts each term's documents list by document ID.
    // This could take a long time (many sorts), and there's no memory-conservation advantage,
    // so we only need to do it at the end.
    sort_and_group_all_par(a1.get_index());
    return persist_indices(a1, file_contents.filepairs);
}


SortedKeysIndex thread_process_files(SyncedQueue &file_contents) {
    std::array<SortedKeysIndex, 10> reducer{};
    while (true) {
        if(file_contents.done_flag && !file_contents.size()) {
            break;
        }

        auto[contents, docidfilepair] = file_contents.pop();


        if (contents == "EMPTY") {
            break;
        }

        log("Processing file ", docidfilepair.file_name);

        auto should_insert = std::min_element(reducer.begin(), reducer.end(), [](auto &i, auto &b) {
            return i.get_index().size() < b.get_index().size();
        });
        auto &should_insert_vec = should_insert->get_index();
        auto temp = Tokenizer::index_string_file(contents, docidfilepair.document_id);
        should_insert_vec.insert(should_insert_vec.end(), temp.begin(), temp.end());
    }
    reducer[0].sort_and_group_shallow();
    for (int i = 1; i < reducer.size(); i++) {
        reducer[i].sort_and_group_shallow();
        reducer[0].merge_into(std::move(reducer[i]));
        assert(reducer[i].get_index().empty());
    }
    return reducer[0];
}

std::string persist_indices(const SortedKeysIndex &master, const FilePairs &filepairs) {

    std::string suffix = random_b64_str(6);

    auto temp_suffix = "TEMP-" + suffix;
    Serializer::serialize(temp_suffix, filepairs);
    Serializer::serialize(temp_suffix, master);

    // once it's done we copy temp to real.
    IndexFileLocker::move_all(temp_suffix, suffix);


    return suffix;
}