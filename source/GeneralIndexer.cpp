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
#include "compactor/Compactor.h"
#include <execution>

#include "SyncedQueue.h"

using FilePairs = std::vector<DocIDFilePair>;
namespace fs = std::filesystem;


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

void queue_produce_file_contents(SyncedQueue &contents) {
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


void sort_and_group_all_par(std::vector<WordIndexEntry> &index) {
    std::for_each(std::execution::par, index.begin(), index.end(), [](WordIndexEntry &elem) {
        std::sort(elem.files.begin(), elem.files.end());
    });
}

std::optional<std::string> GeneralIndexer::read_some_files(void (*producer)(SyncedQueue &)) {
    // Vector of file paths and generated, incremental ID.

    // Vector of arrays with custom allocator.
    SortedKeysIndex a1;

    // Thread synchronization variables.
    // file_contents: a thread-safe queue that holds the contents + filename of each file for the Tokenizer
    //      to process.
    SyncedQueue file_contents;
    file_contents.done_flag = false;

    // Start our thread to open all files and load them into memory, so we don't get stuck on file IO
    // in the processing + indexing thread.
    std::thread filecontentproducer(producer, std::ref(file_contents));


    std::vector<std::future<SortedKeysIndex>> threads;
    for (int i = 0; i < 3; i++) {
        threads.emplace_back(std::async(std::launch::async | std::launch::deferred, [&]() {
            return thread_process_files(file_contents);
        }));
    }
    for (auto &fut : threads) a1.merge_into(fut.get());

    filecontentproducer.join();

    if (a1.get_index().empty()) {
        std::cerr<<"Empty index\n";
        return std::nullopt;
    }

    a1.sort_and_group_shallow();

    // Instead of sorting and grouping by terms, this also sorts each term's documents list by document ID.
    // This could take a long time (many sorts), and there's no memory-conservation advantage,
    // so we only need to do it at the end.
    sort_and_group_all_par(a1.get_index());
    return persist_indices(a1, file_contents.filepairs);
}

SortedKeysIndex
GeneralIndexer::thread_process_files(SyncedQueue &file_contents) {
    std::array<SortedKeysIndex, 10> reducer{};
    while (file_contents.size() || !file_contents.done_flag) {
        auto[contents, docidfilepair] = file_contents.pop();

        if (contents == "EMPTY" || file_contents.done_flag) break;

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


std::string GeneralIndexer::persist_indices(const SortedKeysIndex &master,
                                            const FilePairs &filepairs) {// Multiple indices output possible. Check them.

    std::string suffix = random_b64_str(5);
    if (std::filesystem::is_regular_file(
            fs::path(indice_files_dir / ("master_index" + suffix)))) {
        // File already exists. Get a new suffix that's more random.
        suffix += random_b64_str(50);
    }

    auto temp_suffix = "TEMP-" + suffix;
    Serializer::serialize(temp_suffix, filepairs);
    Serializer::serialize(temp_suffix, master);

    // once it's done we copy temp to real.
    IndexFileLocker::move_all(temp_suffix, suffix);


    return suffix;
}


int GeneralIndexer::read_and_compress_files() {

    auto do_two = []() -> std::optional<std::string> {
        auto first = GeneralIndexer::read_some_files(queue_produce_file_contents);
        if (!first) return std::nullopt;
        auto second = GeneralIndexer::read_some_files(queue_produce_file_contents);
        if (!second) return std::nullopt;
        return Compactor::compact_two_files(*first, *second);
    };

    auto one = do_two();
    auto two = do_two();
    auto three = do_two();
    auto four = do_two();

    if (!one || !two || !three || !four) return -1;

    auto onetwo = Compactor::compact_two_files(*one, *two).value();
    auto threefour = Compactor::compact_two_files(*three, *four).value();
    auto main_suffix = Compactor::compact_two_files(onetwo, threefour).value();
    return 0;
}
