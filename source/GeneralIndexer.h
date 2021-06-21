#ifndef GAME_GENERALINDEXER_H
#define GAME_GENERALINDEXER_H


#include <set>
#include <ostream>
#include <sstream>
#include "WordIndexEntry.h"
#include "Serializer.h"
#include <atomic>
#include <queue>
#include "SortedKeysIndex.h"
#include <condition_variable>

struct SyncedQueue;

namespace GeneralIndexer {

    int read_and_compress_files();


    std::optional<std::string> read_some_files();


    std::string persist_indices(const SortedKeysIndex &master, const std::vector<DocIDFilePair> &filepairs);

    void test_serialization();

    void test_searching();

    SortedKeysIndex thread_process_files(const std::atomic_bool &done_flag, SyncedQueue &file_contents);

    SortedKeysIndex
    thread_process_files(const std::atomic_bool &done_flag, SyncedQueue &file_contents, int each_max_file);
}

#endif //GAME_GENERALINDEXER_H
