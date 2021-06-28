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
    using ContentProducerFunc = void (SyncedQueue &);

    int read_and_compress_files();


    std::string persist_indices(const SortedKeysIndex &master, const std::vector<DocIDFilePair> &filepairs);

    std::optional<std::string> read_some_files(ContentProducerFunc* func);

    SortedKeysIndex thread_process_files(SyncedQueue &file_contents);
}

#endif //GAME_GENERALINDEXER_H
