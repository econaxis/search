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
    std::string read_some_files(ContentProducerFunc* func);
}

void queue_produce_file_contents(SyncedQueue &contents);
#endif //GAME_GENERALINDEXER_H
