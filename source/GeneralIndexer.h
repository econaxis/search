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


extern "C" {
SortedKeysIndex* new_index();
void append_file(SortedKeysIndex *index, const char *content, uint32_t docid);
void persist_indices(SortedKeysIndex *index, const char *filename);
void concat_indices(SortedKeysIndex* main, SortedKeysIndex* add);
}

#endif //GAME_GENERALINDEXER_H
