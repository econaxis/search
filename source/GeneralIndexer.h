
#ifndef GAME_GENERALINDEXER_H
#define GAME_GENERALINDEXER_H


#include <set>
#include <ostream>
#include <sstream>
#include "WordIndexEntry.h"
#include "Serializer.h"
#include <filesystem>

namespace GeneralIndexer {
    int read_some_files();

    void persist_indices(const SortedKeysIndex &master, const std::vector<DocIDFilePair> &filepairs);

    void register_atexit_handler();
}


#endif //GAME_GENERALINDEXER_H
