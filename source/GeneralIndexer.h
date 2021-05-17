
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


    void register_atexit_handler();

    void persist_indices(const SortedKeysIndex &master, const std::vector<DocIDFilePair> &filepairs);

    void test_serialization();

    void test_searching();
}


#endif //GAME_GENERALINDEXER_H
