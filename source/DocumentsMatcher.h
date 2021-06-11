#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H

#include "TopDocs.h"
#include <vector>
#include "SortedKeysIndexStub.h"

namespace DocumentsMatcher {

    TopDocs AND(std::vector<TopDocs> &results);

    TopDocs backup(std::vector<TopDocs> &results);

    TopDocs
    collection_merge_search(std::vector<SortedKeysIndexStub> &indices, const std::vector<std::string> &search_terms);
};

#endif //GAME_DOCUMENTSMATCHER_H
