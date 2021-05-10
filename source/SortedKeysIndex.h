#ifndef GAME_SORTEDKEYSINDEX_H
#define GAME_SORTEDKEYSINDEX_H

#include <set>
#include "WordIndexEntry.h"
#include "Serializer.h"
#include <ostream>
#include <sstream>
#include <unordered_set>
#include <map>


/**
 * Represents the contents of an entire index of some files.
 * This is used only as a container during indexing. When searching, we use SortedKeysIndexStub because we can't load the whole
 * index into memory. This class only contains the rudimentary information like term positions and document ids, but not higher level
 * information like frequencies, which we need when searching.
 */
class SortedKeysIndex {
private:
    std::vector<WordIndexEntry> index;

public:


    std::vector<WordIndexEntry> &get_index();

    explicit SortedKeysIndex(std::vector<WordIndexEntry_unsafe> index);

    SortedKeysIndex() = default;

    void sort_and_group_shallow();

    void merge_into(SortedKeysIndex &&other);

    void sort_and_group_all();

};


#endif //GAME_SORTEDKEYSINDEX_H
