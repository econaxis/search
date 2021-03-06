#ifndef GAME_SORTEDKEYSINDEX_H
#define GAME_SORTEDKEYSINDEX_H

#include "WordIndexEntry.h"


/**
 * Represents the contents of an entire index of some files.
 * This is used only as a container during indexing. When searching, we use SortedKeysIndexStub because we can't load the whole
 * index into memory. This class only contains the rudimentary information like term positions and document ids, but not higher level
 * information like frequencies, which we need when searching.
 */
class SortedKeysIndex {
private:
public:
    std::vector<WordIndexEntry> index;


    std::vector<WordIndexEntry> &get_index();

    SortedKeysIndex(std::vector<WordIndexEntry> index);

    SortedKeysIndex() = default;

    void sort_and_group_shallow();

    void merge_into(SortedKeysIndex &&other);

    void sort_and_group_all();

    const std::vector<WordIndexEntry> &get_index() const;
};


#endif //GAME_SORTEDKEYSINDEX_H
