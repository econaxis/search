#ifndef GAME_SORTEDKEYSINDEXSTUB_H
#define GAME_SORTEDKEYSINDEXSTUB_H


#include <vector>
#include <cstdint>
#include <filesystem>
#include <robin_hood/robin_hood.h>
#include "DocumentPositionPointer.h"
#include "SortedKeysIndex.h"


struct Base26Num {
    uint64_t num; // Represent 3 alphabet letters in uint16_t.
    explicit Base26Num(std::string from);

    bool operator<(Base26Num other) const {
        return num < other.num;
    }
};


/**
 * Makes up every nth element of the larger index. The key is stored as a Base26Number (uint64_t) for memory efficiency.
 * Equivalent to WordIndexEntry if we were to load the whole index to memory with SortedKeysIndex.
 */
struct StubIndexEntry {
    Base26Num key;

    // The position on the file that this key resides at.
    // At this position, it's the start of WordIndexEntry for this key.
    uint32_t doc_position;


    StubIndexEntry(std::string k, uint32_t d) : key(k), doc_position(d) {};

    bool operator<(const StubIndexEntry &other) const {
        return key < other.key;
    }
};

inline bool operator<(const Base26Num &other, const StubIndexEntry &stub) {
    return other < stub.key;
}

inline bool operator<(const StubIndexEntry &stub, const Base26Num &other) {
    return stub.key < other;
}


/**
 * Similar to SortedKeysIndex, but it only loads a specific subset of the index into memory.
 * For example, it loads only every 64th term into memory. The string is converted to a base26 number (as the string
 * is normalized to only contain uppercase English characters). For every query, it binary searches through the sorted
 * array and seeks to that location on disk to lookup the specific key and associated documents.
 */
class SortedKeysIndexStub {

    std::vector<StubIndexEntry> index;
    std::ifstream file;

    void fill_from_file(int interval);

public:
    explicit SortedKeysIndexStub(std::filesystem::path path) : file(path, std::ios_base::binary) {
        fill_from_file(32);
    };

    SortedKeysIndexStub() = default;

    std::vector<MultiSearchResult> search_keys(std::vector<std::string> keys, std::string mode = "AND");


    robin_hood::unordered_map<uint32_t, MultiSearchResult> search_key(const std::string &term);

    robin_hood::unordered_map<uint32_t, MultiSearchResult>
    search_key_prefix_match(const std::string &term,
                            robin_hood::unordered_map<uint32_t, MultiSearchResult> &prev_result);
};


#endif //GAME_SORTEDKEYSINDEXSTUB_H
