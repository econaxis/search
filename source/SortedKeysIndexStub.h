#ifndef GAME_SORTEDKEYSINDEXSTUB_H
#define GAME_SORTEDKEYSINDEXSTUB_H


#include <vector>
#include <cstdint>
#include <filesystem>
#include "DocumentPositionPointer.h"
#include "SortedKeysIndex.h"

inline bool lexicograhical_string(const std::string &s1, const std::string &s2) {
    return std::lexicographical_compare(s1.begin(), s1.end(), s2.begin(), s2.end());
}


struct Base26Num {
    uint64_t num; // Represent 3 alphabet letters in uint16_t.

    Base26Num(const std::string &from);

    bool operator<(Base26Num other) const {
        return num < other.num;
    }
};

int string_prefix_compare(const std::string &shorter, const std::string &longer);


struct StubIndexEntry {
    Base26Num key;
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

class SortedKeysIndexStub {
    std::vector<StubIndexEntry> index;
    std::ifstream file;

public:
    SortedKeysIndexStub(std::filesystem::path path) : file(path, std::ios_base::binary) {};

    SortedKeysIndexStub() = default;

    std::vector<MultiSearchResult> search_keys(std::vector<std::string> keys, std::string mode = "AND");


    void fill_from_file(int interval);

    std::optional<SearchResult> search_key(const std::string &term);

    void
    search_key_prefix_match(const std::string &term, std::map<uint32_t, MultiSearchResult> &prev_result);
};


#endif //GAME_SORTEDKEYSINDEXSTUB_H
