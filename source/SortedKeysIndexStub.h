#ifndef GAME_SORTEDKEYSINDEXSTUB_H
#define GAME_SORTEDKEYSINDEXSTUB_H


#include <vector>
#include <cstdint>
#include <filesystem>
#include <robin_hood/robin_hood.h>
#include <fstream>
#include "DocumentPositionPointer.h"
#include "TopDocs.h"


struct Base26Num {
    uint32_t num; // Represent 3 alphabet letters in uint16_t.
    explicit Base26Num(std::string from);
    explicit Base26Num(uint32_t num): num(num){};

    bool operator<(Base26Num other) const {
        return num < other.num;
    }

    Base26Num operator+(Base26Num other) {
        return Base26Num{num + other.num};
    }
    Base26Num operator-(Base26Num other) {
        if (other.num >= num)  return *this;
        else return Base26Num{num - other.num};
    }
};


/**
 * Makes up every nth element of the larger index. The key is stored as a Base26Number (uint64_t) for memory efficiency.
 * Equivalent to WordIndexEntry if we were to load the whole index to memory with SortedKeysIndex.
 */
struct StubIndexEntry {
    Base26Num key;

    uint32_t terms_position;
    // The position on the file that this key resides at.
    // At this position, it's the start of WordIndexEntry for this key.
    uint32_t doc_position;

    std::string _debugkey;

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

    std::ifstream frequencies, terms;

    void fill_from_file(int interval);

public:
    std::vector<StubIndexEntry> index;

    explicit SortedKeysIndexStub(std::filesystem::path frequencies,
                                 std::filesystem::path terms);;

    explicit SortedKeysIndexStub(std::vector<StubIndexEntry> index) : index(std::move(index)) {};

    void operator=(SortedKeysIndexStub&& other) {
        frequencies.swap(other.frequencies);
        terms.swap(other.terms);
        index = other.index;
    }

    SortedKeysIndexStub() = default;

    TopDocs search_one_term(std::string term);


//    std::vector<SafeMultiSearchResult> search_keys(std::vector<std::string> keys, std::string mode = "AND");
//
//    robin_hood::unordered_map<uint32_t, MultiSearchResult>
//    search_key_prefix_match(const std::string &term,
//                            robin_hood::unordered_map<uint32_t, MultiSearchResult> &prev_result);
//
//    robin_hood::unordered_map<uint32_t, MultiSearchResult> search_key(std::string term);
    TopDocs search_many_terms(std::vector<std::string> terms);
};


#endif //GAME_SORTEDKEYSINDEXSTUB_H
