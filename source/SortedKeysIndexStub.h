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
    uint64_t num; // Represent 3 alphabet letters in uint16_t.
    explicit Base26Num(std::string from);
    explicit Base26Num(uint64_t num): num(num){};

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

    mutable std::ifstream frequencies, terms;
    std::unique_ptr<char[]> buffer;

public:
    std::vector<StubIndexEntry> index;

    explicit SortedKeysIndexStub(std::filesystem::path frequencies,
                                 std::filesystem::path terms);

//    SortedKeysIndexStub& operator=(SortedKeysIndexStub&& other) {
//        frequencies.swap(other.frequencies);
//        terms.swap(other.terms);
//        index = other.index;
//        return *this;
//    }

    SortedKeysIndexStub() = default;

    TopDocs search_one_term(const std::string& term) const;
    TopDocs search_many_terms(const std::vector<std::string> &terms);

    static TopDocs collection_merge_search(std::vector<SortedKeysIndexStub>& indices, const std::vector<std::string>& search_terms);
};



#endif //GAME_SORTEDKEYSINDEXSTUB_H
