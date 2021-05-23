#ifndef GAME_SORTEDKEYSINDEXSTUB_H
#define GAME_SORTEDKEYSINDEXSTUB_H


#include <vector>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include "TopDocs.h"
#include "Base26Num.h"



/**
 * Makes up every nth element of the larger index. The key is stored as a Base26Number (uint64_t) for memory efficiency.
 * Equivalent to WordIndexEntry if we were to load the whole index to memory with SortedKeysIndex.
 */
struct StubIndexEntry {
    Base26Num key;

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
#include "DocIDFilePair.h"
#include <immintrin.h>
/**
 * Similar to SortedKeysIndex, but it only loads a specific subset of the index into memory.
 * For example, it loads only every 64th term into memory. The string is converted to a base26 number (as the string
 * is normalized to only contain uppercase English characters). For every query, it binary searches through the sorted
 * array and seeks to that location on disk to lookup the specific key and associated documents.
 */
class SortedKeysIndexStub {
private:

    mutable std::unique_ptr<__m256[]> alignedbuf;
    mutable std::ifstream frequencies, terms;


    std::vector<DocIDFilePair> filemap;
    std::unique_ptr<char[]> buffer;
    int(&filterfunc)(const std::string&, const std::string&);
public:

    SortedKeysIndexStub(std::string suffix);
    static constexpr int MAX_FILES_PER_TERM = 30000;
    std::vector<StubIndexEntry> index;


    std::string query_filemap(uint32_t docid) {
        auto find = std::find(filemap.begin(), filemap.end(), docid);
        if (find == filemap.end()) return "File not found";
        else return find->file_name;
    }
    SortedKeysIndexStub() = default;

    TopDocs search_one_term(const std::string& term) const;
    TopDocs search_many_terms(const std::vector<std::string> &terms);

    static TopDocs collection_merge_search(std::vector<SortedKeysIndexStub>& indices, const std::vector<std::string>& search_terms);
};



#endif //GAME_SORTEDKEYSINDEXSTUB_H
