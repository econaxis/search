#ifndef GAME_SORTEDKEYSINDEX_H
#define GAME_SORTEDKEYSINDEX_H

#include <set>
#include "WordIndexEntry.h"
#include "Serializer.h"
#include <ostream>
#include <sstream>
#include <unordered_set>
#include <map>

struct MultiSearchResult {
    uint32_t docid;
    uint32_t score;
    std::vector<std::pair<uint32_t, uint32_t>> positions;

    MultiSearchResult(uint32_t docid, uint32_t score, std::initializer_list<std::pair<uint32_t, uint32_t>> positions) : docid(docid),
                                                                                                   score(score),
                                                                                                   positions(
                                                                                                           positions) {};

    MultiSearchResult() = default;

    static bool SortScore(const MultiSearchResult& t1, const MultiSearchResult& t2) {
        return t1.score < t2.score;
    }
};

using SearchResult = std::vector<DocumentPositionPointer>;

class SortedKeysIndex {
private:


    std::vector<WordIndexEntry> index;

    std::optional<const SearchResult *>
    search_key(const std::string &key) const;

public:


    std::vector<WordIndexEntry> &get_index();

    friend void Serializer::serialize(std::ostream &stream, const SortedKeysIndex &index);

    inline friend std::ostream &operator<<(std::ostream &os, const SortedKeysIndex &a) {
        std::ostringstream buffer;
        for (const auto &row : a.index) {
            buffer << "\"" << row.key << "\": [";
            for (const auto &doc_pointer : row.files) {
                buffer << doc_pointer.document_id << ":" << doc_pointer.document_position << ", ";
            }
            buffer.seekp(-2, std::ios_base::cur);
            buffer << "]\n";
        }
        os << buffer.str();
        return os;
    }

    explicit SortedKeysIndex(std::vector<WordIndexEntry> index);

    SortedKeysIndex() = default;

    void sort_and_group_shallow();

    void merge_into(SortedKeysIndex &other);

    void sort_and_group_all();

    std::vector<MultiSearchResult> search_keys(const std::vector<std::string> &keys, std::string type = "AND") const;

};


#endif //GAME_SORTEDKEYSINDEX_H
