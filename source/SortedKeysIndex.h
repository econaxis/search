#ifndef GAME_SORTEDKEYSINDEX_H
#define GAME_SORTEDKEYSINDEX_H

#include <set>
#include "WordIndexEntry.h"
#include "Serializer.h"
#include <ostream>
#include <sstream>

class SortedKeysIndex {
private:
    std::vector<WordIndexEntry> index;
    const std::vector<DocumentPositionPointer> *search_key(const std::string &key) const;
public:

    std::vector<WordIndexEntry>& get_index();
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

    std::vector<DocumentPositionPointer> search_keys(const std::vector<std::string> &keys) const;

    void sort_and_group_shallow();
    int index_size() const;

    void merge_into(SortedKeysIndex &other);

    void reserve_more(size_t len);

    void sort_and_group_all();
};


#endif //GAME_SORTEDKEYSINDEX_H
