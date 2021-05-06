#ifndef GAME_SORTEDKEYSINDEX_H
#define GAME_SORTEDKEYSINDEX_H

#include <set>
#include "WordIndexEntry.h"
#include "Serializer.h"
#include <ostream>
#include <sstream>
#include <unordered_set>
#include <map>

struct MultiSearchResult;


struct PairUint32 {
    uint32_t first, second;

    PairUint32() = default;

    PairUint32(uint32_t a, uint32_t b) : first(a), second(b) {};
};
inline int diff = 0, diff_from_prefix = 0;

struct MultiSearchResult {
    MultiSearchResult(MultiSearchResult &&other) {
        operator=(std::move(other));
    }

    MultiSearchResult &operator=(MultiSearchResult &&other) noexcept;
    MultiSearchResult() = delete;
    uint32_t docid;
    uint32_t score;
    PairUint32 *positions = nullptr;
    bool moved_from = false;
    std::size_t cur_index = 0;


    MultiSearchResult(uint32_t docid, uint32_t score);

    MultiSearchResult(uint32_t docid, uint32_t score, PairUint32 init_pos);
    MultiSearchResult(uint32_t docid, uint32_t score, PairUint32 init_pos, bool sent);

    ~MultiSearchResult();

    static bool SortScore(const MultiSearchResult &t1, const MultiSearchResult &t2) {
        return t1.score < t2.score;
    }

    bool operator==(const MultiSearchResult &other) const {
        return docid == other.docid;
    }

    /**
     * Inserts position into our array of positions.
     * @param elem what element to insert
     * @return True if insertion was successful, false if we're full.
     */
    bool insert_position(PairUint32 elem);


    struct Iterator {
        Iterator &operator++() {
            member++;
            return *this;
        }

        bool operator==(const Iterator &other) const {
            return member == other.member;
        }

        bool operator!=(const Iterator &other) const {
            return !(member == other.member);
        }

        PairUint32 &operator*() {
            return *member;
        }

        PairUint32 &operator->() {
            return *member;
        }

        PairUint32 *member;
    };

    Iterator begin() {
        return Iterator{positions};
    }

    Iterator end() const {
        return Iterator{positions + cur_index + 1};
    }
};
//inline std::map<uint32_t, std::vector<MultiSearchResult*>> debug;
struct SafeMultiSearchResult {
    uint32_t docid;
    uint32_t score;
    std::vector<PairUint32> positions;
    SafeMultiSearchResult(std::size_t suggested);
    explicit SafeMultiSearchResult(MultiSearchResult&& other) {
        docid = other.docid;
        score = other.score;

        if(other.positions!= nullptr) {
            positions = std::vector<PairUint32>(other.positions, other.positions + other.cur_index + 1);
        }
        other.positions = nullptr;
    }
    static bool SortScore(const SafeMultiSearchResult &t1, const SafeMultiSearchResult &t2) {
        return t1.score < t2.score;
    }
};


inline void swap(MultiSearchResult &a, MultiSearchResult &b) {
    MultiSearchResult c(std::move(a));
    a = MultiSearchResult(std::move(b));
    b = std::move(c);
}


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

    std::vector<SafeMultiSearchResult>
    search_keys(const std::vector<std::string> &keys, std::string type = "AND") const;

};


#endif //GAME_SORTEDKEYSINDEX_H
