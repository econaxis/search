#ifndef GAME_WORDINDEXENTRY_H
#define GAME_WORDINDEXENTRY_H


#include <string>
#include <vector>
#include <cassert>
#include <numeric>
#include "DocumentPositionPointer.h"
#include "CustomAllocatedVec.h"
#include "DocumentsTier.h"
#include "DocumentFrequency.h"

struct PreviewResult {
    std::streampos frequencies_pos;
    std::streampos positions_pos;
    std::string key;
};

struct WordIndexEntry_v2 {
    std::string key;
    uint32_t term_pos;

    SingleDocumentsTier files;
};

struct WordIndexEntry_unsafe {
    std::string key;
    using VecType = CustomAllocatedVec<DocumentPositionPointer, 3, 50000>;
    VecType files;

    WordIndexEntry_unsafe(std::string key, const std::vector<DocumentPositionPointer>& f) : key(std::move(key)), files() {
        for (const auto &i : f) {
            files.push_back(i);
        }
    }

    WordIndexEntry_unsafe() : key("a"), files() {};


};

/**
 * Each WordIndexEntry is a list of files that contain the word "key" + where the file has that word.
 */
struct WordIndexEntry {
    std::string key;
    std::vector<DocumentPositionPointer> files;

    void merge_into(const WordIndexEntry& other) {
        assert(other.key == key);
        assert(files.back().document_id < other.files.front().document_id);
        assert(std::is_sorted(files.begin(), files.end()));
        assert(std::is_sorted(other.files.begin(), other.files.end()));

        files.insert(files.end(), other.files.begin(), other.files.end());
    }

    std::vector<DocumentFrequency> get_frequencies_vector() const {
        assert(std::is_sorted(files.begin(), files.end()));
        std::vector<DocumentFrequency> freq_data;
        std::size_t prev_same_idx = 0;
        for (std::size_t i = 0; i <= files.size(); i++) {
            if (i == files.size()) {
                freq_data.emplace_back(files[i - 1].document_id, i - prev_same_idx);
                break;
            }
            if (files[i].document_id != files[prev_same_idx].document_id) {
                // We reached a different index.
                auto num_occurences_in_term = i - prev_same_idx;
                auto docid = files[prev_same_idx].document_id;
                freq_data.emplace_back(docid, num_occurences_in_term);
                prev_same_idx = i;
            }
        }

        return freq_data;
    }


};

inline bool operator<(const WordIndexEntry &elem1, const WordIndexEntry &elem2) {
    return elem1.key < elem2.key;
}


inline bool operator<(const WordIndexEntry &elem1, const std::string &str) {
    return elem1.key < str;
}

inline bool operator<(const std::string &str, const WordIndexEntry &elem1) {
    return str < elem1.key;
}

#endif //GAME_WORDINDEXENTRY_H
