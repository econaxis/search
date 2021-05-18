#ifndef GAME_WORDINDEXENTRY_H
#define GAME_WORDINDEXENTRY_H


#include <string>
#include <vector>
#include <cassert>
#include "DocumentPositionPointer.h"
#include "CustomAllocatedVec.h"

struct PreviewResult {
    std::streamoff freq_off;
    std::streamoff term_off;
    std::string key;
};

struct WordIndexEntry_v2 {
    std::string key;
    uint32_t term_pos;

    std::vector<DocumentPositionPointer_v2> files;
};

struct WordIndexEntry_unsafe {
    std::string key;
    CustomAllocatedVec<DocumentPositionPointer, 7, 100000> files;

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

    std::vector<std::pair<uint32_t, uint32_t>> get_frequencies_vector() const {
        assert(std::is_sorted(files.begin(), files.end()));
        std::vector<std::pair<uint32_t, uint32_t>> freq_data;
        int prev_same_idx = 0;
        for (int i = 0; i <= files.size(); i++) {
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
