#ifndef GAME_WORDINDEXENTRY_H
#define GAME_WORDINDEXENTRY_H

#include <fmt/ostream.h>
#include <string>
#include <vector>
#include <cassert>
#include <numeric>
#include "DocumentPositionPointer.h"
#include "DocumentsTier.h"
#include "DocumentFrequency.h"

struct PreviewResult {
    std::streampos frequencies_pos;
    std::streampos positions_pos;
    std::string key;
};

struct WordIndexEntry_v2 {
    std::string key;
    SingleDocumentsTier files;
};

/**
 * Each WordIndexEntry is a list of files and positions that contain the word "key" + where the file has that word.
 */
struct WordIndexEntry {
    std::string key;
    std::vector<DocumentPositionPointer> files;
    float frequency_multiplier = 1.0f;

    void merge_into(const WordIndexEntry& other) {
        assert(other.key == key);
        assert(std::is_sorted(files.begin(), files.end()));
        assert(std::is_sorted(other.files.begin(), other.files.end()));
//        if(!(files.back().document_id < other.files.front().document_id || files.front().document_id > other.files.back().document_id)) {
//            fmt::print("Merge error {} {} {} {}", files.front().document_id, files.back().document_id, other.files.front().document_id, other.files.back().document_id);
//            assert(false);
//        }

        files.insert(files.end(), other.files.begin(), other.files.end());

        // todo: can optimize, no need sorting
        std::sort(files.begin(), files.end());
        assert(std::is_sorted(files.begin(), files.end()));
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

inline bool operator==(const WordIndexEntry &elem1, const WordIndexEntry &elem2) {
    return elem1.key == elem2.key;
}

inline bool operator<(const WordIndexEntry &elem1, const std::string &str) {
    return elem1.key < str;
}

inline bool operator<(const std::string &str, const WordIndexEntry &elem1) {
    return str < elem1.key;
}

#endif //GAME_WORDINDEXENTRY_H
