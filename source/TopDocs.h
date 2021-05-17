
#ifndef GAME_TOPDOCS_H
#define GAME_TOPDOCS_H

#include <cstring>
#include <vector>
#include <cassert>
#include "DocumentPositionPointer.h"


class TopDocs {
    std::vector<DocumentPositionPointer_v2> docs;
    bool is_sorted = false;
public:
    TopDocs(int reservation = 50) {
        docs.reserve(reservation);
    };

    void append_multi(std::vector<DocumentPositionPointer_v2> other) {
#ifndef MERGE_SORT
        auto begin = other.begin();
        auto end = other.end();

        auto my_begin = docs.begin();
        auto my_end = docs.end();

        std::vector<DocumentPositionPointer_v2> new_merged;
        new_merged.reserve(other.size() + docs.size());

        std::merge(begin, end, my_begin, my_end, std::back_inserter(new_merged));

        docs = std::move(new_merged);
#else
        auto prev_doc_size = docs.size();
        docs.resize(docs.size() + other.size());
        memcpy(docs.data() + prev_doc_size, other.data(), other.size() * sizeof (DocumentPositionPointer_v2));
#endif
    }

    void append_multi(TopDocs other) {
        append_multi(std::move(other.docs));
    }

    void sort_by_ids() {
#ifndef MERGE_SORT
#else
        std::sort(docs.begin(), docs.end(), [](const auto &t, const auto &t1) {
            return t.document_id < t1.document_id;
        });
#endif
    }

    void merge_similar_docs() {
        auto prev_doc = docs.begin();
        auto collected_score = 0;

        // Merge similar docs.
        for (auto doc = docs.begin(); doc != docs.end(); doc++) {
            if (doc->document_id != prev_doc->document_id || docs.end() - doc == 1) {
                prev_doc->frequency = collected_score;
                prev_doc = doc;
            } else {
                collected_score += doc->frequency;
                doc->frequency = 0;
                doc->document_id = 0;
            }
        }

        docs.erase(std::remove_if(docs.begin(), docs.end(), [](const auto &t) {
            return !(t.frequency || t.document_id);
        }), docs.end());

        std::sort(docs.begin(), docs.end(), [](auto &t, auto &t1) {
            return t.frequency < t1.frequency;
        });
    }

    auto cbegin() const {
        return docs.cbegin();
    }

    auto cend() const {
        return docs.cend();
    }

    auto begin() {
        return docs.begin();
    }

    auto end() { return docs.end(); }

    auto size() const { return docs.size(); }

};


#endif //GAME_TOPDOCS_H
