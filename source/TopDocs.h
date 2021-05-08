//
// Created by henry on 2021-05-06.
//

#ifndef GAME_TOPDOCS_H
#define GAME_TOPDOCS_H

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
        std::move(other.begin(), other.end(), std::back_inserter(docs));
    }

    void sort_and_score() {
        if (is_sorted) return;
        auto search_max = docs.begin() + 100;
        if (docs.size() < search_max - docs.begin()) search_max = docs.end();

        std::partial_sort(docs.begin(), search_max, docs.end(), [](const auto &t, const auto &t1) {
            return t.document_id < t1.document_id;
        });
        is_sorted = true;


        auto prev_doc = docs.begin();
        auto collected_score = 0;
        for (auto doc = docs.begin(); doc != search_max; doc++) {
            if (doc->document_id != prev_doc->document_id || docs.end() - doc == 1) {
                prev_doc->frequency = collected_score;
                prev_doc = doc;
            } else {
                collected_score += doc->frequency;
                doc->frequency = 0;
                doc->document_id = 0;
            }
        }

        docs.erase(std::remove_if(docs.begin(), search_max, [](const auto &t) {
            return !(t.frequency || t.document_id);
        }), docs.end());

        std::partial_sort(docs.begin(), search_max, docs.end(), [](auto &t, auto &t1) {
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
