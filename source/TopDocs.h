
#ifndef GAME_TOPDOCS_H
#define GAME_TOPDOCS_H

#include <cstring>
#include <vector>
#include <cassert>
#include "DocumentPositionPointer.h"
#include "CustomAllocatedVec.h"


class TopDocs {
public:
    std::vector<DocumentPositionPointer_v2> docs;
    TopDocs(int reservation = 50) {
//        if (reservation != 0) docs.reserve(reservation);
        docs.reserve(50);
    };


    auto begin() const {
        return docs.begin();
    }

    auto end() const { return docs.end(); }

    auto begin() { return docs.begin(); }

    auto end() { return docs.end(); }

    auto size() const { return docs.size(); }

    template<typename Iterator>
    void append_multi(Iterator ibegin, Iterator iend) {
//        assert(cur_size < docs.size());
//
//        if(iend - ibegin + cur_size < docs.size()) {
//            docs.resize(iend-ibegin + cur_size * 1.5F);
//        }
//        auto prevsize = cur_size;
//
//        std::memcpy(docs.data() + cur_size, ibegin.base(), (iend - ibegin) * sizeof(typename Iterator::value_type));
//
//        cur_size += iend - ibegin;
        auto prevsize = docs.size();
        docs.resize(iend - ibegin + prevsize);
        std::memcpy(docs.data() + prevsize, ibegin.base(), (iend - ibegin) * sizeof(typename Iterator::value_type));
//        std::copy(ibegin, iend, std::back_inserter(docs));
//
//        cur_size += new_size;
//        std::memcpy(reinterpret_cast<char *>(my_end.base()), ibegin.base(),
//                    new_size * sizeof(DocumentPositionPointer_v2));

        std::inplace_merge(begin(), begin() + prevsize, end());

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
        if(docs.empty()) return;
//        assert(cur_size <= docs.size());
//        assert(std::is_sorted(begin(), end()));

        auto &prev_doc = *begin();
        auto collected_score = 0;

        // Merge similar docs.
        for (int i = 0; i < docs.size(); i++) {
            auto &doc = docs[i];
            if (doc.document_id != prev_doc.document_id) {
                prev_doc.frequency += collected_score;
                prev_doc = doc;
                collected_score = 0;
            } else {
                collected_score += doc.frequency;
                doc.frequency = 0;
                doc.document_id = 0;
            }
            prev_doc = doc;
        }
        prev_doc.frequency = collected_score;

//        docs.erase(std::remove_if(docs.begin(), docs.end(), [](const auto &t) {
//            return !(t.frequency && t.document_id);
//        }), docs.end());

        std::sort(begin(), end(), [](auto &t, auto &t1) {
            return t.frequency < t1.frequency;
        });
    }


};


#endif //GAME_TOPDOCS_H
