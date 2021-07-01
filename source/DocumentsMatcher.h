#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H

#include "TopDocs.h"
#include <vector>
#include "SortedKeysIndexStub.h"
#include <cstdint>

namespace DocumentsMatcher {

    TopDocs AND_Driver(std::vector<TopDocs>& outputs);


    TopDocs backup(std::vector<TopDocs> &results);

    struct TopDocsWithPositions {
        TopDocsWithPositions() = default;

        struct Elem {
            Elem(unsigned int i, unsigned int i1);

            uint32_t document_id;
            uint32_t document_freq;
            std::array<uint32_t, 4> matches = {0};
        };

        std::vector<Elem> docs;

        void sort_by_frequencies() {
            std::sort(docs.begin(), docs.end(), [](auto &a, auto &b) {
                return a.document_freq < b.document_freq;
            });
        }

        explicit TopDocsWithPositions(const TopDocs &td) {
            docs.reserve(td.size());
            for (auto t: td) docs.emplace_back(t.document_id, t.document_freq);
        }


        void insert(TopDocsWithPositions other) {
            std::move(other.docs.begin(), other.docs.end(), std::back_inserter(docs));
        }

        std::vector<Elem>::const_iterator begin() const { return docs.begin(); }

        std::vector<Elem>::const_iterator end() const { return docs.end(); }

        std::vector<Elem>::iterator begin() { return docs.begin(); }

        std::vector<Elem>::iterator end() { return docs.end(); }


    };

    inline bool operator<(const TopDocsWithPositions::Elem &one, int two) {
        return one.document_freq < two;
    }

    TopDocsWithPositions combiner_with_position(SortedKeysIndexStub &index, std::vector<TopDocs> &outputs,
                                                const std::vector<std::string> &query_terms);
};


#endif //GAME_DOCUMENTSMATCHER_H
