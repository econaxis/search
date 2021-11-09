//
// Created by henry on 2021-06-03.
//

#ifndef GAME_POSITIONSSEARCHER_H
#define GAME_POSITIONSSEARCHER_H

#include <istream>
#include <vector>
#include "DocumentPositionPointer.h"
#include "DocumentFrequency.h"
#include "TopDocs.h"
#include "SortedKeysIndexStub.h"
#include "DocumentsMatcher.h"

using SingleDocumentsTier = std::vector<DocumentFrequency>;

struct FoundPositions {
    uint8_t terms_index;
    uint32_t document_id;
    uint32_t document_position;
};
using PositionsList = std::vector<FoundPositions>;

struct WordIndexEntry;

namespace PositionsSearcher {

    void serialize_positions(std::ostream &positions, const WordIndexEntry &ie);

    std::vector<DocumentPositionPointer>
    read_positions_all(std::istream &positions, const SingleDocumentsTier &freq_list);

    // Don't use
//    DocumentsMatcher::TopDocsWithPositions
//    rerank_by_positions(const PositionsList &positions_list, const TopDocs &td, const std::vector<std::string>& query_terms);

    // todo: use all prefix matched terms too
    template<typename FilterFunc>
    PositionsList fill_positions_from_docs(const SortedKeysIndexStub &index,
                                           std::vector<TopDocs> &tdvec,
                                           FilterFunc filter) {
        PositionsList positions_list;
        int counter = 0;
        for (auto &td: tdvec) {
            for (auto term = td.pop_next_term(); term.has_value(); term = td.pop_next_term()) {
                counter++;
                for (auto &pos : index.get_positions_from_streampos(term->freq, term->positions)) {
                    if (filter(pos.document_id)) {
                        positions_list.push_back(
                                FoundPositions{static_cast<uint8_t>(term->term.size()), pos.document_id,
                                               pos.document_position});
                    }
                }
            };

        }
        return positions_list;
    };
}

#endif //GAME_POSITIONSSEARCHER_H
