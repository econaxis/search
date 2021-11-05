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

    template<typename FilterFunc>
    PositionsList fill_positions_from_docs(const SortedKeysIndexStub &index,
                                                const std::vector<std::string> &query_terms,
                                                FilterFunc filter) {
        if (query_terms.size() >= 32 || query_terms.size() < 2) {
            log("Positions searcher not active: terms size not within bounds [2, 32]");
            return {};
        }
        PositionsList positions_list;

        for (uint8_t i = 0; i < query_terms.size(); i++) {
            for (auto &pos : index.get_positions_for_term(query_terms[i])) {
                if (filter(pos.document_id)) {
                    positions_list.push_back(FoundPositions{i, pos.document_id, pos.document_position});
                }
            }
        }
        return positions_list;
    }
};


#endif //GAME_POSITIONSSEARCHER_H
