#ifndef GAME_TOPDOCSRESULTSJOINER_H
#define GAME_TOPDOCSRESULTSJOINER_H

#include <span>
#include "DocumentsMatcher.h"
#include "Tokenizer.h"

namespace TopDocsResultsJoiner {
    class IterativeInputter {
        using doctype = DocumentsMatcher::TopDocsWithPositions::Elem;

        struct imbueddoctype {
            uint16_t indexno;
            doctype doc;

            bool operator<(const IterativeInputter::imbueddoctype &other) {
                return doc.document_freq < other.doc.document_freq;
            }
        };

        std::vector<imbueddoctype> docs;
        uint16_t num_indices = 0;
        float curavgscore = 0;

        void add_to_average(float value) {
            curavgscore = (docs.size() * curavgscore + value) / (docs.size() + 1);
        }

        uint32_t compute_cutoff() {
            if (docs.size() < 40) return 0;
            else return curavgscore / 1.2F;
        }

    public:
        void join_results(std::vector<doctype> d) {
            // Imbue top 4 bits of docid with index tag (which index the doc id is associated with)
            for (auto pair = d.rbegin(); pair != d.rend(); pair++) {
                if (pair->document_freq >= compute_cutoff()) {
                    add_to_average(pair->document_freq);
                    docs.push_back({num_indices, std::move(*pair)});
                }
            }
            num_indices++;
        }

        struct Iterator {
            using Self = IterativeInputter::Iterator;
            imbueddoctype *pointer, *end;

            bool operator!=(const Self &other) const {
                return other.pointer != pointer;
            }

            void next() {
                pointer++;
            }

            imbueddoctype *operator->() {
                return pointer;
            }
            Self operator+(int num) const {
                assert(pointer + num < end);
                return {pointer + num, end};
            }

            bool valid() const {
                return pointer < end;
            }
        };

        Iterator get_results() {
            std::sort(docs.begin(), docs.end());
            return Iterator{docs.begin().base(), docs.end().base()};
        }
    };
//
//    inline IterativeInputter query_multiple_indices(std::span<const SortedKeysIndexStub> indices,
//                                  const std::vector<std::string> &terms) {
//        IterativeInputter join;
//        for (const auto &indice : indices) {
//            auto temp = indice.search_many_terms(terms);
//
//            // If we dont' want positions_matching, call DocumentsMatcher::AND_Driver(temp);
//            auto topdocs_with_pos = DocumentsMatcher::combiner_with_position(indice, temp, terms);
//
//            // Insert it into the "joiner" instance. This reconciles topdocs across multiple indices and handles cutoff scores.
//            join.join_results(std::move(topdocs_with_pos.docs));
//        }
//        return join;
//    }
}


#endif //GAME_TOPDOCSRESULTSJOINER_H
