#include "DocumentFrequency.h"
#include "DocumentsMatcher.h"
#include <cassert>
#include <iostream>
#include "Serializer.h"
#include "WordIndexEntry.h"
#include "PositionsSearcher.h"

using namespace Serializer;


// If you try to read this MAGIC_NUM with normal vnum format, you'll get an error.
constexpr uint32_t MAGIC_NUM = 0xFFADCFF0;


void PositionsSearcher::serialize_positions(std::ostream &positions, const WordIndexEntry &ie) {
    // Document ID limits are implicit from the positions file.
    // Both are sorted.

    assert(std::is_sorted(ie.files.begin(), ie.files.end()));

    std::stringstream positionbuf;
    auto prevfile = DocumentPositionPointer{0, 0};
    for (auto &file : ie.files) {
        if (file.document_id == prevfile.document_id) {
            if (file.document_position < prevfile.document_position) {
                throw std::runtime_error("Positions list not sorted");
            }
            serialize_vnum(positionbuf, file.document_position - prevfile.document_position);
        } else serialize_vnum(positionbuf, file.document_position);

        prevfile = file;
    }

    // Serialize magic num to help in debugging, make sure we aren't reading the wrong frame.
    positions.write(reinterpret_cast<const char *>(&MAGIC_NUM), 4);
    positions << positionbuf.rdbuf();
}


std::vector<DocumentPositionPointer>
PositionsSearcher::read_positions_all(std::istream &positions, const std::vector<DocumentFrequency> &freq_list) {
    uint32_t magic_num;
    auto pos1 = positions.tellg();


    positions.read(reinterpret_cast<char *>(&magic_num), 4);

    if (magic_num != MAGIC_NUM) {
        throw std::runtime_error("Magic num not equals");
    }

    auto sum = 0;
    for (auto &df : freq_list) {
        sum += df.document_freq;
    }
    std::vector<DocumentPositionPointer> out;
    out.reserve(sum);
    for (auto &df : freq_list) {
        auto docs_left = df.document_freq;

        auto counter = 0U;
        while (docs_left--) {
            auto pos = read_vnum(positions);

            if (counter + pos < counter) {
                throw std::runtime_error("Number overflow");
            }
            counter += pos;
            out.push_back({df.document_id, counter});
        }
    }

    return out;

}


static std::vector<DocumentPositionPointer> a{};


#include "DocumentsTier.h"
#include <ctime>
#include <cstdlib>

void Push_random_test() {
    std::srand(std::time(nullptr));
    int num = 100000;
    uint maxint = 1 << 31;
    while (num-- > 10) {
        a.emplace_back(std::rand() % (num / 9) + (1 << 25), std::rand() % maxint);
    }
    std::sort(a.begin(), a.end());
}



// Scales the document score according to how far the matched terms are.
float position_difference_scaler(uint32_t posdiff);

template<typename Container>
void insert_to_array(Container &array, uint32_t value);

//! Iterates through two sorted lists using two pointers (fingers) to find two terms with least difference between the two lists.
//! \tparam T
//! \param first1 Iterator/pointer type to beginning of first range
//! \param last1 Last of first range
//! \param first2 Beginning of second range
//! \param last2 Last of second range
//! \return minimum difference
template<typename T>
int two_finger_find_min(T &first1, T last1, T &first2, T last2) {
    assert(first1->document_id == (last1 - 1)->document_id);
    assert(first2->document_id == (last2 - 1)->document_id);

    uint32_t curmin = 1 << 30;
    while (first1 < last1) {
        if (first2 == last2) break;
        if (first1->document_position > first2->document_position)
            first2++;
        else {
            curmin = std::min(curmin, first2->document_position - first1->document_position);
            if (curmin <= 1) break;
            first1++;
        }
    }
    return static_cast<int>(curmin);
}



DocumentsMatcher::TopDocsWithPositions
PositionsSearcher::rerank_by_positions(const SortedKeysIndexStub &index, std::vector<TopDocs> &tds, const TopDocs &td) {
    using TopDocsWithPositions = DocumentsMatcher::TopDocsWithPositions;
    TopDocsWithPositions ret(td);
    if (tds.size() >= 32 || tds.size() < 2) {
        return ret;
    }


    std::vector<std::vector<DocumentPositionPointer>> positions_list(tds.size());

    for (int i = 0; i < tds.size(); i++) {
        std::string terms_list{};
        if (auto it = tds[i].get_first_term(); it) {
            positions_list[i] = index.get_positions_for_term(*it);
            terms_list += std::string(*it) + " ";
        } else {
            log("error: Couldn't find all terms\n");
            return ret;
        }
        log("terms list: ", terms_list);
    }
    for (auto d = ret.begin(); d < ret.end(); d++) {
        int pos_difference = 0;
        for (int i = 0; i < tds.size() - 1; i++) {
            auto[first1, last1] = std::equal_range(positions_list[i].begin(), positions_list[i].end(), d->document_id);
            auto[first2, last2] = std::equal_range(positions_list[i + 1].begin(), positions_list[i + 1].end(),
                                                   d->document_id);

            if (first1->document_id != d->document_id || first2->document_id != d->document_id) {
                print("error: Pos dont exist ", **tds[i].get_first_term(), **tds[i + 1].get_first_term(),
                      index.query_filemap(d->document_id));
                pos_difference = 1 << 29;
                break;
            }

            auto a0 = two_finger_find_min(first1, last1, first2, last2);
            a0 -= strlen(*tds[i].get_first_term());

            if (a0 <= 3)
                log("Two finger find min result: ", index.query_filemap(d->document_id), first1->document_position,
                    first2->document_position);
            pos_difference += abs(a0);
            insert_to_array(d->matches, first1->document_position);
        }
        d->document_freq = d->document_freq * position_difference_scaler(pos_difference);
    }
    ret.sort_by_frequencies();
    return ret;
}

float position_difference_scaler(uint32_t posdiff) {
    if (posdiff <= 2) return 100.f;
    if (posdiff <= 5) return 50.f;
    if (posdiff <= 10) return 25.f;
    if (posdiff <= 20) return 10.f;
    if (posdiff <= 50) return 1.f;
    return 0.9f;
}

template<typename Container>
void insert_to_array(Container &array, uint32_t value) {
    for (auto &i : array) {
        if (i == 0) {
            i = value;
        }
    }
}



