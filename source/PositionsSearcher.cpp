#include <unordered_map>
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

    auto sum = 0U;
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
            out.emplace_back(df.document_id, counter);
        }
    }

    return out;

}


//! Scales the document score according to how far the matched terms are.
int position_difference_scaler(uint32_t posdiff);


//! Inserts an element into a static array by replacing zero-elements with `value`
template<typename Container>
void insert_to_array(Container &array, uint32_t value);

//! Iterates through two sorted lists using two pointers (fingers) to find two terms with least difference between the two lists.
//! \tparam T
//! \param &first1 Iterator/pointer type to beginning of first range. Is also set to the closest position
//! \param last1 Last of first range
//! \param first2 Beginning of second range
//! \param last2 Last of second range
//! \return minimum difference
template<typename T>
int two_finger_find_min(T &first1, T last1, T first2, T last2) {
    assert(first1->document_id == (last1 - 1)->document_id);
    assert(first2->document_id == (last2 - 1)->document_id);

    uint32_t min_score = std::numeric_limits<uint32_t>::max();
    auto &curmin = first1;
    // assert that no copy
    assert(curmin.base() == first1.base());

    while (first1 < last1) {
        if (first2 == last2) break;
        if (first1->document_position > first2->document_position)
            first2++;
        else {
            if (first2->document_position - first1->document_position < min_score) {
                min_score = first2->document_position - first1->document_position;
                curmin = first1;
            }
            if (min_score <= 1) break;
            first1++;
        }
    }
    first1 = curmin;
    first1 = std::min(first1, last1 - 1);
    first2 = std::min(first2, last2 - 1);
    return static_cast<int>(min_score);
}


//! Uses positioning information to rerank the documents list. Documents that have matching terms closer together are boosted to the top.
//! \param index SortedKeysIndexStub (needed for position information)
//! \param tds
//! \param td
//! \return
/*
DocumentsMatcher::TopDocsWithPositions
PositionsSearcher::rerank_by_positions(const PositionsList &positions_list, const TopDocs &td,
                                       const std::vector<std::string> &query_terms) {
    using TopDocsWithPositions = DocumentsMatcher::TopDocsWithPositions;
    TopDocsWithPositions ret(td);

    if (positions_list.empty()) return ret;

    // For each document found...
    for (auto d = ret.begin(); d < ret.end(); d++) {
        int pos_difference = 0;

        std::cout<<"Positions size: "<<positions_list.size()<<"\n";
        // For each matched word...
        for (int i = 0; i < positions_list.size() - 1; i++) {
            auto[first1, last1] = std::equal_range(positions_list[i].begin(), positions_list[i].end(), d->document_id);
            auto[first2, last2] = std::equal_range(positions_list[i + 1].begin(), positions_list[i + 1].end(),
                                                   d->document_id);

            // This shouldn't happen if DocumentsMatcher::AND_Driver does its job correctly.
            // Still, we should check.
            if (first1 == positions_list[i].end() || first2 == positions_list[i + 1].end() ||
                first1->document_id != d->document_id || first2->document_id != d->document_id) {
                std::cout<<"Pos dont exist\n";
                print_range("error: Pos dont exist, probably AND error?", query_terms.begin(), query_terms.end());
                pos_difference = std::numeric_limits<int>::max();
                break;
            }

            auto a0 = two_finger_find_min(first1, last1, first2, last2);

            a0 -= query_terms[i].size() + 1;
            pos_difference += abs(a0);

            std::cout<<"Setting position "<<first1->document_position<<"\n";
            insert_to_array(d->matches, first1->document_position);
        }
        d->document_freq *= position_difference_scaler(pos_difference);
    }
    ret.sort_by_frequencies();
    return ret;
}
*/



int position_difference_scaler(uint32_t posdiff) {
    if (posdiff <= 2) return 100;
    if (posdiff <= 5) return 50;
    if (posdiff <= 10) return 25;
    if (posdiff <= 20) return 10;
    if (posdiff <= 50) return 1;
    return 1;
}

template<typename Container>
void insert_to_array(Container &array, uint32_t value) {
    assert(value != 0);
    for (auto &i : array) {
        if (i == 0) {
            i = value;
        }
    }
}




// ------------------------------------------------------

/***
* Tests
*/
#include "DocumentsTier.h"
#include <ctime>
#include <cstdlib>

static std::vector<DocumentPositionPointer> a{};

void Push_random_test() {
    std::srand(std::time(nullptr));
    int num = 100000;
    uint maxint = 1 << 31;
    while (num-- > 10) {
        a.emplace_back(std::rand() % (num / 9) + (1 << 25), std::rand() % maxint);
    }
    std::sort(a.begin(), a.end());
}

void Compactor_test() {
    Push_random_test();

    WordIndexEntry wie{
            "test", a
    };
    std::stringstream positions, frequencies;
    PositionsSearcher::serialize_positions(positions, wie);
    MultiDocumentsTier::serialize(wie, frequencies);

    MultiDocumentsTier::TierIterator ti(frequencies);
    auto sd = ti.read_all();
    auto test = PositionsSearcher::read_positions_all(positions, sd);

    std::cout << positions.str();
    assert(test == a);
    exit(0);
}
