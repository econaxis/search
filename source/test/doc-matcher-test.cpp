#include "all_includes.h"
#include <gtest/gtest.h>


TEST(DocumentsMatcher, test) {
    constexpr int MAXDOCID = 50000000;
    std::vector<TopDocs> a(7);
    auto all_has = 0b11111;
    std::vector<uint8_t> correct_arr(MAXDOCID);
    for (int i = 0; i < a.size(); i++) {
        std::set<uint32_t> docids;
        for (int j = 0; j < 100000; j++) {
            auto id = ::rand() % MAXDOCID;
            docids.insert(id);
        }
        for (auto &j : docids) {
            correct_arr[j] |= 1 << i;
            a[i].docs.emplace_back(j, ::rand() % 2000);
        }
        std::sort(a[i].docs.begin(), a[i].docs.end());
    }

    std::vector<uint32_t> intersected;

    int unmatched = 0;
    for (int i = 0; i < correct_arr.size(); i++) {
        if (correct_arr[i] % all_has) intersected.push_back(i);
        else {
            unmatched++;
        }
    }
    log("TEST: Unmatched percentage:", unmatched * 100 / correct_arr.size(), " Num matching elements: {}",
        intersected.size());

    std::copy_if(correct_arr.begin(), correct_arr.end(), std::back_inserter(intersected),
                 [&](auto i) { return i & all_has; });


    auto and_result = DocumentsMatcher::AND_Driver(a);

    for (int i = 0; i < and_result.size(); i++) {
        ASSERT_EQ(intersected[i], and_result.docs[i].document_id);
    }
}

TEST(DocumentsMatcher, empty_test) {
    std::vector<TopDocs> td(5);
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).size(), 0);
}

TEST(TopDocs, empty_term) {
    TopDocs td(std::vector<DocumentFrequency>{{5, 2},
                                              {8, 3}});

    auto tdbak = td;

    td.extend_from_tier_iterator(1);

    ASSERT_EQ(tdbak.docs, td.docs);
}