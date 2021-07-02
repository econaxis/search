#include "all_includes.h"
#include <gtest/gtest.h>

using namespace std;

TEST(DocumentsMatcher, test) {
    constexpr int MAXDOCID = 2000000;
    constexpr int num_elems = MAXDOCID / 6;
    std::vector<TopDocs> a(6);
    vector<set<uint32_t>> intersect(6);

    for (int i = 0; i < a.size(); i++) {
        robin_hood::unordered_set<uint32_t> this_docids;
        this_docids.reserve(num_elems);
        for (int j = 0; j < num_elems; j++) {
            auto id = utils::rand() % MAXDOCID;
            if (i == 0) intersect[0].insert(id);
            else {
                if (intersect[i - 1].find(id) != intersect[i - 1].end()) {
                    intersect[i].insert(id);
                }
            }
            this_docids.insert(static_cast<uint32_t>(id));
        }
        for (auto &j : this_docids) {
            a[i].docs.emplace_back(j, 1);
        }
        std::sort(a[i].docs.begin(), a[i].docs.end());
    }

    log("TEST: Matching:", intersect.back().size(), "total:", MAXDOCID);

    auto and_result = DocumentsMatcher::AND_Driver(a);

    std::vector<uint32_t> intersected(intersect.back().begin(), intersect.back().end());

    ASSERT_EQ(intersected.size(), and_result.size());
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

TEST(DocumentsMatcher, one_filled_test) {
    std::vector<TopDocs> td(3);
    td[0] = TopDocs({
                            {12, 9}
                    });
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).size(), 0);
}

TEST(DocumentsMatcher, two_filled_test) {
    std::vector<TopDocs> td(3);
    td[0] = TopDocs({
                            {12, 9}
                    });
    td[1] = TopDocs({
                            {1999, 9},
                            {2001, 9},
                            {3002, 9}
                    });
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).size(), 0);
}

TEST(DocumentsMatcher, two_filled_test_v2) {
    std::vector<TopDocs> td(3);
    td[0] = TopDocs({
                            {12, 9}
                    });
    td[1] = TopDocs({
                            {1,    9},
                            {1999, 9},
                            {2001, 9},
                            {3002, 9}
                    });
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).size(), 0);
}

TEST(DocumentsMatcher, two_filled_test_v3) {
    std::vector<TopDocs> td(2);
    td[0] = TopDocs({
                            {12, 9}
                    });
    td[1] = TopDocs({
                            {12,   9},
                            {1999, 9},
                            {2001, 9},
                            {3002, 9}
                    });
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).size(), 1);
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).docs[0].document_id, 12);
}

TEST(DocumentsMatcher, two_filled_test_v4) {
    std::vector<TopDocs> td(2);
    td[0] = TopDocs({
                            {1,  9},
                            {12, 9}
                    });
    td[1] = TopDocs({
                            {12,   9},
                            {1999, 9},
                            {2001, 9},
                            {3002, 9}
                    });
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).size(), 1);
    ASSERT_EQ(DocumentsMatcher::AND_Driver(td).docs[0].document_id, 12);
}


TEST(DocumentsMatcher, can_extend_if_needed) {
    LOOP_ITERS = MultiDocumentsTier::BLOCKSIZE * 100;
    int total_and_size = 0;
    auto suffix = do_index_custom([&](int index, auto _) {
        auto before = generate_words(3);
        if (index % 3 == 0) before += " testword ";
        if (index % 4 == 0) before += " testwordone ";
        if (index % 5 == 0) before += " testwordtwo ";
        if (index % 7 == 0) before += " testwordthree ";

        if (index % (3 * 4 * 5 * 7) == 0) {
            total_and_size++;
        }

        return before + generate_words(3);
    });
    SortedKeysIndexStub index(suffix);

    auto res = index.search_many_terms({"TESTWORD", "TESTWORDONE", "TESTWORDTWO", "TESTWORDTHREE"});

    while (true) {
        auto anded = DocumentsMatcher::AND_Driver(res);
        total_and_size -= anded.size();

        print("One iteration processed, got ", anded.size());

        ASSERT_GE(total_and_size, 0);

        for(auto& d : anded) {
            for(auto& td : res) {
                auto find = std::find(td.docs.begin(), td.docs.end(), d);
                ASSERT_NE(find, td.docs.end());
                td.docs.erase(find);
            }
        }

        if(anded.size() == 0) {
            break;
        }
    }
    ASSERT_EQ(total_and_size, 0);
//    print("AND size: ", anded.size(), "vs", total_and_size);
}