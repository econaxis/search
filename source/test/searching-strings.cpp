#include "all_includes.h"
#include <gtest/gtest.h>


TEST(Searching, bad_strings) {
    auto suffix = do_index("aaaaaa AAAAAAAAAAAAAAAA ZZZZZZ ZZZZZ fndks 48dknl; fdsa;085 fjbnv c     fds\\fd    ");
    SortedKeysIndexStub index(suffix);

    auto aaaaa = index.search_many_terms({"AAAAAAAAAAAAAAAA"})[0];
    auto fndks = index.search_many_terms({"FNDKS"})[0];
    auto zzzzz = index.search_many_terms({"ZZZZZ"})[0];
    auto zzzzzz = index.search_many_terms({"ZZZZZZ"})[0];
    auto punctuation = index.search_many_terms({";f4280f.!?"})[0];

    EXPECT_GT(aaaaa.size(), 1);
    EXPECT_GT(fndks.size(), 1);
    EXPECT_GT(zzzzz.size(), 1);
    EXPECT_GT(zzzzzz.size(), 1);
    EXPECT_EQ(punctuation.size(), 0);

    EXPECT_EQ(aaaaa.size(), fndks.size());
    EXPECT_EQ(aaaaa.size(), zzzzzz.size());
    EXPECT_EQ(aaaaa.size(), zzzzz.size());
}

TEST(Searching, should_not_contain) {
    auto suffix = do_index();
    SortedKeysIndexStub index(suffix);

    auto aaaaa = index.search_many_terms({"AAAAAAAAAAAAAAAA"})[0];
    auto zzzzz = index.search_many_terms({"ZZZZZZZZZZZZZZ~"})[0];
    auto zzzzzz = index.search_many_terms({"ZZZZZZZZ~"})[0];
    auto punctuation = index.search_many_terms({";f4280f.!?"})[0];

    EXPECT_EQ(aaaaa.size(), 0);
    EXPECT_EQ(zzzzz.size(), 0);
    EXPECT_EQ(zzzzzz.size(), 0);
    EXPECT_EQ(punctuation.size(), 0);
}


// tests that documents with terms closer together should rank higher.
TEST(Searching, more_precise_searching_test) {
    std::vector<int> good_docs;
    auto generator = [&](int index) -> std::string {
        if (utils::rand() % (LOOP_ITERS / 10 + 2) == 0) {
            good_docs.push_back(index);
            return fmt::format("{} {} {}", generate_words(100), "RUDSVF UVNCXK AVNCXRU", generate_words(100));
        } else {
            std::ostringstream random_words;
            std::vector<std::string_view> must_include {"RUDSVF", "UVNCXK", "AVNCXRU"};
            int counter = 0;
            while(!must_include.empty()) {
                if(counter++ % 20 == 0) {
                    random_words<<must_include.back()<<" ";
                    must_include.pop_back();
                } else {
                    random_words<<generate_words(2);
                }
            }
            return random_words.str();
        }
    };

    auto suffix = do_index_custom(generator);
    SortedKeysIndexStub index(suffix);
    auto temp = index.search_many_terms({"RUDSVF", "UVNCXK", "AVNCXRU"});

    // Expand all to avoid the chunking optimization
    for(auto& td : temp) td.extend_from_tier_iterator(std::numeric_limits<int>::max());

    auto topdocs_with_pos = DocumentsMatcher::combiner_with_position(index, temp, {"RUDSVF", "UVNCXK", "AVNCXRU"});

    std::reverse(topdocs_with_pos.begin(), topdocs_with_pos.end());
    std::sort(topdocs_with_pos.begin(), topdocs_with_pos.begin() + good_docs.size(), [](const auto& a, const auto& b) {
        return a.document_id < b.document_id;
    });
    std::sort(good_docs.begin(), good_docs.end());
    ASSERT_TRUE(std::equal(good_docs.begin(), good_docs.end(), topdocs_with_pos.begin(), [](int i, DocumentsMatcher::TopDocsWithPositions::Elem& j) {
        return i == j.document_id;
    }));
}