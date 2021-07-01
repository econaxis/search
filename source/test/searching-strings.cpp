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
    auto zzzzz = index.search_many_terms({"ZZZZZ"})[0];
    auto zzzzzz = index.search_many_terms({"ZZZZZZ"})[0];
    auto punctuation = index.search_many_terms({";f4280f.!?"})[0];

    EXPECT_EQ(aaaaa.size(), 0);
    EXPECT_EQ(zzzzz.size(), 0);
    EXPECT_EQ(zzzzzz.size(), 0);
    EXPECT_EQ(punctuation.size(), 0);
}


TEST(Searching, more_precise_searching_test_please) {
    auto good_docs = 0;
    auto generator = [&](int index) -> std::string {
        if (index % 10 == 0) {
            good_docs++;
            return fmt::format("{} {} {}", generate_words(5), "RUDSVF UVNCXK AVNCXRU", generate_words(5));
        } else {
            return generate_words(5);
        }
    };

    auto suffix = do_index_custom(generator);
    SortedKeysIndexStub index(suffix);
    auto temp = index.search_many_terms({"RUDSVF", "UVNCXK", "AVNCXRU"});

    // If we dont' want positions_matching, call DocumentsMatcher::AND_Driver(temp);
    auto topdocs_with_pos = DocumentsMatcher::combiner_with_position(index, temp, {"RUDSVF", "UVNCXK", "AVNCXRU"});

    ASSERT_LT(topdocs_with_pos.docs.size(), MultiDocumentsTier::BLOCKSIZE);
    ASSERT_EQ(topdocs_with_pos.docs.size(), good_docs);
}