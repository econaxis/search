#include "all_includes.h"
#include <gtest/gtest.h>






TEST(indexing, indexing) {
    std::stringstream fakecin;
    std::cin.rdbuf(fakecin.rdbuf());
    SyncedQueue queue;

    constexpr int iters = 1000;

    std::array<std::string, iters> filenames, files;
    repeat(iters, [&](int i) {
        filenames[i] = random_b64_str(10);
        files[i] = random_b64_str(100);
        fmt::print(fakecin, "filename {} /endfilename file {} /endfile ", filenames[i], files[i]);
    });

    fmt::print(fakecin, "/endindexing\n");


    std::thread thread([&]() { queue_produce_file_contents_stdin(queue); });

    thread.join();

    int curiter = 0;
    while (!queue.done_flag || queue.size()) {
        ASSERT_EQ(files[curiter++], queue.pop().first);
    }




    SUCCEED() << "Done indexing";
}



TEST(indexing, indexes_ok) {
    LOOP_ITERS = 50;
    do_index();
}

TEST(indexing, indexes_correctly_and_deserialize_correctly) {
    LOOP_ITERS = 500;
    auto suffix = do_index("fddsvc fewivx vncms");
    SortedKeysIndexStub index(suffix);

    // At least one of these terms should match
    auto res = index.search_many_terms({"FDDSVC", "FEWIVX", "VNCMS"});

    EXPECT_EQ(res.size(), 3);
    EXPECT_GT(res[0].size(), 0);
    EXPECT_GT(res[1].size(), 0);
    EXPECT_GT(res[2].size(), 0);

    auto AND = DocumentsMatcher::AND_Driver(res);

    EXPECT_GT(AND.size(), 0);

    auto res_bad = index.search_many_terms({"SHOULDNOTHAVE~", "SHOULDNOTHAVEAGAIN"});
    EXPECT_EQ(res_bad[0].size(), 0);
    EXPECT_EQ(res_bad[1].size(), 0);

    AND = DocumentsMatcher::AND_Driver(res_bad);
    EXPECT_EQ(AND.size(), 0);
}
