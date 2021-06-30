#include <bits/stdc++.h>
#include "all_includes.h"
#include <gtest/gtest.h>

using namespace std;

class Environment : public ::testing::Environment {
public:
    ~Environment() override {}

    // Override this to define how to set up the environment.
    void SetUp() override {
        data_files_dir = "/tmp/gtest-search-dir/";
        indice_files_dir = data_files_dir / std::string("indices");
        fs::create_directories(data_files_dir);
        fs::create_directories(indice_files_dir);
    }

    // Override this to define how to tear down the environment.
    void TearDown() override {
        fs::remove_all(data_files_dir);
    }
};

testing::Environment *const foo_env =
        testing::AddGlobalTestEnvironment(new Environment);


namespace {
    [[maybe_unused]] unsigned long rand() {
        static std::random_device dev;
        static std::mt19937 rng(dev());
        static std::uniform_int_distribution<std::mt19937::result_type> dist6; // distribution in range [1, 6]

        return dist6(rng);
    }
}
void repeat(int num, auto call) {
    for (int i = 0; i < num; i++) {
        call(i);
    }
}
std::string generate_words(int num = 100) {
    auto totsize = std::size(strings);
    std::ostringstream res;

    repeat(num, [&](int _) {
        res << strings[::rand() % totsize] << " ";
    });

    return res.str();
}

WordIndexEntry gen_random_wie() {
    std::vector<DocumentPositionPointer> a{};
    int num = 1000;
    const uint maxint = 1 << 31;
    while (num--) {
        a.emplace_back(::rand() % (100) + (1 << 25), ::rand() % maxint);
    }
    std::sort(a.begin(), a.end());

    return WordIndexEntry{
            random_b64_str(10), a
    };
}


std::string do_index(std::string must_include = "empty") {
    std::stringstream fakecin;
    std::cin.rdbuf(fakecin.rdbuf());
    constexpr int iters = 1000;
    std::array<std::string, iters> filenames, files;
    repeat(iters, [&](int i) {
        filenames[i] = random_b64_str(10);
        files[i] = generate_words(100);
        files[i].append(" " + must_include);
        fmt::print(fakecin, "filename {} /endfilename file {} /endfile ", filenames[i], files[i]);
    });

    fmt::print(fakecin, "/endindexing\n");

    auto suffix = GeneralIndexer::read_some_files(queue_produce_file_contents_stdin);
    return *suffix;
}



TEST(SerializationWordIndexEntry, can_serialize_positions_for_one_wie) {
    // Push random numbers onto "a"
    auto wie = gen_random_wie();

    std::stringstream positions, frequencies;
    PositionsSearcher::serialize_positions(positions, wie);
    MultiDocumentsTier::serialize(wie, frequencies);

    MultiDocumentsTier::TierIterator ti(frequencies);
    auto sd = ti.read_all();
    auto test = PositionsSearcher::read_positions_all(positions, sd);

    ASSERT_EQ(test, wie.files);
}

TEST(indexing, indexes_correctly_and_deserialize_correctly) {
    auto suffix = do_index("fddsvc fewivx vncms");
    SortedKeysIndexStub index(suffix);

    // At least one of these terms should match
    auto res = index.search_many_terms({"FDDSVC", "FEWIVX", "VNCMS"});

    EXPECT_EQ(res.size(), 3);
    EXPECT_GT(res[0].size(), 0);
    EXPECT_GT(res[1].size(), 0);
    EXPECT_GT(res[2].size(), 0);
}


std::string serialize_test(std::string suffix) {
    std::vector<WordIndexEntry> wies{};
    for (int i = 0; i < 1000; i++) wies.push_back(gen_random_wie());

    auto ssk = SortedKeysIndex(wies);
    ssk.sort_and_group_shallow();
    ssk.sort_and_group_all();

    EXPECT_TRUE(is_sorted(ssk.get_index().begin(), ssk.get_index().end()));
    for (auto &j : ssk.get_index()) {
        EXPECT_TRUE(is_sorted(j.files.begin(), j.files.end()));
    }

    Serializer::serialize(suffix, ssk);
    std::vector<DocIDFilePair> blank_fp{{1, "test"}};
    Serializer::serialize(suffix, blank_fp);
    return suffix;
}

TEST(SerializationWordIndexEntry, can_serialize_for_many_wies) {
    ASSERT_EQ(serialize_test("TEST"), "TEST") << "Serialization succeeded";
}

TEST(SerializationWordIndexEntry, can_serialize_and_load_wies) {
    serialize_test("TEST-serialize-and-load");
    Compactor::test_makes_sense("TEST-serialize-and-load");
}


TEST(FilePairs, filepairs_test) {
    vector<DocIDFilePair> fp;
    int i = 0;
    for (; i < 100000; i++) fp.push_back({static_cast<uint32_t>(i + 1), random_b64_str(50)});

    Serializer::serialize("TEST-filepairs", fp);
    SUCCEED() << "Serialized filepairs";

    FPStub fpstub(indice_files_dir / "filemap-TEST-filepairs");

    for (auto&[id, filename] : fp) {
        ASSERT_EQ(fpstub.query(id), filename) << "ID is: " << id;
    }
}



void toggle_cout() {
    static std::ostringstream fakecout;
    static std::optional<streambuf *> coutbuf = fakecout.rdbuf();
    coutbuf = std::cout.rdbuf(coutbuf.value());
}

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
    do_index();
}

