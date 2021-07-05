#include <bits/stdc++.h>
#include "all_includes.h"
#include <gtest/gtest.h>

using namespace std;

class Environment : public ::testing::Environment {
public:
    ~Environment() override {}

    // Override this to define how to set up the environment.
    void SetUp() override {
        data_files_dir = filesystem::temp_directory_path() / "gtest-search-dir/";
        indice_files_dir = data_files_dir / std::string("indices");
        fs::create_directories(data_files_dir);
        fs::create_directories(indice_files_dir);
    }

    // Override this to define how to tear down the environment.
    void TearDown() override {
        fs::remove_all(data_files_dir);
        FileListGenerator::delete_names_db();
    }
};

testing::Environment *const foo_env =
        testing::AddGlobalTestEnvironment(new Environment);


WordIndexEntry gen_random_wie() {
    std::vector<DocumentPositionPointer> a{};
    int num = 1000;
    const uint maxint = 1 << 31;
    while (num--) {
        a.emplace_back(utils::rand() % (100) + (1 << 25), utils::rand() % maxint);
    }
    std::sort(a.begin(), a.end());

    return WordIndexEntry{
            random_b64_str(10), a
    };
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

