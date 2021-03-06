#include <filesystem>
#include "compactor/Compactor.h"
#include <fstream>
#include <iostream>
#include <cassert>
#include "SortedKeysIndex.h"
#include "Serializer.h"
#include "Constants.h"
#include "IndexFileLocker.h"

namespace fs = std::filesystem;


fs::path make_path(const std::string &name, const std::string &suffix) {
    return indice_files_dir / (name + "-" + suffix);
}


template<typename stream_t>
struct StreamSet {
    stream_t frequencies;
    stream_t terms;
    stream_t positions;
    std::string suffix;
    std::unique_ptr<char[]> buffer;

    WordIndexEntry read() {
        return Serializer::read_work_index_entry(frequencies, terms, positions);
    }

    void serialize(const WordIndexEntry &wie) {
        Serializer::serialize_work_index_entry(frequencies, terms, positions, wie);
    }

    bool valid() {
        return frequencies.good() && terms.good() && positions.good();
    }

    int getlen() {
        std::vector<int> lengths;
        apply_to_all([&](auto &stream) {
            stream.seekg(0);
            lengths.push_back(Serializer::read_vnum(stream));
        });
        assert(std::equal(lengths.begin(), lengths.end(), lengths.begin()));
        return lengths[0];
    }

    void remove_all() {
        fs::remove(make_path("frequencies", suffix));
        fs::remove(make_path("terms", suffix));
        fs::remove(make_path("positions", suffix));
    }

    template<typename Lambda>
    void apply_to_all(Lambda l) {
        l(frequencies);
        l(terms);
        l(positions);
    }
};

static constexpr std::size_t BUFLEN = 5e5;

template<typename T>
StreamSet<T> open_file_set(const std::string &suffix, bool create = false) {
    auto inoutbinary = std::ios_base::binary | std::ios_base::out;
    if (!create) inoutbinary |= std::ios_base::in;
    StreamSet<T> set{
            .frequencies = T(make_path("frequencies", suffix), inoutbinary),
            .terms = T(make_path("terms", suffix), inoutbinary),
            .positions = T(make_path("positions", suffix), inoutbinary),
            .suffix = suffix,
            .buffer = std::make_unique<char[]>(BUFLEN * 2)
    };

    set.positions.rdbuf()->pubsetbuf(set.buffer.get() + BUFLEN * 0, BUFLEN);
    set.frequencies.rdbuf()->pubsetbuf(set.buffer.get() + BUFLEN * 1, BUFLEN);

    if (!(set.frequencies.good() && set.terms.good() && set.positions.good())) {
        throw std::runtime_error("File cannot be opened " + suffix);
    }
    if (!create) {
        [[maybe_unused]] int len = Serializer::read_vnum(set.frequencies);
        assert(Serializer::read_vnum(set.terms) == len && Serializer::read_vnum(set.positions) == len);
    }
    return set;
}

// Tilde is greater ascii character than all other alphabetical characters.
const std::string INVALIDATED = "~~~INVALIDATED";

using namespace Serializer;

bool Compactor::compact_two_files(std::string &suffix, std::string &suffix1, std::string& joined_suffix) {
    auto streamset = open_file_set<std::fstream>(suffix);
    auto streamset1 = open_file_set<std::fstream>(suffix1);

    if (!(streamset.valid() && streamset1.valid())) {
        std::cerr<<"One is not valid\n";
        return false;
    }

    /* newsize: a counter for the size of the merged index
     * len: size of first index
     * len1: size of second index. */
    uint32_t newsize = 0;
    uint32_t len = streamset.getlen();
    uint32_t len1 = streamset1.getlen();

    auto temp_suffix = joined_suffix + "temp";
    auto ostreamset = open_file_set<std::fstream>(temp_suffix, true);

    // Temporarily pad the beginning of the file with the number of elements
    // This would change as we're merging elements. Don't know the length until after merge.
    ostreamset.apply_to_all([](auto &stream) {
        stream.seekg(0);
        serialize_vnum(stream, 1, true);
    });
    WordIndexEntry wie{INVALIDATED, {}}, wie1{INVALIDATED, {}};
    // All streams are at position 4 bytes from beginning, where data starts.
    while (true) {
        if (len1 % 100 == 0) std::cout << "Remaining: " << len << " " << len1 << "\r";
        if (wie.key == INVALIDATED && len) {
            // Need to refill this key.
            wie = streamset.read();
            len--;
        }
        if (wie1.key == INVALIDATED && len1) {
            // Refill this key.
            wie1 = streamset1.read();
            len1--;
        }
        if (wie1.key == INVALIDATED && wie.key == INVALIDATED && !len && !len1) {
            // Exit loop
            break;
        } else if (wie1.key == INVALIDATED && wie.key == INVALIDATED) {
            throw std::runtime_error("Impossible state");
        }
        // Should put wie1 first.
        if (wie.key.compare(wie1.key) > 0) {
            // wie1 goes first, because its lower in the alphabet.
            newsize++;
            ostreamset.serialize(wie1);
            wie1.key = INVALIDATED;

        } // Now should put wie first
        else if (wie.key.compare(wie1.key) < 0) {
            newsize++;
            ostreamset.serialize(wie);
            wie.key = INVALIDATED;

        } else if (wie.key == wie1.key) {
            // Merge these two and serialize them.
            assert(wie.key != INVALIDATED);

            newsize++;

            wie.merge_into(wie1);
            ostreamset.serialize(wie);
            wie.key = INVALIDATED;
            wie1.key = INVALIDATED;
        } else {
            throw std::runtime_error("Wie key comparison impossible: " + wie.key + wie1.key);
        }
    }
    // Write the actual size of the new index to the beginning position.
    ostreamset.apply_to_all([=](auto &stream) {
        stream.seekg(std::ios_base::beg);
        serialize_vnum(stream, newsize, true);
    });

    streamset.remove_all();
    streamset1.remove_all();

    fs::rename(make_path("frequencies", temp_suffix), make_path("frequencies", joined_suffix));
    fs::rename(make_path("terms", temp_suffix), make_path("terms", joined_suffix));
    fs::rename(make_path("positions", temp_suffix), make_path("positions", joined_suffix));

    return true;
}

extern "C" void compact_two_files(const char* a, const char* b, const char* out) {
    std::string as (a);
    std::string bs (b);
    std::string outs (out);
    Compactor::compact_two_files(as, bs, outs);
}
//// todo: copy-on-write mechanism
//std::optional<std::string> Compactor::compact_two_files() {
//    using namespace Serializer;
//    std::fstream index_file(indice_files_dir / "index_files", std::ios_base::in | std::ios_base::out);
//    assert(index_file);
//
//    IndexFileLocker::acquire_lock_file();
//    auto[err_state1, suffix] = read_and_mark_line(index_file);
//    auto[err_state2, suffix1] = read_and_mark_line(index_file);
//    IndexFileLocker::release_lock_file();
//
//
//    assert(err_state2 == ReadState::GOOD && err_state1 == err_state2);
//
//    auto streamset = open_file_set<std::fstream>(suffix);
//    auto streamset1 = open_file_set<std::fstream>(suffix1);
//
//
//    if (check_file(streamset) && check_file(streamset1)) return compact_two_files(suffix, suffix1);
//    else {
//        // Re-add it back to index files.
//        index_file.seekg(0, std::ios_base::end);
//        index_file << suffix << "\n";
//        index_file << suffix1 << "\n";
//        return "CONTINUE";
//    }
//
//}


// Deserializes and then serializes an index (comprised of terms, frequencies, positions, and filemap) to check for consistency.
// If the index is serialized correctly (e.g. program was not left in a bad state/sudden SIGTERM), then the MD5 sums should match.
//void Compactor::test_makes_sense(const std::string &suffix) {
//    using namespace Serializer;
//
//    auto streamset = open_file_set<std::ifstream>(suffix);
//    int len = streamset.getlen();
//
//    auto filepairs = Serializer::read_filepairs(streamset.filemap);
//
//    assert(!filepairs.empty());
//
//
//    assert(streamset.frequencies && streamset.terms && streamset.positions);
//
//    WordIndexEntry wie;
//
//    while (check_stream_good(dynamic_cast<std::ifstream &>(streamset.terms)) && len > 0) {
//        len--;
//        wie = read_work_index_entry(streamset.frequencies, streamset.terms, streamset.positions);
//
//        for(auto i = wie.files.begin(); i < wie.files.end() - 1; i++) {
//            if(*i > *(i+1)) {
//                throw std::runtime_error("Bad! unsorted");
//            }
//        }
//    }
//    assert(len == 0);
//}
