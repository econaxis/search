#include <filesystem>
#include "IndexFileLocker.h"
#include "compactor/Compactor.h"
#include <fstream>
#include <iostream>
#include <cassert>
#include "SortedKeysIndex.h"
#include "Serializer.h"
#include "Constants.h"
#include "IndexFileLocker.h"
#include "random_b64_gen.h"

namespace fs = std::filesystem;

void insert_string(std::fstream &stream, const std::string &ins) {
    static std::unique_ptr<char[]> temp_buf = std::make_unique<char[]>(100000);
    if (stream.read(temp_buf.get(), 100000).good()) {
        throw std::runtime_error("Buffer too small");
    }
    auto num_write = stream.gcount();
    stream.clear();
    stream.seekp(-num_write, std::ios_base::end);
    stream.sync();
    stream << ins;
    auto pos = stream.tellg();
    stream.write(temp_buf.get(), num_write);
    stream.seekg(pos);
}


std::pair<Compactor::ReadState, std::string> Compactor::read_and_mark_line(std::fstream &stream) {
    std::string line;
    auto before_read = stream.tellg();

    if (!std::getline(stream, line) || stream.eof()) return {Compactor::ReadState::STREAM_ERROR, ""};

    if (line[0] == '#') {
        return read_and_mark_line(stream); //recursive call.
    } else {
        stream.seekg(before_read);
        insert_string(stream, "# joined ");
        //         Use up the remaining line
        std::getline(stream, line);
        return {Compactor::ReadState::GOOD, line};
    }
}

fs::path make_path(const std::string &name, const std::string &suffix) {
    return indice_files_dir / (name + "-" + suffix);
}


template<typename stream_t>
struct StreamSet {
    stream_t frequencies;
    stream_t terms;
    stream_t positions;
    stream_t filemap;
    std::string suffix;
    std::unique_ptr<char[]> buffer;

    WordIndexEntry read() {
        return Serializer::read_work_index_entry(frequencies, terms, positions);
    }

    void serialize(const WordIndexEntry &wie) {
        Serializer::serialize_work_index_entry(frequencies, terms, positions, wie);
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
        fs::remove(make_path("filemap", suffix));
    }

    template<typename Lambda>
    void apply_to_all(Lambda l) {
        l(frequencies);
        l(terms);
        l(positions);
        // Filemap not applied because it's special.
    }
};

static constexpr std::size_t BUFLEN = 5e6;

template<typename T>
StreamSet<T> open_file_set(const std::string &suffix, bool create = false) {
    auto inoutbinary = std::ios_base::binary | std::ios_base::out;
    if (!create) inoutbinary |= std::ios_base::in;
    StreamSet<T> set{
            .frequencies = T(make_path("frequencies", suffix), inoutbinary),
            .terms = T(make_path("terms", suffix), inoutbinary),
            .positions = T(make_path("positions", suffix), inoutbinary),
            .filemap = T(make_path("filemap", suffix), inoutbinary),
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

bool check_stream_good(std::ifstream &stream) {
    stream.get();
    if (!stream.good()) {
        stream.clear();
        stream.unget();
        return false;
    } else {
        stream.unget();
        return true;
    }
}

std::vector<DocIDFilePair> merge_filepairs(std::vector<DocIDFilePair> &one, std::vector<DocIDFilePair> &two) {
    std::vector<DocIDFilePair> merged;
    std::merge(one.begin(), one.end(), two.begin(), two.end(), std::back_inserter(merged));
    return merged;
}

bool check_file(StreamSet<std::fstream> &str) {
    return fs::file_size(make_path("positions", str.suffix)) < 1e10;
}

using namespace Serializer;

std::optional<std::string> Compactor::compact_two_files(std::string &suffix, std::string &suffix1) {
    std::fstream index_file(indice_files_dir / "index_files", std::ios_base::in | std::ios_base::out);


    auto streamset = open_file_set<std::fstream>(suffix);
    auto streamset1 = open_file_set<std::fstream>(suffix1);


    auto joined_suffix = suffix + "-" + suffix1;

    if (joined_suffix.size() > 20) joined_suffix = random_b64_str(5);
    auto temp_joined_suffix = "TEMP-" + joined_suffix;

    auto filepairs = Serializer::read_filepairs(streamset.filemap);
    auto filepairs1 = Serializer::read_filepairs(streamset1.filemap);

    std::cout << suffix << "-" << suffix1 << "\n";
    std::cout << "Greatest id: " << filepairs.back().document_id << " " << filepairs1.back().document_id << "\n";

    const auto diff1 = filepairs.back().document_id + 1;
    auto upgrade_ids = [&](auto &iterable) {

        // Upgrade all documents from 1 to avoid ID duplication
        // All ID's in database 1 will increase by diff1
        for (auto &i : iterable) {
            i.document_id += diff1;
        }
    };

    upgrade_ids(filepairs1);
    auto merged_filepair = merge_filepairs(filepairs, filepairs1);


    /* newsize: a counter for the size of the merged index
     * len: size of first index
     * len1: size of second index. */
    uint32_t newsize = 0;
    uint32_t len = streamset.getlen();
    uint32_t len1 = streamset1.getlen();


    auto ostreamset = open_file_set<std::fstream>(temp_joined_suffix, true);
    serialize(ostreamset.filemap, merged_filepair);

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
            upgrade_ids(wie1.files);

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

    // Move all to valid location
    IndexFileLocker::move_all(temp_joined_suffix, joined_suffix);

    IndexFileLocker::do_lambda([&] {
        std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
        index_file << joined_suffix << "\n";
    });
    return joined_suffix;
}


// todo: copy-on-write mechanism
std::optional<std::string> Compactor::compact_two_files() {
    using namespace Serializer;
    std::fstream index_file(indice_files_dir / "index_files", std::ios_base::in | std::ios_base::out);
    assert(index_file);

    IndexFileLocker::acquire_lock_file();
    auto[err_state1, suffix] = read_and_mark_line(index_file);
    auto[err_state2, suffix1] = read_and_mark_line(index_file);
    IndexFileLocker::release_lock_file();


    assert(err_state2 == ReadState::GOOD && err_state1 == err_state2);

    auto streamset = open_file_set<std::fstream>(suffix);
    auto streamset1 = open_file_set<std::fstream>(suffix1);

    if (!check_file(streamset) || !check_file(streamset1)) {
        // Re-add it back to index files.
        index_file.seekg(0, std::ios_base::end);
        index_file << suffix << "\n";
        index_file << suffix1 << "\n";
        return "CONTINUE";
    }

    return compact_two_files(suffix, suffix1);
}


// Deserializes and then serializes an index (comprised of terms, frequencies, positions, and filemap) to check for consistency.
// If the index is serialized correctly (e.g. program was not left in a bad state/sudden SIGTERM), then the MD5 sums should match.
void Compactor::test_makes_sense(const std::string &suffix) {
    using namespace Serializer;

    auto streamset = open_file_set<std::ifstream>(suffix);
    int len = streamset.getlen();

    auto filepairs = Serializer::read_filepairs(streamset.filemap);

    assert(!filepairs.empty());


    assert(streamset.frequencies && streamset.terms && streamset.positions);

    auto ostreamset = open_file_set<std::fstream>(suffix + "-COPY_DEBUG", true);
    ostreamset.apply_to_all([=](auto &stream) {
        serialize_vnum(stream, len, true);
    });

    WordIndexEntry wie;

    if (len < 5 || filepairs.size() < 5) {
        std::cout << "Too short, failed compactation detected\n";

        fs::remove(make_path("frequencies", suffix));
        fs::remove(make_path("terms", suffix));
        fs::remove(make_path("positions", suffix));
        fs::remove(make_path("filemap", suffix));
        return;
    }
    while (check_stream_good(dynamic_cast<std::ifstream &>(streamset.terms)) && len > 0) {
        len--;

        wie = read_work_index_entry(streamset.frequencies, streamset.terms, streamset.positions);
        assert(std::is_sorted(wie.files.begin(), wie.files.end()));
//        serialize_work_index_entry(ofrequencies, oterms, opositions, wie);
    }
    assert(len == 0);
}

std::pair<Compactor::ReadState, std::string> Compactor::read_line(std::ifstream &stream) {
    std::string line;

    if (!std::getline(stream, line)) return {Compactor::ReadState::STREAM_ERROR, ""};

    if (line[0] == '#') {
        return read_line(stream); //recursive call.
    } else {
        return {Compactor::ReadState::GOOD, line};
    }


}

