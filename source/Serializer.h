#ifndef GAME_SERIALIZER_H
#define GAME_SERIALIZER_H

#include <cstdint>
#include <ostream>
#include "WordIndexEntry.h"
#include "DocIDFilePair.h"
#include "SortedKeysIndexStub.h"
#include <filesystem>
#include <memory_resource>


class SortedKeysIndex;

namespace Serializer {

    void serialize_str(std::ostream &stream, const std::string &str);

    uint32_t read_vnum(std::istream &stream);

    std::string read_str(std::istream &stream);

    WordIndexEntry read_work_index_entry(std::istream &stream);


    void serialize(std::ostream &stream, const DocIDFilePair &p);

    void serialize(std::ostream &stream, const std::vector<DocIDFilePair> &vp);

    DocIDFilePair read_pair(std::istream &stream);

    std::vector<DocIDFilePair> read_filepairs(std::istream &stream);

    void serialize_vnum(std::ostream &stream, uint32_t number, bool pad32);

    void serialize(const std::string& suffix,  SortedKeysIndex &index);

    WordIndexEntry_v2 read_work_index_entry_v2(std::istream &frequencies, std::istream &terms);

    std::vector<StubIndexEntry> read_sorted_keys_index_stub_v2(std::istream &frequencies, std::istream &terms);

    StubIndexEntry read_stub_index_entry_v2(std::istream &frequencies, std::istream &terms);

    void serialize_work_index_entry(std::ostream &frequencies, std::ostream &terms, std::ostream &positions,
                                    const WordIndexEntry &wie);

    WordIndexEntry read_work_index_entry(std::istream &frequencies, std::istream &terms, std::istream &positions);
};

namespace Serializer::ffi {
    namespace fs = std::filesystem;

    std::ifstream *create_ifstream_from_path(const char *path);

    void deallocate(std::ifstream *stream);

    void deallocate(std::ofstream *stream);
}


#endif //GAME_SERIALIZER_H
