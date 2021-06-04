#ifndef GAME_SERIALIZER_H
#define GAME_SERIALIZER_H

#include <cstdint>
#include "WordIndexEntry.h"
#include "DocIDFilePair.h"
#include "SortedKeysIndexStub.h"
#include <iosfwd>
#include <immintrin.h>

class SortedKeysIndex;


namespace Serializer {

    void serialize_str(std::ostream &stream, const std::string &str);

    uint32_t read_vnum(std::istream &stream);
    std::string read_str(std::istream &stream);

    void serialize(std::ostream &stream, const DocIDFilePair &p);

    DocIDFilePair read_pair(std::istream &stream);

    std::vector<DocIDFilePair> read_filepairs(std::istream &stream);

    void serialize_vnum(std::ostream &stream, uint32_t number, bool pad32 = false);

    void serialize(const std::string& suffix, const SortedKeysIndex &index);

    WordIndexEntry_v2 read_work_index_entry_v2(std::istream &frequencies, std::istream &terms);

    std::vector<StubIndexEntry> read_sorted_keys_index_stub_v2(std::istream &frequencies, std::istream &terms);

    StubIndexEntry read_stub_index_entry_v2(std::istream &frequencies, std::istream &terms);

    void serialize_work_index_entry(std::ostream &frequencies, std::ostream &terms, std::ostream &positions,
                                    const WordIndexEntry &wie);

    WordIndexEntry read_work_index_entry(std::istream &frequencies, std::istream &terms, std::istream &positions);

    PreviewResult preview_work_index_entry(std::istream &terms);
    int read_work_index_entry_v2_optimized(std::istream &frequencies, __m256 *buffer);

    void read_packed_u32_chunk(std::istream &frequencies, int length, uint32_t *buffer);

    void serialize(std::string suffix, const std::vector<DocIDFilePair> &vp);

    void serialize(std::ostream &filemapstream, const std::vector<DocIDFilePair> &vp);
};

namespace Serializer::ffi {

    std::ifstream *create_ifstream_from_path(const char *path);

    void deallocate(std::ifstream *stream);

    void deallocate(std::ofstream *stream);
}


#endif //GAME_SERIALIZER_H
