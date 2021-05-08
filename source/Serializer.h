#ifndef GAME_SERIALIZER_H
#define GAME_SERIALIZER_H

#include <cstdint>
#include <ostream>
#include "WordIndexEntry.h"
#include "DocIDFilePair.h"
#include "SortedKeysIndexStub.h"



class SortedKeysIndex;

namespace Serializer {

    void serialize_str(std::ostream &stream, const std::string &str);

    void serialize(std::ostream &stream, const SortedKeysIndex &index);

    uint32_t read_vnum(std::istream &stream);

    std::string read_str(std::istream &stream);

    WordIndexEntry read_work_index_entry(std::istream &stream);


    void serialize(std::ostream &stream, const DocIDFilePair &p);

    void serialize(std::ostream &stream, const std::vector<DocIDFilePair> &vp);

    DocIDFilePair read_pair(std::istream &stream);
    std::vector<DocIDFilePair> read_filepairs(std::istream& stream);

    void serialize_vnum(std::ostream &stream, uint32_t number);

    void serialize_consume(std::ostream &positions, std::ostream &frequencies, std::ostream &terms,
                           WordIndexEntry ie);
    void serialize_consume(std::string suffix, SortedKeysIndex index);

    void read_sorted_keys_index_v2(std::istream &stream);

    WordIndexEntry_v2 read_work_index_entry_v2(std::istream &frequencies, std::istream &terms);

    std::vector<StubIndexEntry> read_sorted_keys_index_stub_v2(std::istream &frequencies, std::istream &terms);

    StubIndexEntry read_stub_index_entry_v2(std::istream &frequencies, std::istream &terms);

};


#endif //GAME_SERIALIZER_H
