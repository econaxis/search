#ifndef GAME_SERIALIZER_H
#define GAME_SERIALIZER_H

#include <cstdint>
#include <ostream>
#include "WordIndexEntry.h"
#include "DocIDFilePair.h"


class SortedKeysIndex;

namespace Serializer {

    void serialize_num(std::ostream &stream, uint32_t value);

    void serialize_str(std::ostream &stream, const std::string &str);

    void serialize(std::ostream &stream, const SortedKeysIndex &index);

    void serialize(std::ostream &stream, const WordIndexEntry &ie);

    uint32_t read_num(std::istream &stream);

    std::string read_str(std::istream &stream);

    WordIndexEntry read_work_index_entry(std::istream &stream);

    SortedKeysIndex read_sorted_keys_index(std::istream &stream);

    void serialize(std::ostream &stream, const DocIDFilePair &p);

    void serialize(std::ostream &stream, const std::vector<DocIDFilePair> &vp);

    DocIDFilePair read_pair(std::istream &stream);
    std::vector<DocIDFilePair> read_filepairs(std::istream& stream);

    void read_some_files();
};


#endif //GAME_SERIALIZER_H
