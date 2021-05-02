#include "Serializer.h"
#include "SortedKeysIndex.h"
#include <memory>
#include "DocIDFilePair.h"


void Serializer::serialize(std::ostream &stream, const DocIDFilePair &p) {
    serialize_num(stream, p.docid);
    serialize_str(stream, p.file_name);
}

void Serializer::serialize(std::ostream &stream, const std::vector<DocIDFilePair> &vp) {
    serialize_num(stream, vp.size());

    for (const auto &i: vp) serialize(stream, i);
}

void Serializer::serialize_str(std::ostream &stream, const std::string &str) {
    auto length = static_cast<uint32_t>(str.length());
    auto *c_str = str.c_str();

    serialize_num(stream, length);
    stream.write(c_str, length);
}

void Serializer::serialize_num(std::ostream &stream, uint32_t value) {
    auto *as_bytes = reinterpret_cast<const char *>(&value);
    stream.write(as_bytes, sizeof(uint32_t));
}

void Serializer::serialize(std::ostream &stream, const SortedKeysIndex &index) {
    serialize_num(stream, index.index.size());
    for (const auto &ie : index.index) {
        serialize(stream, ie);
    }
}


void Serializer::serialize(std::ostream &stream, const WordIndexEntry &ie) {
    /**
     * For each tokenized word (the "key"), we maintain a list of files that has that word.
     * Serialized as follows: key:str, num-of-matching-documents:i32,
     *                        document_id[0]:i32, document_position[0]:i32,
     *                        document_id[1]:i32, document_position[1]:i32, ..., ...
     */

    serialize_str(stream, ie.key); // Serialize the "token" associated with list of matching files.
    serialize_num(stream, ie.files.size());
    for (const auto &fp:ie.files) {
        serialize_num(stream, fp.document_id);
        serialize_num(stream, fp.document_position);
    }
}

uint32_t Serializer::read_num(std::istream &stream) {
//    std::unique_ptr<char[]> buffer(new char[sizeof(uint32_t)]);
    static uint32_t holder;
    stream.read(reinterpret_cast<char *>(&holder), sizeof(uint32_t));
//    auto value = *(reinterpret_cast<uint32_t*>(get()));
    return holder;
}

std::string Serializer::read_str(std::istream &stream) {
//    std::unique_ptr<char[]> buffer(new char[length]);
    constexpr int default_buffer_len = 500;
    static std::string buffer(default_buffer_len, ' ');
    auto length = read_num(stream);

    if (length < default_buffer_len) {
        stream.read(buffer.data(), length);
        return std::string(buffer.data(), length);
    } else {
        // Allocate new string of length size.
        std::string longer_buffer(length, ' ');
        stream.read(longer_buffer.data(), length);
        return std::string(longer_buffer.data(), length);
    }
}

WordIndexEntry Serializer::read_work_index_entry(std::istream &stream) {
    std::string key = read_str(stream);
    auto doc_pointer_len = read_num(stream);
    std::vector<DocumentPositionPointer> docs;
    docs.reserve(doc_pointer_len);
    for (int i = 0; i < doc_pointer_len; i++) {
        docs.emplace_back(read_num(stream), static_cast<uint16_t>(read_num(stream)));
    }
    return WordIndexEntry{std::move(key), std::move(docs)};
}

SortedKeysIndex Serializer::read_sorted_keys_index(std::istream &stream) {
    std::vector<WordIndexEntry> index;

    uint32_t num_word_index_entries = read_num(stream);
    index.reserve(num_word_index_entries);
    for (int i = 0; i < num_word_index_entries; i++) {
        index.push_back(read_work_index_entry(stream));
    }
    return SortedKeysIndex(index);
}

SortedKeysIndex Serializer::read_sorted_keys_index(std::istream &stream, std::streampos start, std::streampos end) {
    std::vector<WordIndexEntry> index;

    uint32_t num_word_index_entries = read_num(stream);
    for (int i = 0; i < num_word_index_entries; i++) {
        index.push_back(read_work_index_entry(stream));
    }
    return SortedKeysIndex(index);
}

std::vector<DocIDFilePair> Serializer::read_filepairs(std::istream &stream) {
    auto sz = read_num(stream);
    std::vector<DocIDFilePair> out;

    while (sz--) {
        out.push_back(read_pair(stream));
    }
    return out;
}


DocIDFilePair Serializer::read_pair(std::istream &stream) {
    return {read_num(stream), read_str(stream)};
}