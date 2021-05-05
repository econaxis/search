#include "Serializer.h"
#include "SortedKeysIndex.h"
#include <memory>
#include <iostream>
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
    uint32_t holder;
    stream.read(reinterpret_cast<char *>(&holder), sizeof(uint32_t));
    return holder;
}

std::string Serializer::read_str(std::istream &stream) {
//    std::unique_ptr<char[]> buffer(new char[length]);
    auto length = read_num(stream);
    // Allocate new string of length size.
    std::string buffer(length, ' ');
    stream.read(buffer.data(), length);
    return buffer;
}


WordIndexEntry Serializer::read_work_index_entry(std::istream &stream) {
    std::string key = read_str(stream);
    auto doc_pointer_len = read_num(stream);
    std::vector<DocumentPositionPointer> docs;
    docs.resize(doc_pointer_len, DocumentPositionPointer(0, 0));

    // Very unsafe, but very fast.
    // Since the memory layout of std::vector and the file is contiguous, we can just read it to memory like this,
    // assuming consistent uint32 layout. It reads two 32 bit integers per doc_pointer_len directly into the vector buffer.
    stream.read(reinterpret_cast<char *>(docs.data()), doc_pointer_len * sizeof(DocumentPositionPointer));

    // Previous variant:
//    for (int i = 0; i < doc_pointer_len; i++) {
//        auto document_id = read_num(stream);
//        auto document_position = read_num(stream);
//        docs.emplace_back(document_id, document_position);
//    }
    return WordIndexEntry{std::move(key), std::move(docs)};
}

SortedKeysIndex Serializer::read_sorted_keys_index(std::istream &stream) {
    std::vector<WordIndexEntry> index;

    uint32_t num_word_index_entries = read_num(stream);
    index.reserve(num_word_index_entries);
    for (int i = 0; i < num_word_index_entries; i++) {
        index.push_back(read_work_index_entry(stream));

        if (i % 100 == 0) std::cout << int(100.F * i / num_word_index_entries) << "%\r" << std::flush;
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