#include "Serializer.h"
#include "Constants.h"
#include "SortedKeysIndex.h"
#include <memory>
#include <iostream>
#include <fstream>
#include "DocIDFilePair.h"
#include "SortedKeysIndexStub.h"


void Serializer::serialize(std::ostream &stream, const DocIDFilePair &p) {
    serialize_vnum(stream, p.docid);
    serialize_str(stream, p.file_name);
}

void Serializer::serialize(std::ostream &stream, const std::vector<DocIDFilePair> &vp) {
    serialize_vnum(stream, vp.size());

    for (const auto &i: vp) serialize(stream, i);
}

void Serializer::serialize_str(std::ostream &stream, const std::string &str) {
    auto length = str.length();
    auto *c_str = str.c_str();

    serialize_vnum(stream, length);
    stream.write(c_str, length);
}


void Serializer::serialize_vnum(std::ostream &stream, uint32_t number) {
// Position of first set bit in the first part determines how many bytes long the number is.
    constexpr uint8_t uint8max = (1 << 7) - 1;        // 1...          one byte
    constexpr uint16_t uint16max = (1 << 14) - 1;         // 01...         two bytes
    constexpr uint32_t uint32max = (1 << 28) - 1;         // 0001...       four bytes
    constexpr uint64_t uint64max = (1ULL << 57) - 1;    // 00000001...   eight bytes

    auto write_num = [&](auto num) {
        stream.write(static_cast<const char *>((void *) (&num)), sizeof(num));
    };
    if (number <= uint8max) {
        number = number << 1;
        number |= 1 << 0;
        write_num(static_cast<uint8_t>(number));
    } else if (number <= uint16max) {
        number = number << 2;
        number |= 1 << 1;
        write_num(static_cast<uint16_t>(number));
    } else if (number <= uint32max) {
        number = number << 4;
        number |= 1 << 3;
        write_num(static_cast<uint32_t>(number));
    } else if (false && number <= uint64max) { // not supported
        number &= (1ULL << 58) - 1;
        write_num(static_cast<uint64_t>(number));
    } else {
        throw std::runtime_error("Number too big");
    }
}


uint32_t Serializer::read_vnum(std::istream &stream) {
    uint32_t holder = 0;
    uint8_t byte = 0;
    stream.read(reinterpret_cast<char *> (&byte), 1);

    if (byte & 1 << 0) {
        // 8 bit number
        holder = byte >> 1; // byte has 7 bits of info.
    } else if (byte & 1 << 1) {
        stream.read(reinterpret_cast<char *> (&holder), 1);
        // 16 bit number
        byte = byte >> 2; // byte has 6 bits of info
        holder = (holder << 6) | byte;
    } else if (byte & 1 << 3) {
        stream.read(reinterpret_cast<char *> (&holder), 3);
        byte = byte >> 4; // byte has 4 bits of info.
        // 32 bit number
        holder = (holder << 4) | byte;
    }

    return holder;
}

/**
 * Serializes word index entry as a vector of VInt (docid), VInt (num occurences)
 */
void Serializer::serialize_consume(std::ostream &positions, std::ostream &frequencies, std::ostream &terms,
                                   WordIndexEntry ie) {
    uint32_t term_pos = terms.tellp();
    serialize_str(terms, ie.key);

    serialize_vnum(frequencies, term_pos);
    serialize_vnum(positions, term_pos);

    std::sort(ie.files.begin(), ie.files.end());
    std::vector<std::pair<uint32_t, uint32_t>> freq_data;
    int prev_same_idx = 0;

    serialize_vnum(positions, ie.files.size());
    for (int i = 0; i <= ie.files.size(); i++) {
        if (i == ie.files.size()) {
            freq_data.emplace_back(ie.files[i - 1].document_id, i - prev_same_idx);
            break;
        }
        if (ie.files[i].document_id != ie.files[prev_same_idx].document_id) {
            // We reached a different index.
            auto num_occurences_in_term = i - prev_same_idx;
            auto docid = ie.files[i].document_id;
            freq_data.emplace_back(docid, num_occurences_in_term);
            prev_same_idx = i;
        }
        serialize_vnum(positions, ie.files[i].document_position);
    }
    serialize_vnum(frequencies, freq_data.size());
    for (auto&[a, b] : freq_data) {
        serialize_vnum(frequencies, a);
        serialize_vnum(frequencies, b);
    }
}

WordIndexEntry_v2 Serializer::read_work_index_entry_v2(std::istream &frequencies, std::istream &terms) {
    uint32_t term_pos = read_vnum(frequencies);
    int num_files = read_vnum(frequencies);

    terms.seekg(term_pos);

    auto key = read_str(terms);

    for (char c : key) assert(c < 91 && c > 64);

    WordIndexEntry_v2 out{key, term_pos, {}};

    out.files.reserve(num_files);
    for (int i = 0; i < num_files; i++) {
        uint32_t docid = read_vnum(frequencies);
        uint32_t freq = read_vnum(frequencies);
        out.files.emplace_back(docid, freq);
    }

    return out;
}

StubIndexEntry Serializer::read_stub_index_entry_v2(std::istream &frequencies, std::istream &terms) {
    uint32_t frequencies_position = frequencies.tellg();
    auto wie = read_work_index_entry_v2(frequencies, terms);
    return StubIndexEntry{
            Base26Num(wie.key), wie.term_pos, frequencies_position, wie.key
    };
}


std::vector<StubIndexEntry> Serializer::read_sorted_keys_index_stub_v2(std::istream &frequencies, std::istream &terms) {
    constexpr int INTERVAL = 32; // read only every Nth entry.

    auto num_entries = Serializer::read_vnum(frequencies);

    std::vector<StubIndexEntry> out;
    out.reserve(num_entries / INTERVAL);

    for (int i = 0; i < num_entries; i++) {
        if (i % INTERVAL == 0) out.push_back(read_stub_index_entry_v2(frequencies, terms));
        else read_stub_index_entry_v2(frequencies, terms);
    }
    std::cout<<out.size()<<" stub entries\n";
    return out;
}

void Serializer::serialize_consume(std::string suffix, SortedKeysIndex index) {
    std::ofstream frequencies(indice_files_dir / ("frequencies-" + suffix), std::ios_base::binary);
    std::ofstream positions(indice_files_dir / ("positions-" + suffix), std::ios_base::binary);
    std::ofstream terms(indice_files_dir / ("terms-" + suffix), std::ios_base::binary);


    std::sort(index.get_index().begin(), index.get_index().end());

    serialize_vnum(frequencies, index.get_index().size());
    serialize_vnum(positions, index.get_index().size());
    for (auto &ie : index.get_index()) {
        serialize_consume(positions, frequencies, terms, std::move(ie));
    }
}


std::string Serializer::read_str(std::istream &stream) {
    auto length = read_vnum(stream);
    std::string buffer(length, ' ');
    stream.read(buffer.data(), length);
    return buffer;
}

std::vector<DocIDFilePair> Serializer::read_filepairs(std::istream &stream) {
    auto sz = read_vnum(stream);
    std::vector<DocIDFilePair> out;

    while (sz--) {
        out.push_back(read_pair(stream));
    }
    return out;
}


DocIDFilePair Serializer::read_pair(std::istream &stream) {
    return {read_vnum(stream), read_str(stream)};
}