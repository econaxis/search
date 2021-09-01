#include "PositionsSearcher.h"
#include <iostream>
#include "Serializer.h"
#include "Constants.h"
#include "SortedKeysIndex.h"
#include "DocumentsTier.h"
#include "DocumentFrequency.h"
#include <immintrin.h>

namespace {
#ifdef HAS_EXEC_INFO
#include <execinfo.h>
#else
#warning "No exec info header found."
    int backtrace(void **arg1, int arg2) {
//        std::cout << "No backtrace available";
        return 0;
    }
    char** backtrace_symbols(void** arg1, int arg2) {
        return nullptr;
    }
#endif
}

using namespace Serializer;

void print_backtrace() {
    void *tracePtrs[15];
    auto count = ::backtrace(tracePtrs, 15);
    char **funcnames = ::backtrace_symbols(tracePtrs, count);
    for (int i = 0; i < count; i++) {
        std::cout << "Backtrace: " << funcnames[i] << "\n";
    }
}


std::string read_str(std::istream &stream) {
    auto length = read_vnum(stream);
    std::string buffer(length, ' ');
    stream.read(buffer.data(), length);
    return buffer;
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
    } else if (byte & 1 << 2) {
        stream.read(reinterpret_cast<char *> (&holder), 3);
        byte = byte >> 3; // byte has 4 bits of info.
        // 32 bit number
        holder = (holder << 5) | byte;
    } else if (byte & 1 << 3) {
        uint64_t bigholder = 0;
        stream.read(reinterpret_cast<char *>(&bigholder), 7);
        byte = byte >> 4;
        bigholder = (bigholder << 4) | byte;

        if (holder > (1 >> 31)) {
            throw std::runtime_error("64 bit number can't be coerced to 32 bits");
        }
        holder = static_cast<uint32_t>(bigholder);
    } else {
        int a = stream.tellg();
        int b = stream.good();
        int c = stream.eof();
        std::cout << "Error: not a valid number; " << a << " " << b << " " << c;
        print_backtrace();

        throw std::runtime_error("Error: not a valid number " + std::to_string(a) + " " + std::to_string(c));
//        return 1<<31;
    }

    return holder;
}


void serialize(std::ostream &stream, const DocIDFilePair &p) {
    serialize_vnum(stream, p.document_id, false);
    serialize_str(stream, p.file_name);
}

void Serializer::serialize(std::string suffix, const std::vector<DocIDFilePair> &vp) {
    std::ofstream filemapstream(indice_files_dir / ("filemap-" + suffix), std::ios_base::binary);

    serialize(filemapstream, vp);
}

void Serializer::serialize(std::ostream &filemapstream, const std::vector<DocIDFilePair> &vp) {
    serialize_vnum(filemapstream, vp.size(), false);

    for (const auto &i: vp) serialize(filemapstream, i);
}

void Serializer::serialize_str(std::ostream &stream, const std::string &str) {
    auto length = str.length();
    auto *c_str = str.c_str();

    serialize_vnum(stream, length, false);
    stream.write(c_str, length);
}


void Serializer::serialize_vnum(std::ostream &stream, uint32_t number, bool pad32) {
// Position of first set bit in the first part determines how many bytes long the number is.
    constexpr uint8_t uint8max = (1 << 7) - 1;        // 1...          one byte
    constexpr uint16_t uint16max = (1 << 14) - 1;         // 01...         two bytes
    constexpr uint32_t uint32max = (1 << 29) - 1;         // 0001...       four bytes
    [[maybe_unused]] constexpr uint64_t uint64max = (1ULL << 57) - 1;    // 00000001...   eight bytes

    auto write_num = [&](auto num) {
        stream.write(static_cast<const char *>((void *) (&num)), sizeof(num));
    };
    if (number <= uint8max && !pad32) {
        number = number << 1;
        number |= 1 << 0;
        write_num(static_cast<uint8_t>(number));
    } else if (number <= uint16max && !pad32) {
        number = number << 2;
        number |= 1 << 1;
        write_num(static_cast<uint16_t>(number));
    } else if (number <= uint32max) {
        number = number << 3;
        number |= 1 << 2;
        write_num(static_cast<uint32_t>(number));
    } else if (!pad32) {
        uint64_t num64 = ((uint64_t) number) << 4;
        num64 |= 1 << 3;
        write_num(num64);
    } else {
        std::cout << "Number: " << number << "\n";
        print_backtrace();
        throw std::runtime_error("Number too big");
    }
}


void Serializer::serialize_work_index_entry(std::ostream &frequencies, std::ostream &terms, std::ostream &positions,
                                            const WordIndexEntry &ie) {
    /**
     * Serialize the headers, which contain the positions in the larger index files that this term is connected with.
     * Since scanning through the terms file is more efficient, we can scan through the terms file only, then when
     * we reach a term that we like to examine further, we can seek to its frequencie/positions pointer and read.
     */
    assert(std::is_sorted(ie.files.begin(), ie.files.end()));
    serialize_str(terms, ie.key);
    serialize_vnum(terms, frequencies.tellp(), false);
    serialize_vnum(terms, positions.tellp(), false);

    // Serialize frequencies data.
    MultiDocumentsTier::serialize(ie, frequencies);

    // Serialize positions data.
    PositionsSearcher::serialize_positions(positions, ie);
}


WordIndexEntry
Serializer::read_work_index_entry(std::istream &frequencies, std::istream &terms, std::istream &positions) {
    // Frequencies data is useles, so we just use this function to consume the stream.
    WordIndexEntry_v2 wie2 = read_work_index_entry_v2(frequencies, terms);

    // then read the positions
    auto dpp = PositionsSearcher::read_positions_all(positions, wie2.files);

    WordIndexEntry wie{wie2.key, dpp};
    return wie;
}

// Frequencies istream should be in the correct aligned position already
PreviewResult Serializer::preview_work_index_entry(std::istream &terms) {

    std::string key = read_str(terms);
    auto frequencies_pos = read_vnum(terms);
    auto positions_pos = read_vnum(terms); // positions_pos, currently unused at this stage.

    return {frequencies_pos, positions_pos, key};
}


void Serializer::read_packed_u32_chunk(std::istream &frequencies, uint32_t length, uint32_t *buffer) {
    frequencies.read(reinterpret_cast<char *>(buffer), length * sizeof(uint32_t));

    auto end = (uint32_t *) buffer + length;
    auto start = (uint32_t *) buffer;

    // since we're using unaligned SIMD operations, we'll lose a bit of performance but remove the
    // alignment constraint
    for (; start + 8 < end; start += 8) {
        auto s256 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(start));
        s256 = _mm256_srai_epi32(s256, 3);

        _mm256_storeu_si256(reinterpret_cast<__m256i *>(start), s256);
    }

    for (; start < end; start++) {
        *start >>= 3;
    }
}


// cooperate with MultiDocumentsTier stream wrapper to iterate through frequency-sorted blocks.
WordIndexEntry_v2 Serializer::read_work_index_entry_v2(std::istream &frequencies, std::istream &terms) {
    auto key = read_str(terms);
    auto frequencies_pos = read_vnum(terms);
    read_vnum(terms); // positions_pos



    frequencies.seekg(frequencies_pos);

    WordIndexEntry_v2 out{key, {}};

    out.files = MultiDocumentsTier::TierIterator(frequencies).read_all();

    assert(std::is_sorted(out.files.begin(), out.files.end()));
    // These VInts are padded to 4 bytes, so we can do this.

    return out;
}

StubIndexEntry Serializer::read_stub_index_entry_v2(std::istream &frequencies, std::istream &terms) {
    uint32_t terms_pos = terms.tellg();
    auto wie = read_work_index_entry_v2(frequencies, terms);
    return StubIndexEntry{
            Base26Num(wie.key), terms_pos
    };
}


std::vector<StubIndexEntry> Serializer::read_sorted_keys_index_stub_v2(std::istream &frequencies, std::istream &terms) {
    constexpr int INTERVAL = STUB_INTERVAL; // read only every Nth entry.
    assert(frequencies.tellg() == 0 && terms.tellg() == 0);

    auto num_entries = Serializer::read_vnum(frequencies);
    [[maybe_unused]] auto num_entries1 = Serializer::read_vnum(terms);

    // Make sure the file isn't corrupted, we're getting some meaningful checks.
    assert(num_entries == num_entries1);

    std::vector<StubIndexEntry> out;
    out.reserve(num_entries / INTERVAL);


    for (int i = 0; i < num_entries; i++) {
        if (i % INTERVAL == 0 || i == num_entries - 1) {
            out.push_back(read_stub_index_entry_v2(frequencies, terms));
        } else preview_work_index_entry(terms);
    }
    return out;
}

void Serializer::serialize(const std::string &suffix, const SortedKeysIndex &index) {
    std::ofstream frequencies(indice_files_dir / ("frequencies-" + suffix), std::ios_base::binary);
    std::ofstream positions(indice_files_dir / ("positions-" + suffix), std::ios_base::binary);
    std::ofstream terms(indice_files_dir / ("terms-" + suffix), std::ios_base::binary);


    assert(std::is_sorted(index.get_index().begin(), index.get_index().end()));

    serialize_vnum(frequencies, index.get_index().size(), false);
    serialize_vnum(positions, index.get_index().size(), false);
    serialize_vnum(terms, index.get_index().size(), false);
    for (const auto &ie : index.get_index()) {
        serialize_work_index_entry(frequencies, terms, positions, ie);
    }
}


DocIDFilePair read_pair(std::istream &stream) {
    uint32_t docid = read_vnum(stream);
    auto str = read_str(stream);
    return {docid, str};
}

std::vector<DocIDFilePair> Serializer::read_filepairs(std::istream &stream) {
    int sz = read_vnum(stream);
    std::vector<DocIDFilePair> out;

    while (sz--) {
        out.push_back(read_pair(stream));
    }
    return out;
}


std::ifstream *Serializer::ffi::create_ifstream_from_path(const char *path) {
    fs::path fspath(path);
    auto *stream = new std::ifstream(fspath, std::ios_base::binary);
    return stream;
}

void Serializer::ffi::deallocate(std::ifstream *stream) {
    delete stream;
}

void Serializer::ffi::deallocate(std::ofstream *stream) {
    delete stream;
}