#include "Serializer.h"
#include "Constants.h"
#include "SortedKeysIndex.h"


void Serializer::serialize(std::ostream &stream, const DocIDFilePair &p) {
    serialize_vnum(stream, p.document_id, false);
    serialize_str(stream, p.file_name);
}

void Serializer::serialize(std::ostream &stream, const std::vector<DocIDFilePair> &vp) {
    serialize_vnum(stream, vp.size(), false);

    for (const auto &i: vp) serialize(stream, i);
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
    constexpr uint32_t uint32max = (1 << 28) - 1;         // 0001...       four bytes
    constexpr uint64_t uint64max = (1ULL << 57) - 1;    // 00000001...   eight bytes

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
    } else {
        int a = stream.tellg();
        int b = stream.good();
        int c = stream.eof();
        std::cout << "Error: not a valid number; " << a << b << c;
    }

    return holder;
}


void Serializer::serialize_work_index_entry(std::ostream &frequencies, std::ostream &terms, std::ostream &positions,
                                            const WordIndexEntry &ie) {
    assert(std::is_sorted(ie.files.begin(), ie.files.end()));
    uint32_t term_pos = terms.tellp();
    serialize_str(terms, ie.key);

    /**
     * The frequencies file should be an array, where each element corresponds to a WordIndexEntry.
     *      Each element: [term_pos] [positions_pos] [number of elements = n]
     *                     n elements of [document_id][frequency]...
     */
    serialize_vnum(frequencies, term_pos, true);
//    serialize_vnum(frequencies, positions.tellp(), true);
    serialize_vnum(frequencies, 0, true);


    // contains <document_id, frequency> for the number of times this word appears in the document.
    std::vector<std::pair<uint32_t, uint32_t>> freq_data = ie.get_frequencies_vector();
    serialize_vnum(frequencies, freq_data.size(), true);
    for (auto&[a, b] : freq_data) {
        serialize_vnum(frequencies, a, true);
        serialize_vnum(frequencies, b, true);
    }


    /**
     * The positions file should be an array, where each element corresponds to a WordIndexEntry.
     *      Each element: [number of elements = n]
     *              Then, n repeats of: [document_id] [document_position]
     *
     *      The number of elements = sum of all frequencies for this WordIndexEntry in the frequencies file.
     *      Problem/TODO: There is a lot of duplicated data because we're storing document_id for each position entry.
     *      For example, document with id = 123 that contains the word "the" in 20 places would require
     *      20 * 32 bits for the document id (all 123) and 20 * 32 bits for the document position.
     */
    serialize_vnum(positions, ie.files.size(), false);
    for (const auto &i : ie.files) {
        serialize_vnum(positions, i.document_id, false);
        serialize_vnum(positions, i.document_position, false);
    }

}


WordIndexEntry
Serializer::read_work_index_entry(std::istream &frequencies, std::istream &terms, std::istream &positions) {
    // Frequencies data is useles, so we just use this function to consume the stream.
    WordIndexEntry_v2 wie2 = read_work_index_entry_v2(frequencies, terms);

    int num_positions = read_vnum(positions);
    WordIndexEntry wie{wie2.key, {}};
    wie.files.reserve(num_positions);

    while (num_positions--) {
        int docid = read_vnum(positions);
        int position = read_vnum(positions);
        wie.files.emplace_back(docid, position);
    }

    return wie;
}


PreviewResult Serializer::preview_work_index_entry(std::istream &frequencies, std::istream &terms) {
    uint32_t frequencies_off = 0;
    uint32_t terms_off = 0;
    uint32_t buf1[3];
    frequencies.read(reinterpret_cast<char *>(buf1), sizeof(uint32_t) * 3);

    frequencies_off += frequencies.gcount();
    buf1[0] >>= 4; // Rightshift by 4. This is term position.
    buf1[2] >>= 4; // Rightshift number by 4 bits. This is the number of files.
    frequencies.ignore(buf1[2] * 2 * sizeof(uint32_t));
    frequencies_off += frequencies.gcount();

    std::string key = read_str(terms);
    terms_off += terms.gcount();


    return {frequencies_off, terms_off, key};
}

#include <immintrin.h>

constexpr auto MAX_FILES_PER_TERM =  SortedKeysIndexStub::MAX_FILES_PER_TERM;
int Serializer::read_work_index_entry_v2_optimized(std::istream &frequencies,
                                                   __m256 *buffer) {

    uint32_t num_buffers[3];
    frequencies.read(reinterpret_cast<char *>(num_buffers), 3 * sizeof(uint32_t));
//    auto term_pos = num_buffers[0] >> 4;
    auto num_files = num_buffers[2] >> 4;
    auto *mybuffer = (DocumentPositionPointer_v2*) buffer;

    if(num_files > MAX_FILES_PER_TERM) {
        auto excess = num_files - MAX_FILES_PER_TERM;
        frequencies.ignore(excess * sizeof(DocumentPositionPointer_v2));
        num_files = MAX_FILES_PER_TERM;
    }
//    assert(buffer.size() * 8>= num_files * 2);

    // These VInts are padded to 4 bytes, so we can do this.
    frequencies.read(reinterpret_cast<char *>(mybuffer), num_files * sizeof(DocumentPositionPointer_v2));

    auto end = (uint32_t*) mybuffer + num_files * 2;
    auto start = (uint32_t*) mybuffer;

    for (; start + 8 < end; start += 8) {
        auto s256 = _mm256_load_si256(reinterpret_cast<const __m256i *>(start));
        s256 = _mm256_srai_epi32(s256, 4);

        _mm256_store_si256( reinterpret_cast<__m256i *>(start), s256);
    }

    for(; start < end; start++) {
        *start >>= 4;
    }

    return num_files;
}

WordIndexEntry_v2 Serializer::read_work_index_entry_v2(std::istream &frequencies, std::istream &terms) {
    auto term_pos = read_vnum(frequencies);
    read_vnum(frequencies); // skip positions.
    auto num_files = read_vnum(frequencies);
    terms.seekg(term_pos);

    auto key = read_str(terms);

    WordIndexEntry_v2 out{key, term_pos, {}};
    out.files.resize(num_files);
    // These VInts are padded to 4 bytes, so we can do this.
    frequencies.read(reinterpret_cast<char *>(out.files.data()), num_files * 2 * sizeof(uint32_t));
    for (auto &i : out.files) {
        i.frequency >>= 4;
        i.document_id >>= 4;
    }

    return out;
}

StubIndexEntry Serializer::read_stub_index_entry_v2(std::istream &frequencies, std::istream &terms) {
    uint32_t frequencies_position = frequencies.tellg();
    auto wie = read_work_index_entry_v2(frequencies, terms);
    return StubIndexEntry{
            Base26Num(wie.key), frequencies_position
    };
}


std::vector<StubIndexEntry> Serializer::read_sorted_keys_index_stub_v2(std::istream &frequencies, std::istream &terms) {
    constexpr int INTERVAL = 16; // read only every Nth entry.
    assert(frequencies.tellg() == 0 && terms.tellg() == 0);

    auto num_entries = Serializer::read_vnum(frequencies);
    auto num_entries1 = Serializer::read_vnum(terms);

    // Make sure the file isn't corrupted, we're getting some meaningful checks.
    assert(num_entries == num_entries1);

    std::vector<StubIndexEntry> out;
    out.reserve(num_entries / INTERVAL);


    for (int i = 0; i < num_entries; i++) {
        if (i % INTERVAL == 0) {
            out.push_back(read_stub_index_entry_v2(frequencies, terms));
            if (i% 50000 == 0) std::cout<<"Reading file "<<i * 100 / num_entries<<"% \r";
        }
        else preview_work_index_entry(frequencies, terms);
    }
    std::cout << out.size() << " stub entries\n";
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


std::string Serializer::read_str(std::istream &stream) {
    auto length = read_vnum(stream);
    std::string buffer(length, ' ');
    stream.read(buffer.data(), length);
    return buffer;
}

std::vector<DocIDFilePair> Serializer::read_filepairs(std::istream &stream) {
    int sz = read_vnum(stream);
    std::vector<DocIDFilePair> out;

    while (sz--) {
        out.push_back(read_pair(stream));
    }
    return out;
}


DocIDFilePair Serializer::read_pair(std::istream &stream) {
    uint32_t docid = read_vnum(stream);
    auto str = read_str(stream);
    return {docid, str};
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