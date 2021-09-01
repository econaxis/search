#include "DocumentsTier.h"
#include "WordIndexEntry.h"
#include "Serializer.h"
#include "DocumentFrequency.h"
#include <ostream>



void MultiDocumentsTier::serialize(const WordIndexEntry &wie, std::ostream &frequencies) {
    using namespace Serializer;

    // Create the data instance

    auto data = std::vector<SingleDocumentsTier>{};
    auto array = wie.get_frequencies_vector();
    assert(std::is_sorted(array.begin(), array.end()));
    std::sort(array.begin(), array.end(), DocumentFrequency::FreqSorter);
    auto window_beg = array.begin();
    while (true) {
        auto end = std::min(window_beg + BLOCKSIZE, array.end());
        std::sort(window_beg, end);

        data.emplace_back(window_beg, end);

        window_beg += BLOCKSIZE;

        if (end == array.end()) break;
    }


    /*
     * Format:
     *
     * (n = how many blocks in total)
     * [n blocks of PackedFrequencies]
     *
     * PackedFrequencies:
     * (`BLOCKSIZE` number of document_ids, packed and difference-encoded)
     * (`BLOCKSIZE` number of frequencies, packed)
     * Last block contains the remaining elements, not BLOCKSIZE
     */
    serialize_vnum(frequencies, data.size());

    for (auto blocktier = data.begin(); blocktier != data.end(); blocktier++) {
        if (blocktier != data.end() - 1) assert(blocktier->size() == BLOCKSIZE);
        else {
            // Have to serialize the number of elements for the last block (as it's not BLOCKSIZE).
            serialize_vnum(frequencies, blocktier->size());
        }

        auto prev_docid = 0U;
        for (auto &a: *blocktier) {
            serialize_vnum(frequencies, a.document_id - prev_docid, true);
            prev_docid = a.document_id;
        }

        for (auto &a: *blocktier) {
            serialize_vnum(frequencies, a.document_freq, true);
        }
    }
}

std::optional<SingleDocumentsTier> MultiDocumentsTier::TierIterator::read_next() {
    if (remaining == 0) {
        return std::nullopt;
    }

    frequencies.seekg(read_position);

    SingleDocumentsTier output;
    uint32_t num_elems;

    if (remaining == 1) num_elems = Serializer::read_vnum(frequencies);
    else num_elems = BLOCKSIZE;


    output.resize(num_elems);
    std::vector<uint32_t> buffer(num_elems);
    // Read in the document ids
    Serializer::read_packed_u32_chunk(frequencies, num_elems, buffer.data());
    auto prevdocid = 0;
    for (int i = 0; i < num_elems; i++) {
        output[i].document_id = buffer[i] + prevdocid;
        prevdocid = output[i].document_id;
    }

    // Read in the frequencies
    Serializer::read_packed_u32_chunk(frequencies, num_elems, buffer.data());
    for (int i = 0; i < num_elems; i++) output[i].document_freq = buffer[i];
    remaining--;

    read_position = frequencies.tellg();
    return output;
}

SingleDocumentsTier MultiDocumentsTier::TierIterator::read_all() {
    SingleDocumentsTier out;
    while(true) {
        auto a = read_next();
        if(!a) break;
        out.insert(out.end(), a->begin(), a->end());
        std::inplace_merge(out.begin(), out.end() - a->size(), out.end());
    }
    assert(std::is_sorted(out.begin(), out.end()));
    return out;
}

MultiDocumentsTier::TierIterator::TierIterator(std::istream &frequencies) : frequencies(frequencies) {
    remaining = Serializer::read_vnum(frequencies);
    read_position = frequencies.tellg();
}