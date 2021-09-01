
#ifndef GAME_DOCUMENTFREQUENCY_H
#define GAME_DOCUMENTFREQUENCY_H

#include <cstdint>

struct DocumentFrequency {
    uint32_t document_id;
    uint32_t document_freq;

    DocumentFrequency(uint32_t a, uint32_t b) : document_id(a), document_freq(b) {};
    DocumentFrequency() = default;

    bool operator<(const DocumentFrequency &other) const {
        return document_id < other.document_id;
    }
    bool operator<=(const DocumentFrequency& other) const {
        return document_id <= other.document_id;
    }
    bool operator==(const DocumentFrequency& other) const {
        return (document_id == other.document_id);
    }
    bool operator!=(const DocumentFrequency& other) const {
        return (document_id != other.document_id);
    }


    static bool FreqSorter(const DocumentFrequency &one, const DocumentFrequency &two) {
        return one.document_freq < two.document_freq;
    }
};



#endif //GAME_DOCUMENTFREQUENCY_H
