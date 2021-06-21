#ifndef GAME_DOCUMENTPOSITIONPOINTER_H
#define GAME_DOCUMENTPOSITIONPOINTER_H
#include <cstdint>
#include <vector>
#include <ostream>
#include <cassert>
#include "DocumentFrequency.h"

struct DocumentPositionPointer {
    uint32_t document_id;
    uint32_t document_position;

    bool operator< (const DocumentPositionPointer& other) const {
        if (document_id != other.document_id) return document_id < other.document_id;
        else return document_position < other.document_position;
    }
    bool operator> (const DocumentPositionPointer& other) const {
        if (document_id != other.document_id) return document_id > other.document_id;
        else return document_position > other.document_position;
    }

    bool operator!=(const DocumentPositionPointer& other) const {
        return document_id != other.document_id;
    }
    bool operator==(const DocumentPositionPointer& other) const {
        return document_id == other.document_id;
    }

    DocumentPositionPointer operator+(const DocumentPositionPointer& other) const {
        return DocumentPositionPointer {document_id + other.document_id, document_position + other.document_position};
    }
    DocumentPositionPointer operator-(const DocumentPositionPointer& other) const {
        return DocumentPositionPointer {document_id - other.document_id, document_position - other.document_position};
    }

    DocumentPositionPointer() = default;
    DocumentPositionPointer(uint32_t docid, uint32_t docpos) : document_id(docid), document_position(docpos) {};
};

inline bool operator==(const DocumentPositionPointer& one, const DocumentFrequency& two) {
    return one.document_id == two.document_id;
}
inline bool operator<(const DocumentPositionPointer& one, const DocumentFrequency& two) {
    return one.document_id < two.document_id;
}
inline bool operator<(const DocumentFrequency& one, const DocumentPositionPointer& two) {
    return one.document_id < two.document_id;
}

std::ostream &operator<<(std::ostream &os, std::vector<DocumentPositionPointer> vec);


#endif //GAME_DOCUMENTPOSITIONPOINTER_H
