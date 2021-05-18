
#ifndef GAME_DOCUMENTPOSITIONPOINTER_H
#define GAME_DOCUMENTPOSITIONPOINTER_H
#include <cstdint>
#include <vector>
#include <ostream>

struct DocumentPositionPointer {
    uint32_t document_id;
    uint32_t document_position;

    bool operator< (const DocumentPositionPointer& other) const {
        return document_id < other.document_id;
    }

    bool operator!=(const DocumentPositionPointer& other) const {
        return document_id != other.document_id;
    }

    DocumentPositionPointer() = default;
    DocumentPositionPointer(uint32_t docid, uint32_t docpos) : document_id(docid), document_position(docpos) {};
};


struct DocumentPositionPointer_v2 {
    uint32_t document_id;
    uint32_t frequency;

    bool operator<(const DocumentPositionPointer_v2& other) const {
        return document_id < other.document_id;
    }
    bool operator==(const DocumentPositionPointer_v2& other) const {
        return (document_id == other.document_id);
    }
    bool operator!=(const DocumentPositionPointer_v2& other) const {
        return (document_id != other.document_id);
    }

    DocumentPositionPointer_v2(uint32_t a, uint32_t b) : document_id(a), frequency(b) {};
    DocumentPositionPointer_v2() = default;
};
std::ostream &operator<<(std::ostream &os, std::vector<DocumentPositionPointer> vec);


#endif //GAME_DOCUMENTPOSITIONPOINTER_H
