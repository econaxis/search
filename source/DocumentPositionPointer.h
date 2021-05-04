
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


    DocumentPositionPointer(uint32_t docid, uint16_t docpos) : document_id(docid), document_position(docpos) {};
};
std::ostream &operator<<(std::ostream &os, std::vector<DocumentPositionPointer> vec);

#endif //GAME_DOCUMENTPOSITIONPOINTER_H
