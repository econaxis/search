#ifndef GAME_DOCIDFILEPAIR_H
#define GAME_DOCIDFILEPAIR_H

#include <cstdint>
#include <string>
#include <microtar/microtar.h>

struct DocIDFilePair {
    uint32_t document_id;
    std::string file_name;
};

inline bool operator!=(const DocIDFilePair& one, uint32_t two) {
    return one.document_id != two;
}
inline bool operator==(const DocIDFilePair& one, uint32_t two) {
    return one.document_id == two;
}
inline bool operator<(const DocIDFilePair& one, uint32_t two) {
    return one.document_id < two;
}
inline bool operator<(const DocIDFilePair& one, const DocIDFilePair& two) {
    return one.document_id < two.document_id;
}


#endif //GAME_DOCIDFILEPAIR_H
