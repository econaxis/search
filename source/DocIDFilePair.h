
#ifndef GAME_DOCIDFILEPAIR_H
#define GAME_DOCIDFILEPAIR_H

#include <cstdint>
#include <string>
#include <microtar/microtar.h>

struct DocIDFilePair {
    uint32_t docid;
    std::string file_name;
};

inline bool operator!=(const DocIDFilePair& one, uint32_t two) {
    return one.docid != two;
}
inline bool operator==(const DocIDFilePair& one, uint32_t two) {
    return one.docid == two;
}


#endif //GAME_DOCIDFILEPAIR_H
