#ifndef GAME_FPSTUB_H
#define GAME_FPSTUB_H

#include <fstream>
#include <filesystem>
#include <robin_hood/robin_hood.h>

namespace fs = std::filesystem;
class FPStub {
    robin_hood::unordered_map<uint32_t, std::string> map;
public:

    explicit FPStub(const fs::path& path);
    FPStub(const FPStub& other) {
        map = other.map;
    }

    std::string query(uint32_t docid) const;
};


#endif //GAME_FPSTUB_H
