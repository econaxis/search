#ifndef GAME_FPSTUB_H
#define GAME_FPSTUB_H

#include <fstream>
#include <filesystem>
#include <robin_hood/robin_hood.h>

namespace fs = std::filesystem;
class FPStub {
    struct StringSlice {
        std::size_t index;
        std::size_t size;
    };

    std::string joined_names;
    robin_hood::unordered_map<uint32_t, StringSlice> map;
public:

    explicit FPStub(const fs::path& path);
    FPStub(const FPStub& other) {
        map = other.map;
        joined_names = other.joined_names;
    }

    std::string query(uint32_t docid) const;
};


#endif //GAME_FPSTUB_H
