#ifndef GAME_FPSTUB_H
#define GAME_FPSTUB_H

#include <fstream>
#include <filesystem>
#include <robin_hood/robin_hood.h>

namespace fs = std::filesystem;
class FPStub {
    static constexpr int interval = 1;
    std::vector<uint32_t> diffvec;
    std::unique_ptr<char[]> buffer;

    mutable std::ifstream stream;
    robin_hood::unordered_map<uint32_t, std::string> map;

public:

    FPStub(fs::path path);

    std::string query(int docid) const;
};


#endif //GAME_FPSTUB_H
