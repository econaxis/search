//
// Created by henry on 2021-05-25.
//

#ifndef GAME_FPSTUB_H
#define GAME_FPSTUB_H

#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;
class FPStub {
    static constexpr int interval = 5;
    std::vector<uint32_t> diffvec;
    mutable std::ifstream stream;
public:

    FPStub(fs::path path);

    std::string query(int docid) const;
};


#endif //GAME_FPSTUB_H
