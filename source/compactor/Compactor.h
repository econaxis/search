//
// Created by henry on 2021-04-30.
//

#ifndef GAME_COMPACTOR_H
#define GAME_COMPACTOR_H


#include <optional>
#include <filesystem>

namespace Compactor {
    namespace fs=std::filesystem;
    enum class ReadState {
        PROCESSED_ALREADY,
        GOOD,
        STREAM_ERROR
    };
    void create_directory(const std::filesystem::path& dirpath);
    void compact_directory(const std::filesystem::path &path, int max_merge = 100);


    std::tuple<Compactor::ReadState, fs::path, fs::path> read_one_index(std::fstream &stream);

    std::pair<Compactor::ReadState, std::string> read_and_mark_line(std::fstream &stream);
    std::pair<Compactor::ReadState, std::string> read_line(std::ifstream &stream);
};


#endif //GAME_COMPACTOR_H
