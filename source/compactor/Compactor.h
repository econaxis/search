//
// Created by henry on 2021-04-30.
//

#ifndef GAME_COMPACTOR_H
#define GAME_COMPACTOR_H


#include <optional>
#include <filesystem>

namespace Compactor {
    enum class ReadState {
        PROCESSED_ALREADY,
        GOOD,
        STREAM_ERROR
    };
    void create_directory(const std::filesystem::path& dirpath);
    void compact_directory(const std::filesystem::path &path, int max_merge = 2);


    std::tuple<ReadState, std::string, std::string> read_one_index(std::fstream &stream);

    std::pair<ReadState, std::string> read_and_mark_line(std::fstream &stream);
};


#endif //GAME_COMPACTOR_H
