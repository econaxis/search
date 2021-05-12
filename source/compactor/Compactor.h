
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

    std::pair<Compactor::ReadState, std::string> read_and_mark_line(std::fstream &stream);
    std::pair<Compactor::ReadState, std::string> read_line(std::ifstream &stream);

    std::optional<std::string> compact_two_files();

    void test_makes_sense(const std::string& suffix);
};


#endif //GAME_COMPACTOR_H
