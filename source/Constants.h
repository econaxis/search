#ifndef GAME_CONSTANTS_H
#define GAME_CONSTANTS_H

#include <filesystem>
extern std::filesystem::path data_files_dir;
extern std::filesystem::path indice_files_dir;
constexpr inline int STUB_INTERVAL = 10;
extern "C" void initialize_directory_variables(const char* hint);

#endif //GAME_CONSTANTS_H
