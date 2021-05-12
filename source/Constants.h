
#ifndef GAME_CONSTANTS_H
#define GAME_CONSTANTS_H

#include <filesystem>
extern std::filesystem::path data_files_dir;
extern std::filesystem::path indice_files_dir;
extern const std::string unique_directory_suffix;
void initialize_directory_variables();
#endif //GAME_CONSTANTS_H
