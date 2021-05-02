//
// Created by henry on 2021-05-01.
//

#include <filesystem>
#include "Constants.h"
#include "random_b64_gen.h"

namespace fs=std::filesystem;

extern const fs::path data_files_dir = "/home/henry/.cache/data-files";
extern const std::string unique_directory_suffix = random_b64_str(5);