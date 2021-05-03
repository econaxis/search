//
// Created by henry on 2021-05-01.
//

#include <filesystem>
#include "Constants.h"
#include "random_b64_gen.h"

namespace fs=std::filesystem;

extern fs::path data_files_dir = "ERROR!/must-call-initialize-directory-variables-first";


extern const std::string unique_directory_suffix = random_b64_str(5);

void initialize_directory_variables() {
    auto data_files_dir_env =std::getenv("DATA_FILES_DIR");
    if(data_files_dir_env) {
        data_files_dir = fs::path(data_files_dir_env);
    } else {
        data_files_dir = fs::path("/mnt/henry-80q7/.cache/data-files");
    }
}
