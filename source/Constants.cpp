
#include <filesystem>
#include "Constants.h"
#include "random_b64_gen.h"
#include <iostream>

namespace fs = std::filesystem;

fs::path data_files_dir = "ERROR!/must-call-initialize-directory-variables-first";
fs::path indice_files_dir = "ERROR!/must-call-initialize-directory-variables-first";
const std::string unique_directory_suffix = random_b64_str(5);

void initialize_directory_variables() {
    auto data_files_dir_env = std::getenv("DATA_FILES_DIR");
    if (data_files_dir_env) {
        data_files_dir = fs::path(data_files_dir_env);
        std::cout << "Using data file dir: " << data_files_dir_env << "\n";
    } else {
        data_files_dir = fs::path("/mnt/nfs/extra/gutenberg");
    }
    indice_files_dir = data_files_dir / "indices";
}

