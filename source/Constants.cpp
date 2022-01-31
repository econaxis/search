
#include <filesystem>
#include "Constants.h"
#include "random_b64_gen.h"
#include <iostream>
#include <cassert>

namespace fs = std::filesystem;

fs::path data_files_dir = "ERROR!/must-call-initialize-directory-variables-first";
fs::path indice_files_dir = "ERROR!/must-call-initialize-directory-variables-first";

const char *DEFAULT_PATH = ".";

extern "C" void initialize_directory_variables(const char *hint) {
    if (hint != nullptr) {
        data_files_dir = fs::path(hint);
        indice_files_dir = data_files_dir;
    } else {
        const char *data_files_dir_env = std::getenv("DATA_FILES_DIR");
        if (data_files_dir_env == nullptr) {
            data_files_dir = fs::path("./");
            indice_files_dir = data_files_dir;
        } else {
            data_files_dir = fs::path(data_files_dir_env);
            indice_files_dir = data_files_dir;
        }

    }
    std::cout << "Using dir: " << data_files_dir << "\n";
    std::cout<<std::filesystem::current_path()<<"\n";
}

