
#include <filesystem>
#include "Constants.h"
#include "random_b64_gen.h"
#include <iostream>
#include <cassert>

namespace fs = std::filesystem;

fs::path data_files_dir = "ERROR!/must-call-initialize-directory-variables-first";
fs::path indice_files_dir = "ERROR!/must-call-initialize-directory-variables-first";

extern "C" void initialize_directory_variables() {
    auto data_files_dir_env = std::getenv("DATA_FILES_DIR");
    assert(data_files_dir_env);
    data_files_dir = fs::path(data_files_dir_env);
    indice_files_dir = data_files_dir / "indices";
    std::cout<<"Using dir: "<<data_files_dir<<"\n";
}

