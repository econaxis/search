//
// Created by henry on 2021-05-01.
//
#include "compactor/Compactor.h"
#include <filesystem>
#include "Constants.h"

namespace fs=std::filesystem;
int main() {
    initialize_directory_variables();

    Compactor::compact_directory(data_files_dir/"indices");
}
