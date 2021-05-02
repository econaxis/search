//
// Created by henry on 2021-05-01.
//
#include "compactor/Compactor.h"
#include <filesystem>
#include "Constants.h"

namespace fs=std::filesystem;
int main() {
    Compactor::compact_directory(data_files_dir/"indices");
}
