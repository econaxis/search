//
// Created by henry on 2021-05-01.
//
#include "compactor/Compactor.h"
#include <filesystem>

namespace fs=std::filesystem;
int main() {
    Compactor::compact_directory(fs::path("../data-files/indices"));
}
