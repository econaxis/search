#include "compactor/Compactor.h"
#include "Constants.h"
#include "IndexFileLocker.h"
#include <cassert>
#include <iostream>

namespace fs = std::filesystem;

int main(int argc, char *argv[]) {
    initialize_directory_variables();

    if (argc == 2 && argv[1] == "check") {
        std::ifstream index_file(indice_files_dir / "index_files");
        std::ofstream index_file_working(indice_files_dir / "index_files_working");
        std::string line;
        while (std::getline(index_file, line)) {
            std::cout << "Testing " << line << "\n";
            try {
                Compactor::test_makes_sense(line);
                index_file_working << line << "\n" << std::flush;
            } catch (std::runtime_error e) {
                std::cerr << e.what() << "\n";
            }
        }
        return 0;
    } else {
        while (true) {
            auto joined_suffix = Compactor::compact_two_files();
            if (joined_suffix) {
                Compactor::test_makes_sense(joined_suffix.value());
                assert(IndexFileLocker::acquire_lock_file());
                std::fstream index_file(indice_files_dir / "index_files", std::ios_base::app);
                index_file << joined_suffix.value() << "\n";
                IndexFileLocker::release_lock_file();

                std::cout << "Compacted to " << joined_suffix.value() << "\n";
            } else {
                break;
            }
        };
    }


}
