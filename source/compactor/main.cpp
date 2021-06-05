#include "compactor/Compactor.h"
#include "Constants.h"
#include "IndexFileLocker.h"
#include <cassert>
#include <iostream>
#include <cstring>
#include <thread>
#include <vector>

#include <future>

namespace fs = std::filesystem;

void Compactor_test();

int main(int argc, char *argv[]) {

    initialize_directory_variables();


    if (argc == 2 && strcmp(argv[1], "check") == 0) {
        std::ifstream index_file(indice_files_dir / "index_files");
        std::ofstream index_file_working(indice_files_dir / "index_files_working");
        std::string line;
        std::vector<std::future<std::string>> threads;
        while (std::getline(index_file, line)) {
            threads.emplace_back(std::async(std::launch::async | std::launch::deferred, [=]() {
                std::cout << "Testing " << line << "\n";
                try {
                    Compactor::test_makes_sense(line);
                    return line;
                } catch (const std::runtime_error &e) {
                    std::cerr << e.what() << "\n";
                    return std::string{""};
                }
            }));

            if (threads.size() > 8) {
                while (!threads.empty()) {
                    auto line = threads.back().get();
                    threads.pop_back();

                    if (!line.empty()) {
                        index_file_working << line << "\n" << std::flush;
                    } else {
                        std::cerr << line << " invalid\n";
                    }
                }
            }
        }
        return 0;
    } else {
        while (true) {
            auto joined_suffix = Compactor::compact_two_files();

            if (joined_suffix) {
                if (joined_suffix.value() == "CONTINUE") {
                    continue;
                }
                std::cout << "Compacted to " << joined_suffix.value() << "\n";
            } else {
                break;
            }
        };
    }


}
