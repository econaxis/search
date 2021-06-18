#include "compactor/Compactor.h"
#include "Constants.h"
#include "IndexFileLocker.h"
#include <cassert>
#include <iostream>
#include <cstring>
#include <thread>
#include <vector>
#include "FileListGenerator.h"
#include <future>

namespace fs = std::filesystem;

void Compactor_test();

static fs::path make_path(const std::string &name, const std::string &suffix) {
    return indice_files_dir / (name + "-" + suffix);
}

int main(int argc, char *argv[]) {

//    Compactor_test();

    initialize_directory_variables();


    if (argc == 2 && strcmp(argv[1], "check") == 0) {
        fs::rename(indice_files_dir/"index_files", indice_files_dir/"index_files_old");

        std::ifstream index_file(indice_files_dir/"index_files_old");
        std::ofstream index_file_working(indice_files_dir / "index_files");
        std::string line;
        std::vector<std::future<std::string>> threads;
        while (std::getline(index_file, line)) {
            threads.emplace_back(std::async(std::launch::async | std::launch::deferred, [=]() {
                try {
                    Compactor::test_makes_sense(line);
                    std::cout<<"Checked "<<line<<"\n";
                    return line;
                } catch (const std::runtime_error &e) {
                    std::cerr << "Error for line "<<line<<"\n"<< e.what() << "\n";
                    fs::remove(make_path("frequencies", line));
                    fs::remove(make_path("terms", line));
                    fs::remove(make_path("positions", line));
                    fs::remove(make_path("filemap", line));
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
        FileListGenerator::delete_names_db();
    }


}
