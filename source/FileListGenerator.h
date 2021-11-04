#ifndef GAME_FILELISTGENERATOR_H
#define GAME_FILELISTGENERATOR_H

#include <filesystem>
#include "rust-interface.h"
#include <iostream>
#include <memory>
#include "DocIDFilePair.h"
#include "IndexFileLocker.h"
#include "random_b64_gen.h"

constexpr unsigned int MAX_FILES_PER_INDEX = 300000;

namespace FileListGenerator {
    using FilePairs = std::vector<DocIDFilePair>;
    namespace fs = std::filesystem;

    inline std::ifstream &get_files_list() {
        static auto dir_it = std::ifstream(data_files_dir / "total-files-list");
        return dir_it;
    }

    // Creates a list of files to index.
    // Deals with multiple processes by acquiring a lock file.
    inline FilePairs from_file() {
        using namespace std::chrono;

        // Add some jitter as we're not sure that creating a file is an atomic operation in the filesystem implementation.
        std::this_thread::sleep_for(milliseconds(random_long(10, 100)));

        while (!IndexFileLocker::acquire_lock_file()) {
            std::cerr << "Blocking: lock file already exists\n";
            std::this_thread::sleep_for(seconds(5 + random_long(0, 5)));
        }

        FilePairs filepairs;

        auto &dir_it = get_files_list();

        uint32_t doc_id_counter = 1;
        std::string file_line;
        auto cur_size = 0ULL;
        // Consume directory iterator and push into filepairs vector
        while (std::getline(dir_it, file_line)) {
            auto abspath = data_files_dir / "data" / file_line;

            cur_size += fs::file_size(abspath);

            // Don't index more than x files or 500MB at a time.
            if (filepairs.size() > MAX_FILES_PER_INDEX || cur_size > static_cast<int>(200e6)) break;
            filepairs.push_back(DocIDFilePair{doc_id_counter, file_line});

            doc_id_counter++;

        }
        std::cout << filepairs.size() << " files will be processed\n";

        // Release the lock file.
        IndexFileLocker::release_lock_file();

        return filepairs;
    }


};


#endif //GAME_FILELISTGENERATOR_H
