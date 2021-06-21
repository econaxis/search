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
    std::shared_ptr<NamesDatabase *> ndb{nullptr};


    NamesDatabase *get_ndb() {
        if (!ndb || *ndb == nullptr) {
            auto path = indice_files_dir;
            ndb = std::make_shared<NamesDatabase *>(new_name_database(path.c_str()));
        }
        return *ndb;
    }

    void delete_names_db() {
        if (*ndb != nullptr) {
            // NO need to do it, hold NDB for the rest of the program
            // drop_name_database(*ndb);
        }
    }

    std::ifstream &get_index_files() {
        static auto dir_it = std::ifstream(data_files_dir / "total-files-list");
        return dir_it;
    }

    // Creates a list of files to index.
    // Deals with multiple processes by acquiring a lock file.
    FilePairs from_file() {
        using namespace std::chrono;
        std::this_thread::sleep_for(milliseconds(random_long(10, 1000)));

        while (!IndexFileLocker::acquire_lock_file()) {
            std::cerr << "Blocking: lock file already exists\n";
            std::this_thread::sleep_for(seconds(5 + random_long(0, 5)));
        }

        FilePairs filepairs;

        auto &dir_it = get_index_files();

        uint32_t doc_id_counter = 1;
        std::string file_line;
        auto cur_size = 0ULL;
        // Consume directory iterator and push into filepairs vector
        while (std::getline(dir_it, file_line)) {
            if (search_name_database(get_ndb(), file_line.c_str())) {
                // Entry already exists.
                std::cout << "Entry " << file_line << " already exists\r";
                continue;
            }
            auto abspath = data_files_dir / "data" / file_line;

            cur_size += fs::file_size(abspath);

            // Don't index more than x files or 500MB at a time.
            if (doc_id_counter > MAX_FILES_PER_INDEX || cur_size > static_cast<int>(500e6)) break;
            doc_id_counter++;
            register_temporary_file(get_ndb(), file_line.c_str(), doc_id_counter);
            filepairs.push_back(DocIDFilePair{doc_id_counter, file_line});
        }
        std::cout << filepairs.size() << " files will be processed\n";

        serialize_namesdb(get_ndb());
        // Release the lock file.
        IndexFileLocker::release_lock_file();

        return filepairs;
    }


};


#endif //GAME_FILELISTGENERATOR_H
