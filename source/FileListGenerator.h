#ifndef GAME_FILELISTGENERATOR_H
#define GAME_FILELISTGENERATOR_H
#include <filesystem>
#include "rust-interface.h"
#include <iostream>
constexpr std::string_view LOCKFILE = "/tmp/search-total-files-list.lock";
constexpr unsigned int MAX_FILES_PER_INDEX = 50000;

namespace FileListGenerator {
    using FilePairs = std::vector<DocIDFilePair>;
    namespace fs = std::filesystem;
    NamesDatabase *ndb = nullptr;

    bool acquire_lock_file() {
        using namespace std::chrono;
        if (fs::exists(LOCKFILE)) {
            return false;
        } else {
            std::ofstream ofs(data_files_dir / LOCKFILE);
            auto now = system_clock::now();
            auto now1 = system_clock::to_time_t(now);
            ofs << std::put_time(std::localtime(&now1), "%c");
        }
        return true;
    }

    void init_names_db() {
        assert(ndb == nullptr);
        auto path = indice_files_dir;
        ndb = new_name_database(path.c_str());
    }


    FilePairs from_file() {
        if (!ndb) init_names_db();

        FilePairs filepairs;
        auto dir_it = std::ifstream(data_files_dir / "total-files-list");

        while (!acquire_lock_file()) {
            using namespace std::chrono_literals;
            std::cerr << "Blocking: lock file already exists\n";
            std::this_thread::sleep_for(10s);
        }
        uint32_t doc_id_counter = 1;
        std::string file_line;
        auto cur_size = 0ULL;
        // Consume directory iterator and push into filepairs vector
        while (std::getline(dir_it, file_line)) {
            if(search_name_database(ndb, file_line.c_str())) {
                // Entry already exists.
                std::cout<<"Entry "<<file_line<<" already exists\r";
                continue;
            }
            auto abspath = data_files_dir / "data" / file_line;
            if (!fs::exists(abspath) || !fs::is_regular_file(abspath)) {
                std::cerr << "Path " << abspath.c_str() << " nonexistent\n";
                continue;
            }

            cur_size+= fs::file_size(abspath);

            // Don't index more than x files or 500MB at a time.
            if (doc_id_counter > MAX_FILES_PER_INDEX || cur_size > 3e8) break;
            doc_id_counter++;
//            register_temporary_file(ndb, file_line.c_str(), doc_id_counter);
            filepairs.push_back(DocIDFilePair{doc_id_counter, file_line});
        }
        // Clean up NDB and serialize changes (temporary file registrations) to disk.
        drop_name_database(ndb);
        ndb = nullptr;

        // Release the lock file.
        fs::remove(LOCKFILE);

        return filepairs;
    }


};


#endif //GAME_FILELISTGENERATOR_H
