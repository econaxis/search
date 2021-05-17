#ifndef GAME_FILELISTGENERATOR_H
#define GAME_FILELISTGENERATOR_H
#include <filesystem>
#include "rust-interface.h"
constexpr std::string_view LOCKFILE = "/tmp/search-total-files-list.lock";
constexpr unsigned int MAX_FILES_PER_INDEX = 100000;

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

        if (!acquire_lock_file()) {
            std::cerr << "Lock file exists\n";
            return filepairs;
        }
        uint32_t doc_id_counter = 1;
        std::string file_line;
        // Consume directory iterator and push into filepairs vector
        while (std::getline(dir_it, file_line)) {
            if(search_name_database(ndb, file_line.c_str())) {
                // Entry already exists.
                std::cout<<"Entry "<<file_line<<" already exists\r";
                continue;
            }
            if (doc_id_counter > MAX_FILES_PER_INDEX) break;
            filepairs.push_back(DocIDFilePair{doc_id_counter++, file_line});
        }
        // Release the lock file.
        fs::remove(LOCKFILE);
        // Clean up NDB
        drop_name_database(ndb);
        ndb = nullptr;

        return filepairs;
    }


};


#endif //GAME_FILELISTGENERATOR_H
