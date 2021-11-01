
#ifndef GAME_LOCKFILE_H
#define GAME_LOCKFILE_H

#include "Constants.h"
#include <filesystem>
#include <string>
#include <string_view>
#include <fstream>
#include <thread>
#include <iostream>
#include <cstdlib>

constexpr std::string_view LOCKFILE = "/tmp/search-total-files-list.lock";

namespace IndexFileLocker {
    namespace fs = std::filesystem;



    inline bool acquire_lock_file() {
        return true;
        using namespace std::chrono;

        while (fs::exists(LOCKFILE)) {
            std::cout<<"Error: blocking on lockfile\n";
            std::this_thread::sleep_for(milliseconds (std::rand() % 10000 + 1));
        }

        std::ofstream ofs(std::string{LOCKFILE});
        auto now = system_clock::now();
        auto now1 = system_clock::to_time_t(now);
        ofs << std::put_time(std::localtime(&now1), "%c");
        ofs.close();

        return true;
    }

    inline void release_lock_file() {
        fs::remove(fs::path(LOCKFILE));
    }

    inline void move_all(std::string old_suffix, std::string new_suffix) {
        fs::rename(indice_files_dir / ("filemap-" + old_suffix), indice_files_dir / ("filemap-" + new_suffix));
        fs::rename(indice_files_dir / ("terms-" + old_suffix), indice_files_dir / ("terms-" + new_suffix));
        fs::rename(indice_files_dir / ("frequencies-" + old_suffix), indice_files_dir / ("frequencies-" + new_suffix));
        fs::rename(indice_files_dir / ("positions-" + old_suffix), indice_files_dir / ("positions-" + new_suffix));
    }

    template<typename Callable>
    inline void do_lambda(Callable c) {
        acquire_lock_file();
        c();
        release_lock_file();
    }
};


#endif 
