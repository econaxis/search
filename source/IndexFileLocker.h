
#ifndef GAME_LOCKFILE_H
#define GAME_LOCKFILE_H
#include <filesystem>
constexpr std::string_view LOCKFILE = "/tmp/search-total-files-list.lock";

namespace IndexFileLocker {
    namespace fs = std::filesystem;

    bool acquire_lock_file() {
        using namespace std::chrono;
        if (fs::exists(fs::path(LOCKFILE))) {
            return false;
        } else {
            std::ofstream ofs(LOCKFILE);
            auto now = system_clock::now();
            auto now1 = system_clock::to_time_t(now);
            ofs << std::put_time(std::localtime(&now1), "%c");
            ofs.close();
        }
        return true;
    }

    void release_lock_file() {
	fs::remove(fs::path(LOCKFILE));
    }

};


#endif 
