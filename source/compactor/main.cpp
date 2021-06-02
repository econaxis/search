#include "compactor/Compactor.h"
#include "Constants.h"
#include "IndexFileLocker.h"
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

int main() {
    initialize_directory_variables();


    while (true) {

        auto joined_suffix = Compactor::compact_two_files();

        if (joined_suffix) {
            Compactor::test_makes_sense(joined_suffix.value());
        } else {
            break;
        }
    };
}
