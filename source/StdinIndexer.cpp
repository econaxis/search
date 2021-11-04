#define STDININDEXER_PRINT_PROGRESS

#include <iostream>
#include "GeneralIndexer.h"
#include "Constants.h"
#include "FileListGenerator.h"
#include "StdinIndexer.h"
#include "IndexFileLocker.h"


int main() {
    initialize_directory_variables();

    auto str = GeneralIndexer::read_some_files(queue_produce_file_contents_stdin);
    IndexFileLocker::do_lambda([&] {
        std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
        index_file << str << "\n";
    });


    std::cout<<"Suffix: " << str <<"\n";
}




