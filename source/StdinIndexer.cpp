//
// Created by henry on 2021-06-23.
//

#include <iostream>
#include <atomic>
#include "SyncedQueue.h"
#include "GeneralIndexer.h"
#include "Constants.h"
#include "FileListGenerator.h"
#include "StdinIndexer.h"
#include "IndexFileLocker.h"


int main() {
    initialize_directory_variables();

    FileListGenerator::get_ndb();

    auto str = GeneralIndexer::read_some_files(queue_produce_file_contents_stdin);
    IndexFileLocker::do_lambda([&] {
        std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
        index_file << *str << "\n";
    });


    if(str) std::cout<<"Suffix: " <<*str <<"\n";
    else {
        std::cerr<<"No suffix received\n";
    }
}




