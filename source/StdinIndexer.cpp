//
// Created by henry on 2021-06-23.
//

#include <iostream>
#include <atomic>
#include <iostream>
#include "SyncedQueue.h"
#include "GeneralIndexer.h"
#include "Constants.h"
#include "IndexFileLocker.h"

void queue_produce_file_contents_stdin(SyncedQueue &contents);

int main() {
    initialize_directory_variables();

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


void queue_produce_file_contents_stdin(SyncedQueue &contents) {
    std::vector<SyncedQueue::value_type> thread_local_holder;
    uint docid = 0;
    while (true) {
        std::string file, filename;
        file.reserve(10000);
        std::string word;
        std::cin >> word;

        if (word == "/endindexing") {
            contents.push_multi(thread_local_holder.begin(), thread_local_holder.end());
            thread_local_holder.clear();
            break;
        }

        // Filename
        if (word != "filename") {
            std::cerr << ("Word: " + word + " wrong");
            exit(-1);
        }
        std::cin >> word;

        while (word != "/endfilename") {
            filename.append(word);
            std::cin >> word;
        }

        // File
        std::cin >> word;
        if (word != "file") {
            std::cerr << ("Word: " + word + " wrong");
            exit(-1);
        }
        std::cin >> word;
        while (word != "/endfile") {
            file.append(word + " ");
            std::cin >> word;
        }

        thread_local_holder.emplace_back(std::move(file), DocIDFilePair{docid++, filename});

        if (thread_local_holder.size() >= 20) {
            contents.push_multi(thread_local_holder.begin(), thread_local_holder.end());
            thread_local_holder.clear();
            std::cout << "Progress: " << docid - contents.size() << "\n";
        }
    }
    contents.done_flag = true;
    contents.cv.notify_all();
}

