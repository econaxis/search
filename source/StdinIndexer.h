//
// Created by henry on 2021-06-23.
//

#ifndef GAME_STDININDEXER_H
#define GAME_STDININDEXER_H

#include "SyncedQueue.h"

inline void queue_produce_file_contents_stdin(SyncedQueue &contents) {
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
            throw std::runtime_error ("Word: " + word + " wrong");
        }
        std::cin >> word;

        while (word != "/endfilename") {
            filename.append(word);
            std::cin >> word;
        }

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

        // Erase the trailing whitespace
        file.erase(file.end() -1 );
        thread_local_holder.emplace_back(std::move(file), DocIDFilePair{docid++, filename});

        if (thread_local_holder.size() >= 50) {
            contents.push_multi(thread_local_holder.begin(), thread_local_holder.end());
            thread_local_holder.clear();

#ifdef STDININDEXER_PRINT_PROGRESS
            std::cout << "Progress: " << docid - contents.size() << "\r";
#endif
        }
    }
    contents.input_is_done = true;
    contents.cv.notify_all();
}

#endif //GAME_STDININDEXER_H
