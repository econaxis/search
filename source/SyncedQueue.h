//
// Created by henry on 2021-06-23.
//

#ifndef GAME_SYNCEDQUEUE_H
#define GAME_SYNCEDQUEUE_H

#include <queue>
#include "DocIDFilePair.h"
#include <string>
#include <atomic>
#include <condition_variable>

struct SyncedQueue {
    using value_type = std::pair<std::string, DocIDFilePair>;

    // A queue might be currently starved for input, then we want to keep waiting
    // A queue might be done with input, but still have items to be processed.
    // A queue might be done with input and have no items to be processed.
    // This flag keeps track of whether there is more input to follow, for consumers to wait
    // even if the queue is currently empty.
    std::atomic_bool input_is_done = false;
    std::queue<value_type> queue;
    std::vector<DocIDFilePair> filepairs;
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::lock_guard<std::mutex> get_lock() const;

    std::size_t size() const;

    void push(value_type elem);

    template<typename T>
    void push_multi(T begin, T end);

    std::pair<std::string, DocIDFilePair> pop();

    template<typename Callable>
    void wait_for(Callable c);
};

#endif //GAME_SYNCEDQUEUE_H
