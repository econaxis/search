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
    std::atomic_bool done_flag = false;
    std::queue<value_type> queue;
    std::vector<DocIDFilePair> filepairs;
    mutable std::mutex mutex;
    std::condition_variable cv;

    std::lock_guard<std::mutex> get_lock() const {
        return std::lock_guard(mutex);
    }

    uint32_t size() const {
        return queue.size();
    };

    void push(value_type elem) {
        auto l = get_lock();
        queue.push(std::move(elem));
        cv.notify_one();
    }

    template<typename T>
    void push_multi(T begin, T end) {
        auto l = get_lock();

        for (auto i = begin; i < end; i++) {
            queue.push(std::move(*i));
        }

        cv.notify_one();
    }

    std::pair<std::string, DocIDFilePair> pop() {
        using namespace std::chrono_literals;
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] {
            return this->size() || (done_flag && !this->size());
        });

        if(done_flag && !this->size()) return {"EMPTY", {0, "EMPTY"}};

        auto b = queue.front();
        queue.pop();

        filepairs.push_back(b.second);
        cv.notify_one();
        return b;
    }

    template<typename Callable>
    void wait_for(Callable c) {
        std::unique_lock lock(mutex);
        cv.wait(lock, c);
    }
};

#endif //GAME_SYNCEDQUEUE_H
