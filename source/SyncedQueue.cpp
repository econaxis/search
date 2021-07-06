//
// Created by henry on 2021-06-23.
//

#include "SyncedQueue.h"

std::lock_guard<std::mutex> SyncedQueue::get_lock() const {
    return std::lock_guard(mutex);
}

template<typename Callable>
void SyncedQueue::wait_for(Callable c) {
    std::unique_lock lock(mutex);
    cv.wait(lock, c);
}

std::pair<std::string, DocIDFilePair> SyncedQueue::pop() {
    using namespace std::chrono_literals;
    std::unique_lock lock(mutex);
    cv.wait(lock, [&] {
        return this->size() || (input_is_done && !this->size());
    });

    if(input_is_done && !this->size()) return {"EMPTY", {0, "EMPTY"}};

    auto b = queue.front();
    queue.pop();

    filepairs.push_back(b.second);
    cv.notify_one();
    return b;
}

template<typename T>
void SyncedQueue::push_multi(T begin, T end) {
    auto l = get_lock();

    for (auto i = begin; i < end; i++) {
        queue.push(std::move(*i));
    }

    cv.notify_one();
}

// Explicitly instantiate template definition
template void SyncedQueue::push_multi(std::vector<SyncedQueue::value_type>::iterator begin, std::vector<SyncedQueue::value_type>::iterator end);

void SyncedQueue::push(SyncedQueue::value_type elem) {
    auto l = get_lock();
    queue.push(std::move(elem));
    cv.notify_one();
}

std::size_t SyncedQueue::size() const {
    return queue.size();
}
