//
// Created by henry on 2021-05-05.
//

#ifndef GAME_CONTIGUOUSALLOCATOR_H
#define GAME_CONTIGUOUSALLOCATOR_H

#include <memory>
#include <iostream>
#include <stack>
#include "SortedKeysIndex.h"

template<typename T>
class ContiguousAllocator {
    std::size_t current_allocated;
    std::unique_ptr<T[]> memory;
    std::stack<T *> free;
public:
    static constexpr int BLOCK_INTERVAL = 4; // Each vector should have 4 positions.
    static constexpr int TOTAL_BLOCKS = 1000000;
    static constexpr int TOTAL_SIZE = BLOCK_INTERVAL * TOTAL_BLOCKS;

    ContiguousAllocator() {
        memory = std::make_unique<T[]>(TOTAL_SIZE);
        std::cout << "Allocator created at " << memory.get() << "\n";
        current_allocated = 0;
    }

    void free_block(T *what) {
        free.push(what);
    }

    T *get_new_block() {
        if (current_allocated >= TOTAL_SIZE) {
            if (!free.empty()) {
                auto *temp = free.top();
                free.pop();
                return temp;
            } else {
//                throw std::runtime_error("Memory of contiguous allocator exceeded");
                return nullptr;
            }

        }
        T *block_loc = &(memory.get()[current_allocated]);
        current_allocated += BLOCK_INTERVAL;
        return block_loc;
    }
    void clear() {
        while(!free.empty()) free.pop();
        current_allocated = 0;
    }

    ~ContiguousAllocator() {
    }
};

inline ContiguousAllocator<PairUint32>& get_default_allocator(bool should_clear = false) {
    static ContiguousAllocator<PairUint32> allocator{};

    if(should_clear) {
        allocator.clear();
    }
    return allocator;
}
#endif //GAME_CONTIGUOUSALLOCATOR_H
