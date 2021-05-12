
#ifndef GAME_CONTIGUOUSALLOCATOR_H
#define GAME_CONTIGUOUSALLOCATOR_H

#include <memory>
#include <iostream>
#include <stack>
#include <queue>

template<typename T>
class ContiguousAllocator {
    std::size_t current_allocated;
    std::unique_ptr<T[]> memory;

    std::stack<T *> free;
public:
    static constexpr int BLOCK_INTERVAL = 2; // Each vector should have how many positions?
    static constexpr int TOTAL_BLOCKS = 1000000; // How many vectors needed?
    static constexpr int TOTAL_SIZE = BLOCK_INTERVAL * TOTAL_BLOCKS;

    ContiguousAllocator() {
        memory = std::make_unique<T[]>(TOTAL_SIZE);
        current_allocated = 0;
    }

    void free_block(T *what) {
        free.push(what);
    }

    T *get_new_block() {
        if (!free.empty()) {
            T *temp = free.top();
            if (!free.empty()) free.pop();
            return temp;
        } else if (current_allocated < TOTAL_SIZE) {
            T *block_loc = &(memory.get()[current_allocated]);
            current_allocated += BLOCK_INTERVAL;
            return block_loc;
        } else {
            throw std::runtime_error("Memory of contiguous allocator exceeded");
//            return nullptr;
        }
    }


    void clear() {
        while (!free.empty()) free.pop();
        current_allocated = 0;
    }

    T *allocate_on_heap(int how_many) {
        T *heap = new T[how_many];
        return heap;
    }


};

#endif //GAME_CONTIGUOUSALLOCATOR_H
