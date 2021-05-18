
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
    int BLOCK_INTERVAL; // Each vector should have how many positions?
    int TOTAL_BLOCKS; // How many vectors needed?

    ContiguousAllocator(int block_interval = 4, int total_blocks = 300000): BLOCK_INTERVAL(block_interval), TOTAL_BLOCKS(total_blocks) {
        memory = std::make_unique<T[]>(BLOCK_INTERVAL * TOTAL_BLOCKS);
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
        } else if (current_allocated < BLOCK_INTERVAL * TOTAL_BLOCKS) {
            T *block_loc = &(memory.get()[current_allocated]);
            current_allocated += BLOCK_INTERVAL;
            return block_loc;
        } else {
            throw std::runtime_error("Mem contiguous alloc. exceeded");
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
