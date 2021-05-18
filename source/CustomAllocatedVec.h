
#ifndef GAME_CUSTOMALLOCATEDVEC_H
#define GAME_CUSTOMALLOCATEDVEC_H

#include <cstring>
#include "ContiguousAllocator.h"
#include <mutex>

template<typename T, int block_size = 7, int num_blocks = 10000>
class CustomAllocatedVec {
    static ContiguousAllocator<T> &get_default_allocator() {
        static ContiguousAllocator<T> def{block_size, num_blocks};
        return def;
    }

    T *block;
    uint32_t total_size, cur_size;
    bool is_heap = false;

public:
    CustomAllocatedVec() {
        block = get_default_allocator().get_new_block();
        cur_size = 0;
        total_size = get_default_allocator().BLOCK_INTERVAL;
    }

    std::vector<T> to_vector() const {
        std::vector<T> out(cur_size);
        std::memcpy(out.data(), block, sizeof(T) * cur_size);
        return out;
    }

    void push_back(T elem) {
        if (cur_size < total_size) {
            block[cur_size] = elem;
            cur_size++;
        } else {
            // Size exceeded. Just not add for now.
            reserve(total_size * 2);
            push_back(std::move(elem));
        }
    }

    T &operator[](uint index) {
        if (index < cur_size) return block[index];
        else {
            throw std::runtime_error("Index exceeded size");
        }
    }

    void clear() {
        cur_size = 0;
        total_size = 0;
    }

    bool empty() const {
        return !cur_size;
    }

    void reserve(int how_many) {
        if (how_many <= total_size && block) return;

        T *heap = new T[how_many];

        if (block) std::memcpy(heap, block, cur_size * sizeof(T));

        free_mem();
        block = heap;
        is_heap = true;
        total_size = how_many;
    }

    CustomAllocatedVec &operator=(CustomAllocatedVec<T, block_size, num_blocks> &&other) {

        this->block = other.block;
        this->total_size = other.total_size;
        this->cur_size = other.cur_size;
        this->is_heap = other.is_heap;
        other.block = nullptr;
        return *this;
    }

    CustomAllocatedVec(CustomAllocatedVec<T, block_size, num_blocks> &&other) {
        operator=(std::move(other));
    }

    CustomAllocatedVec(const CustomAllocatedVec<T, block_size, num_blocks> &other) {
        if (other.is_heap) {
            block = new T[other.total_size];
            is_heap = true;
        } else {
            block = get_default_allocator().get_new_block();
            is_heap = false;
        }
        cur_size = other.cur_size;
        total_size = other.total_size;
        memcpy(block, other.block, cur_size * sizeof(T));
    };

    void free_mem() {
        if (is_heap) {
            delete[]block;
            block = nullptr;
        }
        else if (block != nullptr) {
            get_default_allocator().free_block(block);
            block = nullptr;
        }
    }

    ~CustomAllocatedVec() {
        free_mem();
    }

    std::size_t size() const {
        return cur_size;
    }

    struct iterator;


    iterator begin() {
        return iterator{block};
    }

    iterator end() {
        return iterator{block + cur_size};
    }

    iterator begin() const {
        return iterator{block};
    }

    iterator end() const {
        return iterator{block + cur_size};
    }

    std::size_t size() {
        return cur_size;
    }
};

#include <iterator>

template<typename T, int block_size, int num_blocks>
struct CustomAllocatedVec<T, block_size, num_blocks>::iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = int;
    using pointer = T *;
    using reference = T &;
    using value_type = T;
    T *where;

    bool operator!=(const iterator &other) {
        return where != other.where;
    }

    bool operator==(const iterator &other) {
        return where == other.where;
    }

    value_type *base() {
        return where;
    }

    value_type *operator->() {
        return where;
    }

    iterator &operator++() {
        where++;
        return *this;
    }

    iterator &operator--() {
        where--;
        return *this;
    }

    int operator-(const iterator &other) {
        return where - other.where;
    }

    iterator operator-(int i) {
        return iterator{where - i};
    }

    iterator operator+(int i) {
        return iterator{where + i};
    }

    T &operator*() {
        return *where;
    }

    bool operator<(const iterator &other) {
        return where < other.where;
    }
};


#endif //GAME_CUSTOMALLOCATEDVEC_H
