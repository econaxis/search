#include <iostream>
#include "DocIDFilePair.h"
#include "SortedKeysIndex.h"


SortedKeysIndex::SortedKeysIndex(std::vector<WordIndexEntry> index) : index(std::move(index)) {
    std::sort(this->index.begin(), this->index.end());
}


using InputIt = std::vector<WordIndexEntry>::iterator;

template<typename Out>
static Out merge_combine(InputIt first1, InputIt last1, InputIt first2, InputIt last2, Out d_first) {
    for (; first1 != last1; ++d_first) {
        if (first2 == last2) {
            return std::copy(first1, last1, d_first);
        }
        if (*first2 < *first1) {
            *d_first = *first2;
            ++first2;
        } else if (*first1 < *first2) {
            *d_first = *first1;
            ++first1;
        } else {
            std::move(first2->files.begin(), first2->files.end(), std::back_inserter(first1->files));
            *d_first = *first1;
            ++first1;
            ++first2;
        }
    }
    return std::copy(first2, last2, d_first);
}


void SortedKeysIndex::merge_into(SortedKeysIndex &&other) {
    assert(std::is_sorted(index.begin(), index.end()));
    assert(std::is_sorted(other.index.begin(), other.index.end()));

    std::vector<WordIndexEntry> newmerged;
    newmerged.resize(other.get_index().size() + index.size());
    auto final = merge_combine(other.index.begin(), other.index.end(), index.begin(), index.end(), newmerged.begin());
    newmerged.resize(final- newmerged.begin());

    assert(std::is_sorted(newmerged.begin(), newmerged.end()));
    other.index.clear();
    index = newmerged;
}


void SortedKeysIndex::sort_and_group_shallow() {
    if (!std::is_sorted(index.begin(), index.end())) {
        throw std::runtime_error("Not sorted. Unexpected state, unsorted or duplicate values found");
    }

    for (int i = 1; i < index.size(); i++) {
        if (index[i] == index[i - 1]) {
            throw std::runtime_error("Ununique. Unexpected state, unsorted or duplicate values found");
        }
    }
    return;

    std::sort(index.begin(), index.end());

    auto it = index.begin();


    // For empty vector, index.begin() == index.end().
    while (it != index.end() && it < index.end() - 1) {
        auto cur_key = it->key;
        auto next = it + 1;
        for (; next < index.end() && next->key == cur_key; next++) {
            std::move(next->files.begin(), next->files.end(), std::back_inserter(it->files));
            next->files.clear();
        }
        it = next;
    }
    index.erase(std::remove_if(index.begin(), index.end(), [](const WordIndexEntry &entry) {
        return entry.files.empty(); //if empty, then erase element
    }), index.end());

}

void SortedKeysIndex::sort_and_group_all() {
    std::for_each(index.begin(), index.end(), [](WordIndexEntry &elem) {
        std::sort(elem.files.begin(), elem.files.end());
    });
}

void SortedKeysIndex::check_dups() {
    for (auto &wie : index) {
        auto prev = 0;
        for (auto&[docid, pos] : wie.files) {
            if (docid * 14344213 + pos == prev) {
                throw std::runtime_error("Duplicate found");
            } else {
                prev = docid * 14344213 + pos;
            }
        }
    }
}

std::vector<WordIndexEntry> &SortedKeysIndex::get_index() {
    return index;
}

const std::vector<WordIndexEntry> &SortedKeysIndex::get_index() const {
    return index;
}


