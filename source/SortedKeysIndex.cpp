#include "DocIDFilePair.h"
#include "SortedKeysIndex.h"

std::vector<WordIndexEntry>::iterator vector_find(std::vector<WordIndexEntry> &vec, const std::string &key) {
    auto it = std::lower_bound(vec.begin(), vec.end(), key, [](const auto &_vec_elem, const auto &_key) {
        return _vec_elem.key < _key;
    });

    if (it->key != key) {
        return vec.end();
    } else return it;
}

std::vector<WordIndexEntry>::const_iterator
vector_find(const std::vector<WordIndexEntry> &vec, const std::string &key) {
    auto it = std::lower_bound(vec.begin(), vec.end(), key, [](const auto &_vec_elem, const auto &_key) {
        return _vec_elem.key < _key;
    });

    if (it == vec.end() || it->key != key) {
        return vec.cend();
    } else return it;
}


SortedKeysIndex::SortedKeysIndex(std::vector<WordIndexEntry_unsafe> index)  {
    this->index.reserve(index.size());
    for(auto & i : index) {
        std::vector<DocumentPositionPointer> a (i.files.begin(), i.files.end());
        this->index.push_back(WordIndexEntry{i.key, std::move(a)});
    }
}


void SortedKeysIndex::merge_into(SortedKeysIndex &&other) {
//    auto &this_index = this->index;
//    auto &other_index = other.index;
//    auto similar_keys = std::vector<WordIndexEntry>();
//    std::set_intersection(this_index.begin(), this_index.end(), other_index.begin(),
//                          other_index.end(), std::back_inserter(similar_keys));
//
//    // For all those with similar keys, we have to merge_into manually
//    for (const auto &entry : similar_keys) {
//        auto it = vector_find(this_index, entry.key);
//        const auto other_it = vector_find(other_index, entry.key);
//        std::copy(other_it->files.begin(), other_it->files.end(), std::back_inserter(it));
//
//        other_index.erase(other_it);
//    }

//    // Use default merge for all those different
//    std::vector<WordIndexEntry> new_index;
//    new_index.reserve(index.size() + this->index.size());
//
//    std::merge(index.begin(), index.end(), this->index.begin(), this->index.end(), std::back_inserter(new_index));
//    this->index = std::move(new_index);
    std::move(other.index.begin(), other.index.end(), std::back_inserter(index));
}


void SortedKeysIndex::sort_and_group_shallow() {
    std::sort(index.begin(), index.end());

    auto it = index.begin();

    // For empty vector, index.begin() == index.end().
    while (it != index.end() && it < index.end() - 1) {
        auto cur_key = it->key;
        auto next = it + 1;
        for (; next < index.end() && next->key == cur_key; next++) {
            for (auto& i : next->files) {
                it->files.push_back(i);
            }
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

std::vector<WordIndexEntry> &SortedKeysIndex::get_index() {
    return index;
}


