//
// Created by henry on 2021-04-29.
//
#include "random_b64_gen.h"
#include "DocIDFilePair.h"
#include <mutex>
#include <thread>
#include <unordered_map>
#include <filesystem>
#include <iostream>
#include <random>
#include <fstream>
#include "Tokenizer.h"
#include "Serializer.h"
#include <execution>
#include "SortedKeysIndex.h"
#include "WordIndexEntry.h"


using SearchResult = std::vector<DocumentPositionPointer>;

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


SortedKeysIndex::SortedKeysIndex(std::vector<WordIndexEntry> index) : index(std::move(index)) {

}

void SortedKeysIndex::reserve_more(std::size_t len) {
    index.reserve(index.size() + len);
}


void SortedKeysIndex::merge_into(SortedKeysIndex &other) {
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

// todo : std::optional returning reference to temporary
const SearchResult *SortedKeysIndex::search_key(const std::string &key) const {
    /*
     * For a given key, get the vector of DocumentPositionPointers for that key. Empty vector if key not found.
     */
    if (auto pos = vector_find(index, key); pos != index.end()) {
        return &(pos->files);
    } else {
        return nullptr;
    }
}

SearchResult SortedKeysIndex::search_keys(const std::vector<std::string> &keys) const {

    std::vector<const SearchResult *> results;
    int cur_min_idx = -1, cur_min_value = 100000000;
    results.reserve(keys.size());
    for (auto &key : keys) {
        if (auto searchresult = search_key(key); searchresult) {
            results.push_back(search_key(key));
            if (results.back()->size() < cur_min_value) {
                cur_min_value = results.back()->size();
                cur_min_idx = results.size() - 1;
            }
        } else {
            return {};
        }
    }

    std::vector<std::vector<DocumentPositionPointer>::const_iterator> result_idx;
    std::transform(results.begin(), results.end(), std::back_inserter(result_idx),
                   [](const auto &elem) { return elem->begin(); });


    SearchResult match_indexes;
    for (auto &i : *results[cur_min_idx]) {
        bool matchall = true;
        for (int a = 0; a < results.size(); a++) {
            auto iter = std::lower_bound(result_idx[a], results[a]->end(), i);
            result_idx[a] = iter;
            if (iter == results[a]->end() || iter->document_id != i.document_id) {
                matchall = false;
                break;
            }
        }

        if (matchall) {
            match_indexes.push_back({i.document_id, i.document_position});
        }
    }

    return match_indexes;
}


void SortedKeysIndex::sort_and_group_all() {
    std::sort(std::execution::par, index.begin(), index.end());
    std::size_t unique_elements = 0;

    auto it = index.begin();

    // For empty vector, index.begin() == index.end().
    while (it != index.end() && it < index.end() - 1 ) {
        auto cur_key = it->key;
        auto next = it + 1;
        it->files.reserve(it->files.size() *4);
        for (; next < index.end() && next->key == cur_key; next++) {
            std::move(next->files.begin(), next->files.end(), std::back_inserter(it->files));
            next->files.clear();
        }
        unique_elements++;
        it = next;
    }
    index.erase(std::remove_if(index.begin(), index.end(), [](const WordIndexEntry &entry) {
        return entry.files.empty(); //if empty, then erase element
    }), index.end());
    for (WordIndexEntry &elem : index) {
        std::sort(elem.files.begin(), elem.files.end());
    }
}


int SortedKeysIndex::index_size() const {
    return index.size();
}

std::vector<WordIndexEntry> &SortedKeysIndex::get_index() {
    return index;
}

