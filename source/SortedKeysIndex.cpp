//
// Created by henry on 2021-04-29.
//
#include "DocIDFilePair.h"
#include <thread>
#include <execution>
#include <map>
#include "SortedKeysIndex.h"
#include "DocumentsMatcher.h"
#include "WordIndexEntry.h"

#include "ContiguousAllocator.h"

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


SortedKeysIndex::SortedKeysIndex(std::vector<WordIndexEntry> index) : index(std::move(index)) {}


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
    index.reserve(index.size() + other.index.size());
    std::move(other.index.begin(), other.index.end(), std::back_inserter(index));
}


std::optional<const SearchResult *>
SortedKeysIndex::search_key(const std::string &key) const {
    /*
     * For a given key, get the vector of DocumentPositionPointers for that key. Empty vector if key not found.
     */
    if (auto pos = vector_find(index, key); pos != index.end()) {
        return &(pos->files);
    } else {
        return std::nullopt;
    }
}


std::vector<SafeMultiSearchResult>
SortedKeysIndex::search_keys(const std::vector<std::string> &keys, std::string type) const {

    std::vector<const SearchResult *> results;
    std::vector<std::string> result_terms;

    SearchResult empty_result_variable; // Since we have pointers, just make an empty stack variable representing empty array.
    // This variable will never outlive the results vector.
    results.reserve(keys.size());
    for (auto &key : keys) {
        if (auto searchresult = search_key(key); searchresult) {
            results.push_back(*searchresult);
        } else {
            results.push_back(&empty_result_variable);
        }
        result_terms.push_back(key);
    }
    if (type == "OR") return DocumentsMatcher::OR(results, result_terms);
    if (type == "AND") return DocumentsMatcher::AND(results, result_terms);
    else {
        throw std::runtime_error("type must be 'OR' or 'AND'");
    }
}


void SortedKeysIndex::sort_and_group_shallow() {
    std::sort(std::execution::par, index.begin(), index.end());

    auto it = index.begin();

    // For empty vector, index.begin() == index.end().
    while (it != index.end() && it < index.end() - 1) {
        auto cur_key = it->key;
        auto next = it + 1;
        it->files.reserve(it->files.size() * 4);
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
    for (WordIndexEntry &elem : index) {
        std::sort(elem.files.begin(), elem.files.end());
    }
}

std::vector<WordIndexEntry> &SortedKeysIndex::get_index() {
    return index;
}


MultiSearchResult::MultiSearchResult(uint32_t docid, uint32_t score,
                                     PairUint32 init_pos) : docid(docid),
                                                            score(score) {
    diff++;
    positions = get_default_allocator().get_new_block();
    insert_position(init_pos);
}


MultiSearchResult::MultiSearchResult(uint32_t docid, uint32_t score) : docid(docid),
                                                                       score(score) {
    diff++;
    positions = get_default_allocator().get_new_block();
}


MultiSearchResult::~MultiSearchResult() {
    diff--;
    if (positions != nullptr) {
        get_default_allocator().free_block(positions);
    }
}

bool MultiSearchResult::insert_position(PairUint32 elem) {
    if (cur_index >= ContiguousAllocator<PairUint32>::BLOCK_INTERVAL) return false;
    positions[cur_index++] = elem;
    return true;
}

MultiSearchResult &MultiSearchResult::operator=(MultiSearchResult &&other) noexcept {
    diff++;
    docid = other.docid;
    score = other.score;
    positions = other.positions;
    cur_index = other.cur_index;
    moved_from = false;
    other.moved_from = true;
    other.positions = nullptr;
    return *this;
}


SafeMultiSearchResult::SafeMultiSearchResult(std::size_t suggested) {
    positions.reserve(suggested);
}
