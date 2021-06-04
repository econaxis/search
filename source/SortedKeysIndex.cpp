#include "DocIDFilePair.h"
#include "SortedKeysIndex.h"


SortedKeysIndex::SortedKeysIndex(std::vector<WordIndexEntry_unsafe> index)  {
    this->index.reserve(index.size());
    for(auto & i : index) {
        std::vector<DocumentPositionPointer> a (i.files.begin(), i.files.end());
        this->index.push_back(WordIndexEntry{i.key, std::move(a)});
    }
}

void SortedKeysIndex::merge_into(SortedKeysIndex &&other) {
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
    for (auto& wie : index) {
        auto prev = 0;
        for(auto& [docid, pos] : wie.files) {
            if (docid* 14344213 + pos == prev) {
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


