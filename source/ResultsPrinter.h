//
// Created by henry on 2021-05-04.
//

#ifndef GAME_RESULTSPRINTER_H
#define GAME_RESULTSPRINTER_H

#include <fstream>
#include "SortedKeysIndexStub.h"
#include "Constants.h"
#include <chrono>
#include <iostream>
#include "TopDocs.h"
#include "DocIDFilePair.h"

namespace ResultsPrinter {
    using namespace std::chrono;

    void print_results(const TopDocs& results, std::vector<DocIDFilePair>& filemap) {
        auto end = results.cend();
        if (results.size() > 10) end = results.cbegin() + 10;
        for(auto i = results.cbegin(); i < end; i++) {
            auto filename = std::find(filemap.begin(), filemap.end(), i->document_id);
            std::cout<<filename->file_name<<" "<<i->document_id<<" "<<i->frequency<<"\n";
        }
    }

//    void print_results(std::vector<MultiSearchResult> &temp1, std::vector<DocIDFilePair> &filepairs);
//
//    void print_results(std::vector<SafeMultiSearchResult> &temp1, std::vector<DocIDFilePair> &filepairs);
};


#endif //GAME_RESULTSPRINTER_H
