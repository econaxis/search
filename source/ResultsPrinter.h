
#ifndef GAME_RESULTSPRINTER_H
#define GAME_RESULTSPRINTER_H

#include <fstream>
#include "SortedKeysIndexStub.h"
#include "Constants.h"
#include <chrono>
#include <iostream>
#include "TopDocs.h"
#include "DocIDFilePair.h"
#include <algorithm>

namespace ResultsPrinter {
    using namespace std::chrono;
    namespace fs = std::filesystem;

    std::string highlight_result(fs::path name, uint32_t position) {
        std::ifstream stream(name);
        uint32_t file_len = fs::file_size(name);
        uint32_t before_range = position - 80;
        uint32_t after_range = position + 80;

        before_range = std::clamp(before_range, 0U, file_len);
        after_range = std::clamp(after_range, 0U, file_len);

        std::string word;
        stream.seekg(before_range);
        stream>>word;
//
        std::stringstream out;

        while (stream.tellg() < position) out<<stream.rdbuf()<<" ";
        out<<"**"<<stream.rdbuf()<<"** ";
        while(stream.tellg() < after_range) out<<stream.rdbuf()<<" ";

        return out.str();
    }

    void print_results(const TopDocs& results, std::vector<std::vector<DocIDFilePair>>& filemap) {
        auto end = results.cend();
        if (results.size() > 20) end = results.cbegin() + 20;
        for(auto i = results.cbegin(); i < end; i++) {
            auto filename = std::find(filemap[i->unique_identifier].begin(), filemap[i->unique_identifier].end(), i->document_id);

//            auto highlight = highlight_result(filename, i->)
            std::cout<<filename->file_name<<" "<<i->document_id<<" "<<i->frequency<<"\n";
        }
    }

//    void print_results(std::vector<MultiSearchResult> &temp1, std::vector<DocIDFilePair> &filepairs);
//
//    void print_results(std::vector<SafeMultiSearchResult> &temp1, std::vector<DocIDFilePair> &filepairs);
};


#endif //GAME_RESULTSPRINTER_H
