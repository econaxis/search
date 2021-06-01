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

    int debugtime = 0;
    std::ofstream debugfilebuffer("/tmp/debug.txt");

    std::optional<uint32_t> search(std::string &text, std::string &needle, uint32_t start = 0) {
        auto start_iter = text.begin() + start;
        auto find = std::search(start_iter, text.end(),
                                std::boyer_moore_horspool_searcher(needle.begin(), needle.end()));

        if (find == text.end()) return std::nullopt;

        return find - text.begin();
    }

    std::string_view slice(const std::string_view &other, int beg, int end) {
        return other.substr(beg, end - beg);
    }

    std::string highlight_result(std::string &str, std::string term) {


        auto position_option = search(str, term);

        if (!position_option) return "";

        int position = position_option.value();
        int end_pos = position + term.size();

        int before_range = position - 20;
        int after_range = end_pos + 20;

        before_range = std::clamp(before_range, 0, (int) str.size());
        after_range = std::clamp(after_range, 0, (int) str.size());

        std::string word;
        std::stringstream out;

        auto strview = std::string_view(str);

        std::string ret = "";
        ret.append(slice(strview, before_range, position));
        ret.append("**");
        ret.append(slice(strview, position, end_pos));
        ret.append("**");
        ret.append(slice(strview, end_pos, after_range));

        return ret;
    }
/*
    void print_results(const TopDocs &results, std::vector<std::vector<DocIDFilePair>> &filemap,
                       std::vector<std::string> &terms) {
        auto start = high_resolution_clock::now();
        int num_iter = std::min(50UL, results.size());
        for (auto i = results.end() - num_iter; i != results.end(); ++i) {
            auto filename = std::find(filemap[i->unique_identifier].begin(), filemap[i->unique_identifier].end(),
                                      i->document_id);

            if (filename == filemap[i->unique_identifier].end()) {
                std::cout << i->document_id << " " << i->unique_identifier << " not found\n";
                return;
            }

            std::ifstream stream(data_files_dir / "data" / filename->file_name);
            std::stringstream buf;
            buf << stream.rdbuf();

            auto str = buf.str();

            for (char &c : str) c = std::toupper(c);

            debugfilebuffer << "<<<<< " << filename->file_name << " >>>>>>\n";
            for (auto &t : terms) {
                debugfilebuffer << highlight_result(str, t) << "\n";
            }
            debugfilebuffer << "\n";

        }
        debugtime += duration_cast<milliseconds>(high_resolution_clock::now() - start).count();
    }
*/
 };


#endif //GAME_RESULTSPRINTER_H
