//
// Created by henry on 2021-04-30.
//

#include <filesystem>
#include "compactor/Compactor.h"
#include <fstream>
#include <iostream>
#include <cassert>
#include "SortedKeysIndex.h"
#include "Serializer.h"
#include "random_b64_gen.h"

namespace fs = std::filesystem;

void Compactor::create_directory(const fs::path &dirpath) {
    fs::create_directory(dirpath);
}

std::tuple<Compactor::ReadState, std::string, std::string> Compactor::read_one_index(std::fstream& stream) {
    auto [state, index_line] = read_and_mark_line(stream);
    auto [state1, filemap_line] = read_and_mark_line(stream);

    assert(state == state1);

    if(state ==ReadState::GOOD) {
        return {ReadState::GOOD, index_line, filemap_line};
    } else if (state == ReadState::PROCESSED_ALREADY) {
        // Recursive call to read again.
        return read_one_index(stream);
    } else/* if (state == ReadState::STREAM_ERROR)*/ {
        return {ReadState::STREAM_ERROR, "", ""};
    }

}

std::pair<Compactor::ReadState, std::string> Compactor::read_and_mark_line(std::fstream& stream) {
    std::string line;
    auto before_read = stream.tellg();

    if(!std::getline(stream, line)) return {Compactor::ReadState::STREAM_ERROR, ""};

    auto after_read = stream.tellg();

    if(line[0] == '#') {
        return {Compactor::ReadState::PROCESSED_ALREADY, ""};
    }
    else {
        stream.seekg(before_read);
        stream<<"#";
        stream.seekg(after_read);
        return {Compactor::ReadState::GOOD, line};
    }
}

void Compactor::compact_directory(const fs::path &path, int max_merge) {
    Compactor::create_directory(path / "previously-compacted");
    auto index_file = std::fstream(path / "index_files", std::ios_base::in | std::ios_base::out);
    index_file.seekg(std::ios_base::beg);

    uint32_t cur_hash = 0, max_doc_id = -1;
    auto master_ssk = SortedKeysIndex();
    auto master_filemap = std::vector<DocIDFilePair>();

    while (max_merge--) {
        auto [read_state, index_p, filemap_p] = read_one_index(index_file);

        if(read_state == ReadState::STREAM_ERROR) break;



        auto cur_processed_file = std::ifstream(index_p, std::ios_base::binary);
        auto cur_processed_filemap = std::ifstream(filemap_p, std::ios_base::binary);
        if (!cur_processed_file || !cur_processed_filemap) {
            std::cout << "couldn't open file " << index_p <<" "<<filemap_p<< "!\n";
        } else {
            std::cout << "Merging file " << index_p << "\n";
            auto cur_processed_ssk = Serializer::read_sorted_keys_index(cur_processed_file);
            auto filemap = Serializer::read_filepairs(cur_processed_filemap);

            for(auto& i : cur_processed_ssk.get_index()) {
                for(auto& j : i.files) {
                    j.document_id += cur_hash;
                    max_doc_id = std::max(max_doc_id, j.document_id);
                }
            }
            for(auto& i : filemap) {
                i.docid += cur_hash;
            }

            cur_hash = max_doc_id+1;
            master_ssk.merge_into(cur_processed_ssk);
            std::copy(filemap.begin(), filemap.end(), std::back_inserter(master_filemap));
        }
    }

    master_ssk.sort_and_group_all();
    auto random_suffix = random_b64_str(5);
    auto master_ssk_path = path / ("master_index" + random_suffix);
    auto filemap_p = path / ("filemap" + random_suffix);
    auto master_ssk_f = std::ofstream(master_ssk_path, std::ios_base::binary);
    auto filemap_f = std::ofstream(filemap_p, std::ios_base::binary);

    Serializer::serialize(master_ssk_f, master_ssk);
    Serializer::serialize(filemap_f, master_filemap);
    index_file.seekg(-1, std::ios_base::end);

    if (index_file.get() != '\n') {
        index_file << '\n'; // check last character is a newline.
    }
//    index_file.clear();
    index_file << master_ssk_path.string() << "\n" << filemap_p.string() << "\n";

}

