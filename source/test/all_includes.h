//
// Created by henry on 2021-06-30.
//

#ifndef GAME_ALL_INCLUDES_H
#define GAME_ALL_INCLUDES_H

#define LOOP_ITERS 800

#include "rust-interface.h"
#include "c_abi.h"
#include "SortedKeysIndex.h"
#include "dict_strings.h"
#include "IndexFileLocker.h"
#include "logger.h"
#include "TermsListSearcher.h"
#include "Base26Num.h"
#include "SortedKeysIndexStub.h"
#include "compactor/Compactor.h"
#include "Tokenizer.h"
#include "DocIDFilePair.h"
#include "FileListGenerator.h"
#include "SyncedQueue.h"
#include "WordIndexEntry.h"
#include "DocumentsMatcher.h"
#include "DocumentPositionPointer.h"
#include "DocumentFrequency.h"
#include "ContiguousAllocator.h"
#include "TopDocs.h"
#include "FPStub.h"
#include "DocumentsTier.h"
#include "Serializer.h"
#include "random_b64_gen.h"
#include "GeneralIndexer.h"
#include "Constants.h"
#include "StdinIndexer.h"
#include "PositionsSearcher.h"

namespace {
    [[maybe_unused]] unsigned long rand() {
        static std::random_device dev;
        static std::mt19937 rng(dev());
        static std::uniform_int_distribution<unsigned long> dist6;

        return dist6(rng);
    }
}

inline void repeat(int num, auto call) {
    for (int i = 0; i < num; i++) {
        call(i);
    }
}

inline std::string random_alphabetic_string(int len = 5) {
    static constexpr std::string_view alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string a(len, ' ');
    for (int i = 0; i < len; i++) a[i] = alphabet[::rand() % alphabet.size()];
    return a;
}

inline std::string generate_words(int num = 100) {
    std::ostringstream res;

    repeat(num, [&](int _) {
        auto word_size = ::rand() % 12 + 2;
        res << random_alphabetic_string(word_size)<<" ";
    });

    return res.str();
}

inline std::string do_index_custom(auto callable) {
    std::stringstream fakecin;
    std::cin.rdbuf(fakecin.rdbuf());
    std::array<std::string, LOOP_ITERS> filenames, files;
    repeat(LOOP_ITERS, [&](int i) {
        filenames[i] = random_b64_str(10);
        files[i] = fmt::format("{}", callable(i));
        fmt::print(fakecin, "filename {} /endfilename file {} /endfile ", filenames[i], files[i]);
    });

    fmt::print(fakecin, "/endindexing\n");

    auto suffix = GeneralIndexer::read_some_files(queue_produce_file_contents_stdin);
    return *suffix;
}

inline std::string do_index(std::string must_include = "empty") {
    auto call = [&](int _) {
        return fmt::format("{} {} {}", generate_words(100), must_include, generate_words(100));
    };

    return do_index_custom(call);
}


#endif //GAME_ALL_INCLUDES_H
