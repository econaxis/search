#include "ResultsPrinter.h"
#include "Tokenizer.h"
#include "GeneralIndexer.h"
#include "compactor/Compactor.h"
#include "dict_strings.h"
#include "random_b64_gen.h"
#include "SortedKeysIndexStub.h"


namespace fs = std::filesystem;


void profile_indexing(std::vector<SortedKeysIndexStub> &index, std::vector<std::vector<DocIDFilePair>> &filemap,
                      char *argv[]) {
    using namespace std::chrono;

    int NUM_SEARCHES = std::atoi(argv[1]);
    std::uniform_int_distribution<uint> dist(0, 5460); // ASCII table codes for normal characters.
    auto t1 = high_resolution_clock::now();
    for (int i = 0; i < NUM_SEARCHES; i++) {
        auto temp = (std::string) strings[dist(randgen())];
        auto temp1 = (std::string) strings[dist(randgen())];
        auto temp2 = (std::string) strings[dist(randgen())];
        auto temp3 = (std::string) strings[dist(randgen())];

        Tokenizer::clean_token_to_index(temp);
        Tokenizer::clean_token_to_index(temp1);
        Tokenizer::clean_token_to_index(temp2);
        Tokenizer::clean_token_to_index(temp3);

        std::vector<std::string> query{temp, temp1, temp2, temp3};
        TopDocs result;
        if (temp.size() && temp1.size() && temp2.size() && temp3.size()) {
            result = SortedKeysIndexStub::collection_merge_search(index, query);
        }
//        ResultsPrinter::print_results(result, filemap, query);

        if (i % 300 == 0)
            std::cout << "Matched " << result.size() << " files for " << temp1 << " " << temp << " "
                      << i * 100 / NUM_SEARCHES << "%\n" << std::flush;
    }
    auto time = high_resolution_clock::now() - t1;
    auto timedbl = duration_cast<milliseconds>(time).count();
    std::cout << "Time for " << NUM_SEARCHES << " queries: " << timedbl << "\n";

    exit(0);
}

std::pair<std::vector<SortedKeysIndexStub>, std::vector<std::vector<DocIDFilePair>>>
load_all_indices() {
    std::ifstream index_file(data_files_dir / "indices" / "index_files", std::ios_base::in);

    if (!index_file) {
        std::cerr << "Index file doesn't exist at path: " << data_files_dir / "indices" / "index_files" << "\n";
        return {};
    }

    std::vector<std::vector<DocIDFilePair>> filepairs;
    std::vector<SortedKeysIndexStub> indices;

    while (true) {
        auto[statedb, line] = Compactor::read_line(index_file);
        if (statedb != Compactor::ReadState::GOOD) break;

        std::cout << "Used database file: " << line << "\n";

        std::ifstream filepairstream(indice_files_dir / ("filemap-" + line), std::ios_base::binary);
//        auto temp = Serializer::read_filepairs(filepairstream);
//        temp.clear();
//        filepairs.push_back(temp);
        indices.emplace_back(indice_files_dir / ("frequencies-" + line),
                             indice_files_dir / ("terms-" + line));

        if (indices.size() >= 10000) break;
    }


    return {std::move(indices), std::move(filepairs)};
}

#include <immintrin.h>
#include <chrono>

[[maybe_unused]] static unsigned int measure() {
    using namespace std::chrono;
    static auto lasttime = high_resolution_clock::now();
    unsigned int ret = duration_cast<nanoseconds>(high_resolution_clock::now() - lasttime).count();
    lasttime = high_resolution_clock::now();
    return ret;
}

void test() {
    auto t = std::unique_ptr<DocumentPositionPointer_v2[]>(new (std::align_val_t(64)) DocumentPositionPointer_v2[500000000]);
    auto titer = t.get();

    for(;titer - t.get() < 500000000; titer++) {
        *titer = DocumentPositionPointer_v2{static_cast<uint32_t>((titer - t.get())%65500), 17};
    }

    std::vector<__m256i> buf16(50000000);

    auto *cur_iterator = buf16.data();
    auto beg = (uint32_t *) t.get();
    auto end = (uint32_t *) t.get() + 500000000;

    uint32_t selector = 0x0000FFFF;
    __m256i select = _mm256_set1_epi32(selector);
    measure();
    for (auto i = beg; i + 32 < end; i += 32) {
        __m256i first = _mm256_load_si256((__m256i *) i);
        __m256i second = _mm256_load_si256((__m256i *) (i + 8));
        __m256i third = _mm256_load_si256((__m256i *) (i + 16));
        __m256i fourth = _mm256_load_si256((__m256i *) (i + 24));
        __m256i packed = _mm256_packus_epi32(first, second);
        packed = _mm256_permute4x64_epi64(packed, 0b11011000);
        packed = _mm256_and_si256(packed, select);


        __m256i packed1 = _mm256_packus_epi32(third, fourth);
        packed1 = _mm256_permute4x64_epi64(packed1, 0b11011000);
        packed1 = _mm256_and_si256(packed1, select);



        __m256i joined_all = _mm256_packus_epi32(packed, packed1);

        __m256i reordered = _mm256_permute4x64_epi64(joined_all, 0b11011000);
        _mm256_store_si256((__m256i *) cur_iterator, reordered);
        cur_iterator++;
    }

    int timea = measure() / 1000;
    cur_iterator = buf16.data();
    measure();
//    for(auto &p : t) {
//        *cur_iterator = (uint16_t) p.document_id;
//        cur_iterator++;
//    }
    int timeb = measure() / 1000;

    std::cout<<timea<<" "<<timeb<<"\n";
    bool dummy = false;
}


int main(int argc, char *argv[]) {
    test();
    using namespace std::chrono;
    initialize_directory_variables();


    if (argc == 1) {
        GeneralIndexer::register_atexit_handler();
        while (GeneralIndexer::read_some_files() != 0) {};
        return 1;
    };


    auto[indices, filemap] = load_all_indices();
    profile_indexing(indices, filemap, argv);
    std::string inp_line;
    std::cout << "Ready\n>> ";

    while (std::getline(std::cin, inp_line)) {
        if (inp_line == ".exit") break;
        std::vector<std::string> terms;
        auto ss = std::istringstream(inp_line);
        std::string word;
        while (ss >> word) {
            std::string s(word);
            if (Tokenizer::clean_token_to_index(s)) {
                std::cout << s << " ";
                terms.emplace_back(s);
            }
        }
        auto temp1 = SortedKeysIndexStub::collection_merge_search(indices, terms);
//        ResultsPrinter::print_results(temp1, filemap, terms);
    }
}
