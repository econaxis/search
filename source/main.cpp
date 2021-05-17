#include "Serializer.h"
#include "ResultsPrinter.h"
#include "Tokenizer.h"
#include "GeneralIndexer.h"
#include "compactor/Compactor.h"
#include "dict_strings.h"
#include "random_b64_gen.h"
#include "SortedKeysIndexStub.h"


namespace fs = std::filesystem;


void profile_indexing(std::vector<SortedKeysIndexStub> &index) {
    using namespace std::chrono;

    constexpr int NUM_SEARCHES = 100000;
    std::uniform_int_distribution<uint> dist(0, 514); // ASCII table codes for normal characters.
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
        if (temp.size() && temp1.size() && temp2.size() && temp3.size()){
            result = SortedKeysIndexStub::collection_merge_search(index, query);
//            result = index[0].search_many_terms(query);
        }

        if (i % 1000 == 0)
            std::cout << "Matched " << result.size() << " files for " << temp1 << " " << temp << " "
                      << i * 100 / NUM_SEARCHES << "%\r"<<std::flush;
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
//        filepairs.push_back(temp);
        indices.emplace_back(indice_files_dir / ("frequencies-" + line),
                                    indice_files_dir / ("terms-" + line));

        if(indices.size() > 3) break;
    }


    // DEBUG - clear filepairs, we don't need it.
    filepairs.clear();
    return {std::move(indices), std::move(filepairs)};
}


void test() {
    std::ifstream f("/mnt/nfs/extra/data-files/data/Jerry%27s%20Girls");
    std::string buf (100000, ' ');
    f.read(buf.data(), 100000);
    buf.erase(f.gcount(), buf.size() - f.gcount());

    auto test = Tokenizer::index_string_file(buf, 1);
    SortedKeysIndex ind(test);
    ind.sort_and_group_shallow();
    ind.sort_and_group_all();
    auto deb = ind.get_index()[203].get_frequencies_vector();
}

int main(int argc, char *argv[]) {
    using namespace std::chrono;
    initialize_directory_variables();


    if (argc == 1) {
        GeneralIndexer::register_atexit_handler();
        while (GeneralIndexer::read_some_files() != 0) {};
        return 1;
    };


    auto [indices, filemap] = load_all_indices();
    profile_indexing(indices);

    std::string inp_line;
    std::cout << "Ready\n>> ";

    auto &output_stream = std::cout;
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
        ResultsPrinter::print_results(temp1, filemap);
    }
}
