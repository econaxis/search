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
        auto temp4 = (std::string) strings[dist(randgen())];

        Tokenizer::clean_token_to_index(temp);
        Tokenizer::clean_token_to_index(temp1);
        Tokenizer::clean_token_to_index(temp2);
        Tokenizer::clean_token_to_index(temp3);
        Tokenizer::clean_token_to_index(temp4);

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

        indices.emplace_back(line);

        if (indices.size() >= 3) break;
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
    constexpr int numelem = 500000;
    auto t = std::unique_ptr<DocumentPositionPointer_v2[]>(new (std::align_val_t(64)) DocumentPositionPointer_v2[numelem]);
    auto t32 = std::unique_ptr<uint16_t []>(new (std::align_val_t(64)) uint16_t[numelem]);
    auto titer = t.get();
    auto titer32 = t32.get();


    for(;titer - t.get() < numelem; titer++) {
        *titer = DocumentPositionPointer_v2{static_cast<uint32_t>((titer - t.get())%65500), 17};
        *titer32 = static_cast<uint16_t>((titer - t.get())%65500);
        titer32++;
    }



//    auto *cur_iterator = buf16.data();
//    auto beg = (uint32_t *) t.get();
//    auto end = (uint32_t *)(t.get() + numelem );


//    uint32_t selector = 0x0000FFFF;
//    __m256i select = _mm256_set1_epi32(selector);
//    measure();
//    for (auto i = beg; i + 32 < end; i += 32) {
//        __m256i first = _mm256_load_si256((__m256i *) i);
//        __m256i second = _mm256_load_si256((__m256i *) (i + 8));
//        __m256i third = _mm256_load_si256((__m256i *) (i + 16));
//        __m256i fourth = _mm256_load_si256((__m256i *) (i + 24));
//        __m256i packed = _mm256_packus_epi32(first, second);
//        packed = _mm256_permute4x64_epi64(packed, 0b11011000);
//        packed = _mm256_and_si256(packed, select);
//
//
//        __m256i packed1 = _mm256_packus_epi32(third, fourth);
//        packed1 = _mm256_permute4x64_epi64(packed1, 0b11011000);
//        packed1 = _mm256_and_si256(packed1, select);
//
//
//
//        __m256i joined_all = _mm256_packus_epi32(packed, packed1);
//
//        __m256i reordered = _mm256_permute4x64_epi64(joined_all, 0b11011000);
//        _mm256_storeu_si256((__m256i *) cur_iterator, reordered);
//        cur_iterator+=16;
//    }

//    for(auto &p : t) {
//        *cur_iterator = (uint16_t) p.document_id;
//        cur_iterator++;
//    }

    int counter1 = 0, counter2 = 0;
    measure();
    for(int i =0; i < 100000; i++) {
        auto a  = std::upper_bound(t.get(), t.get() + numelem, 18320,[&](auto& t1, auto& t2) {
            counter1++;
            return t1 < t2.document_id;
        }) - 1;
        if (a->document_id != 18320) {
            throw std::runtime_error("fdsa");
        }
    }
    std::cout<<measure()<<"\n";
    measure();
    for(int i =0; i < 100000; i++) {
        auto a = std::upper_bound(t32.get(), t32.get() + numelem, 18320) - 1;
        if (*a != 18320) {
            throw std::runtime_error("ffdsadsa");
        }
    }
    int b = measure();
    std::cout<<b<<"\n"<<counter1<<" "<<counter2<<"\n";
}

std::string file = "    <title>Spurius Maelius</title>\n"
                   "    <ns>0</ns>\n"
                   "    <id>2978110</id>\n"
                   "    <revision>\n"
                   "      <id>994986955</id>\n"
                   "      <parentid>983171579</parentid>\n"
                   "      <timestamp>2020-12-18T16:15:26Z</timestamp>\n"
                   "      <contributor>\n"
                   "        <username>Avilich</username>\n"
                   "        <id>36246437</id>\n"
                   "      </contributor>\n"
                   "      <comment>inadequate link</comment>\n"
                   "      <model>wikitext</model>\n"
                   "      <format>text/x-wiki</format>\n"
                   "      <text bytes=\"2387\" xml:space=\"preserve\">{{short description|Wealthy Roman plebeian}}\n"
                   "'''Spurius Maelius''' (died 439 BC) was a wealthy [[Ancient Rome|Roman]] [[plebeian]] who was slain because he was suspected of intending to make himself king.&lt;ref name=&quot;Livy1881&quot;&gt;{{cite book|author=Livy|title=The History of Rome|url=https://books.google.com/books?id=p8w_AAAAYAAJ&amp;pg=PA293|year=1881|publisher=Harper &amp; Brothers|pages=293–}}&lt;/ref&gt;\n"
                   "\n"
                   "==Biography==\n"
                   "During a severe famine, Spurius Maelius bought up a large amount of [[grain supply to the city of Rome|wheat]] and sold it at a low price to the people of Rome. According to [[Livy]], Lucius Minucius Augurinus, the [[Patrician (ancient Rome)|patrician]] ''praefectus annonae'' (president of the market), thereupon accused him of collecting arms in Maelius' house, and that he was holding secret meetings at which plans were being undoubtedly formed to establish a monarchy. The cry was taken up. Maelius, summoned before the aged [[Cincinnatus]] (specially appointed ''[[Roman dictator|dictator]]''), refused to appear, and was slain by the [[Magister equitum|Master of the Horse]], [[Gaius Servilius Ahala]]. Afterward his house was razed to the ground, his wheat distributed amongst the people, and his property confiscated. The open space called the Equimaelium, on which his house had stood, preserved the memory of his death along the [[Vicus Jugarius]]. [[Cicero]] calls Ahala's deed a glorious one, but, whether Maelius entertained any ambitious projects or not, his [[summary execution]] was an act of [[murder]], since by the ''[[Valerio-Horatian Laws|Lex Valeria Horatia de provocatione]]'' the dictator was bound to allow the right of appeal.{{sfn|Chisholm|1911|p=298}}\n"
                   "\n"
                   "==See also==\n"
                   "* [[Marcus Junius Brutus]]\n"
                   "\n"
                   "==References==\n"
                   "{{reflist}}\n"
                   "\n"
                   ";Attribution\n"
                   "*{{1911|wstitle=Maelius, Spurius |volume=17 |page=298}} Endnotes:\n"
                   "\n"
                   "==Sources==\n"
                   "*Niebuhr's ''History of Rome'', ii. 418 (Eng. trans., 1851);\n"
                   "*G. Cornewall Lewis, ''Credibility of early Roman History'', ii.;\n"
                   "*Livy, iv. 13;\n"
                   "*Ancient sources: [[Livy]], iv.13; Cicero, ''De senectute'' 16, ''De amicitia'' 8, ''De republica'', ii.49; [[Florus]], i.26; [[Dionysius Halicarnassensis]] xii.I.\n"
                   "\n"
                   "{{DEFAULTSORT:Maelius, Spurius}}\n"
                   "[[Category:439 BC deaths]]\n"
                   "[[Category:Ancient Roman murder victims]]\n"
                   "[[Category:Ancient Roman plebeians]]\n"
                   "[[Category:5th-century BC Romans]]\n"
                   "[[Category:Maelii]]\n"
                   "[[Category:Year of birth missing]]</text>\n"
                   "      <sha1>7lduy7euf0mqac27hol00i3g3ilpsex</sha1>\n"
                   "    </revision>\n"
                   "  </page>\n"
                   "  <page>";

int main(int argc, char *argv[]) {
    auto test = Tokenizer::index_string_file(file, 32);
    auto ssk = SortedKeysIndex(test);

    ssk.sort_and_group_shallow();
    ssk.sort_and_group_all();
    auto wie = ssk.get_index()[69];
    auto t =wie.get_frequencies_vector();

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
