//#include "compactor/Compactor.h"
//#include "Constants.h"
//#include "IndexFileLocker.h"
//#include <cassert>
//#include <iostream>
//#include <cstring>
//#include <thread>
//#include <vector>
//#include "FileListGenerator.h"
//#include <future>
//#include "Serializer.h"
//#include <map>
//#include <queue>
//
//namespace fs = std::filesystem;
//
//void Compactor_test();
//
//static fs::path make_path(const std::string &name, const std::string &suffix) {
//    return indice_files_dir / (name + "-" + suffix);
//}
//
//int get_index_file_len() {
//    std::ifstream i(indice_files_dir / "index_files", std::ios_base::in);
//    assert(i);
//    std::string s;
//    int counter = 0;
//    while (std::getline(i, s) && i.good()) {
//        counter++;
//    }
//    return counter;
//}
//
//void clear_queue(std::ofstream &index_file_working, std::vector<std::future<std::string>> &threads,
//                 std::map<std::string, std::string> &fpmap) {
//    using namespace std::chrono_literals;
//    for (auto i  = 0; i < threads.size(); i++) {
//        auto line_fut = threads.begin() + i;
//
//        if(i >= threads.size()) return;
//
//        if(line_fut->valid()&&line_fut->wait_for(1us) != std::future_status::ready) {
//            continue;
//        }
//
//        auto line = line_fut->get();
//        threads.erase(line_fut);
//
//        if (!line.empty()) {
//            std::ifstream fp_stream(indice_files_dir / ("filemap-" + line));
//            auto fps = Serializer::read_filepairs(fp_stream);
//
//            auto errored = false;
//            for (auto &p : fps) {
//                if (auto it = fpmap.find(p.file_name); it == fpmap.end()) {
//                    fpmap[p.file_name] = line;
//                } else if (!errored) {
//                    errored = true;
//                    std::cout << it->second << " duplicates " << line << "\n";
//                }
//            }
//
//            std::cout << line << " " << std::max_element(fps.begin(), fps.end())->document_id << "\n";
//            index_file_working << line << "\n" << std::flush;
//        } else {
//            std::cerr << line << " invalid\n";
//        }
//    }
//}
//
//int main(int argc, char *argv[]) {
//
////    Compactor_test();
//
//    initialize_directory_variables();
//
//
//    if (argc == 2 && strcmp(argv[1], "check") == 0) {
//        fs::rename(indice_files_dir / "index_files", indice_files_dir / "index_files_old");
//        fs::remove(indice_files_dir / "file_metadata.msgpack");
//
//        std::ifstream index_file(indice_files_dir / "index_files_old");
//        std::ofstream index_file_working(indice_files_dir / "index_files", std::ios_base::out);
//        std::string line;
//        std::vector<std::future<std::string>> threads;
//        std::map<std::string, std::string> fpmap;
//
//        while (std::getline(index_file, line)) {
//            threads.emplace_back(std::async(std::launch::async, [=]() {
//                try {
//                    Compactor::test_makes_sense(line);
//
//                    std::cout << "Checked " << line << "\n";
//                    return line;
//                } catch (const std::runtime_error &e) {
//                    std::cerr << "Error for line " << line << "\n" << e.what() << "\n";
//                    fs::remove(make_path("frequencies", line));
//                    fs::remove(make_path("terms", line));
//                    fs::remove(make_path("positions", line));
//                    fs::remove(make_path("filemap", line));
//                    return std::string{""};
//                }
//            }));
//
//            while (threads.size() >= 5) {
//                clear_queue(index_file_working, threads, fpmap);
//            }
//        }
//        while (!threads.empty()) clear_queue(index_file_working, threads, fpmap);
//
//
//        return 0;
//    } else {
//        while (true) {
//            if (get_index_file_len() <= 8) break;
//
//            auto joined_suffix = Compactor::compact_two_files();
//
//            if (joined_suffix) {
//                if (joined_suffix.value() == "CONTINUE") {
//                    continue;
//                }
////                IndexFileLocker::do_lambda([&] {
////                    std::ofstream index_file(indice_files_dir / "index_files", std::ios_base::app);
////                    index_file << *joined_suffix << "\n";
////                });
//                std::cout << "Compacted to " << joined_suffix.value() << "\n";
//            } else {
//                break;
//            }
//        };
//    }
//
//
//}
