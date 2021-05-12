

//void ResultsPrinter::print_results(std::vector<SafeMultiSearchResult> &temp1, std::vector<DocIDFilePair> &filepairs) {
//    std::ifstream matched_file;
//    for (int i = std::min(10UL, temp1.size()) - 1; i >= 0; i--) {
//        auto &v = temp1[i];
//        auto pos = std::lower_bound(filepairs.begin(), filepairs.end(), v.document_id, [](auto &a, auto &b) {
//            return a.document_id < b;
//        });
//        if (pos == filepairs.end()) continue;
//
//        std::string prebuffer(100, ' '), word, postbuffer(100, ' ');
//        matched_file.open(data_files_dir / pos->file_name);
//
//        std::cout << pos->file_name << ":\n";
//        for (auto[score, t0] : v.positions) {
//            matched_file.seekg(t0);
//            matched_file >> word;
//            std::cout << word << " ";
//        }
//        std::cout << "\n=================\n";
//        matched_file.close();
//    }
//    std::cout << "Done search " << temp1.size() << std::endl;
//
//
//    std::cout << "\n>> ";
//}
