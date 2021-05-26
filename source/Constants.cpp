
#include <filesystem>
#include "Constants.h"
#include "random_b64_gen.h"
#include <iostream>

namespace fs = std::filesystem;

fs::path data_files_dir = "ERROR!/must-call-initialize-directory-variables-first";
fs::path indice_files_dir = "ERROR!/must-call-initialize-directory-variables-first";
const std::string unique_directory_suffix = random_b64_str(5);

void initialize_directory_variables() {
    auto data_files_dir_env = std::getenv("DATA_FILES_DIR");
    if (data_files_dir_env) {
        data_files_dir = fs::path(data_files_dir_env);
        std::cout << "Using data file dir: " << data_files_dir_env << "\n";
    } else {
        data_files_dir = fs::path("/mnt/nfs/extra/data-files");
    }
    indice_files_dir = data_files_dir / "indices";
}


extern const std::array<std::string_view, 128> stop_words = std::array<std::string_view, 128>{
        "a", "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
        "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they",
        "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am",
        "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing",
        "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
        "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from",
        "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there",
        "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such",
        "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
        "should", "now"};