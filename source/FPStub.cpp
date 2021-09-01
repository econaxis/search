#include "Serializer.h"
#include <filesystem>
#include "FPStub.h"
#include <fstream>

FPStub::FPStub(const fs::path &path) {
    auto stream = std::ifstream(path, std::ios_base::binary);
    assert(stream);
    auto fp = Serializer::read_filepairs(stream);

    if(fp.empty()) return;

    // Pre-allocate by assuming the first filename's length is representative of the average filenames.
    // Estimates broadly the total size required to hold all filenames in memory.
    // TODO: if holding filenames in memory ever gets too big, we might try virtual memory tricks.
    joined_names.reserve(fp.size() * fp[0].file_name.size());

    for (auto &p : fp) {
        map.emplace(p.document_id, FPStub::StringSlice{joined_names.size(), p.file_name.size()});
        joined_names.append(p.file_name);
    }
}

std::string FPStub::query(uint32_t docid) const {
    auto it = map.find(docid);
    if (it == map.end()) return "File not found";

    return std::string(joined_names, it->second.index, it->second.size);
}
