#include "Serializer.h"
#include <filesystem>
#include "FPStub.h"
#include <fstream>

#include <algorithm>


FPStub::FPStub(const fs::path& path)  {
    auto stream = std::ifstream(path, std::ios_base::binary);
    assert(stream);
    Serializer::read_vnum(stream);
    auto fp = Serializer::read_filepairs(stream);
    for (auto &p : fp) {
        map.emplace(p.document_id, p.file_name);
    }
}

std::string FPStub::query(uint32_t docid) const {
    auto it =  map.find(docid);
    if(it == map.end()) return "File not found";
    else return it->second;
}
