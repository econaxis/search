

#include "Serializer.h"
#include <filesystem>
#include "FPStub.h"

#include <algorithm>


FPStub::FPStub(fs::path path) : stream(path, std::ios_base::binary) {
    assert(stream);
    auto sz = Serializer::read_vnum(stream);
    for (auto i = 0; i < sz; i++) {
        auto dfp = Serializer::read_pair(stream);
        map.emplace(dfp.document_id, dfp.file_name);
    }
}

std::string FPStub::query(int docid) const {
    auto it =  map.find(docid);
    if(it == map.end()) return "File not found";
    else return it->second;
}
