

#include "Serializer.h"
#include <filesystem>
#include "FPStub.h"

#include <algorithm>
#include <robin_hood/robin_hood.h>


FPStub::FPStub(fs::path path) : stream(path, std::ios_base::binary) {
    assert(stream);

    buffer = std::make_unique<char[]>(3000);
    stream.rdbuf()->pubsetbuf(buffer.get(), 3000);
    uint sz = Serializer::read_vnum(stream);
    diffvec.reserve(sz / 16 + 1);

    for (int i = 0; i < sz; i++) {
        if (i % interval == 0) {
            diffvec.push_back(stream.tellg());
        }
        auto dfp = Serializer::read_pair(stream);
        map.emplace(dfp.document_id, dfp.file_name);
    }
}

std::string FPStub::query(int docid) const {
    auto loc = std::lower_bound(diffvec.begin(), diffvec.end(), docid) - 1;
    if(loc >= diffvec.end() || loc < diffvec.begin()) {
        return "File not found";
    }



    auto it =  map.find(docid);
//    if(it == map.end()) return "File not found";
//    else return it->second;

    if(loc >= diffvec.end()) return "File not found";
    stream.seekg(*loc);

    auto pair = Serializer::read_pair(stream);

    while (pair.document_id < docid && stream.good()) {
        pair = Serializer::read_pair(stream);
    }

    if(pair.document_id != docid) {
        return "File not found";
    }

    assert(pair.file_name == it->second);

    return pair.file_name;
}
