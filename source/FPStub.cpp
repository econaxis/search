//
// Created by henry on 2021-05-25.
//

#include "Serializer.h"
#include <filesystem>
#include "FPStub.h"

FPStub::FPStub(fs::path path) : stream(path, std::ios_base::binary) {
    int sz = Serializer::read_vnum(stream);
    diffvec.reserve(sz / 16 + 1);

    for (int i = 0; i < sz; i++) {
        if (i % interval == 0) {
            diffvec.push_back(stream.tellg());
        }
        auto dfp = Serializer::read_pair(stream);
        assert(dfp.document_id == i+1);
    }
}

std::string FPStub::query(int docid) const {
    int loc = docid / interval - 1;
    if(loc > diffvec.size()) {
        return "File not found";
    }
    int dloc = diffvec.at(loc);

    stream.seekg(dloc);

    auto pair = Serializer::read_pair(stream);

    while (pair.document_id < docid && stream.good()) {
        pair = Serializer::read_pair(stream);
    }

    if(pair.document_id != docid) {
        return "File not found";
    }

    return pair.file_name;
}
