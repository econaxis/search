#include "c_abi.h"
#include "Serializer.h"

namespace ffi = Serializer::ffi;
namespace sr = Serializer;

ifstream *create_ifstream_from_path(const char *path) {
    return ffi::create_ifstream_from_path(path);
};

void deallocate_ifstream(ifstream *stream) {
    return ffi::deallocate(stream);
};

void deallocate_ofstream(ofstream *stream) {
    return ffi::deallocate(stream);
}

void read_from_ifstream(ifstream *stream, char *buffer, uint32_t max_len) {
    stream->read(buffer, max_len);
}

uint32_t read_str(ifstream *stream, char *buf) {
    std::string str = sr::read_str(*stream);
    if (str.size() >= 500) {
        throw std::runtime_error("String too big");
    }
    str.copy(buf, str.size());
    return str.size();
}

uint32_t read_vnum(ifstream *stream) {
    return sr::read_vnum(*stream);
}


void read_filepairs(ifstream *stream, std::vector<DocIDFilePair> **vecpointer, uint32_t *length) {
//    auto *vec = new std::vector<DocIDFilePair>();
//    *vec = sr::read_filepairs(*stream);
//    *vecpointer = vec;
//    // 4 bytes for docid, 8 bytes for char*
//    *length = vec->size() * (4 + sizeof(std::size_t));
    auto *vec = new std::vector<DocIDFilePair>();
    vec->push_back(DocIDFilePair{1, "one"});
    vec->push_back(DocIDFilePair{2, "two"});
    vec->push_back(DocIDFilePair{3, "three"});
    *vecpointer = vec;
    *length = vec->size();
}

struct RustDIFP {
    uint32_t docid;
    char *name;
};

void copy_filepairs_to_buf(std::vector<DocIDFilePair> *vec, RustDIFP *buf, uint32_t max_length) {
    if (max_length != vec->size()) throw std::runtime_error("Incorrect size");
    for (int i = 0; i < vec->size(); i++) {
        buf[i].docid = vec->at(i).document_id;
        buf[i].name = vec->at(i).file_name.data();
    }
}

void deallocate_vec(std::vector<DocIDFilePair> *ptr) {
    std::cout<<"Dropping vector\n";
    delete ptr;
}
