#ifndef C_ABI_H
#define C_ABI_H

#include <fstream>
#include <vector>
#include "DocIDFilePair.h"
#include "rust-interface.h"


struct RustDIFP;
class SortedKeysIndexStub;

extern "C" {
typedef std::ifstream ifstream;
typedef std::ofstream ofstream;
ifstream *create_ifstream_from_path(const char *path);
void deallocate_ifstream(ifstream *stream);
void deallocate_ofstream(ofstream *stream);

void read_from_ifstream(ifstream *stream, char *buffer, uint32_t max_len);
uint32_t read_str(ifstream *stream, char *buf);
uint32_t read_vnum(ifstream *stream);
void read_filepairs(ifstream *stream, std::vector<DocIDFilePair> **vecpointer, uint32_t *length);
void deallocate_vec(std::vector<DocIDFilePair> *ptr);
void copy_filepairs_to_buf(std::vector<DocIDFilePair> *vec, RustDIFP *buf, uint32_t max_length);

#endif





void search_multi_indices(int num_indices, SortedKeysIndexStub **indices, int num_terms, const char **query_terms_ptr, RustVec* output_buffer);
uint32_t query_for_filename(SortedKeysIndexStub *index, uint32_t docid, char *buffer, uint32_t bufferlen);

void initialize_dir_vars();
SortedKeysIndexStub* clone_one_index(SortedKeysIndexStub* other);

SortedKeysIndexStub *load_one_index(const char* suffix_name);

void delete_one_index(SortedKeysIndexStub* ssk);
};

