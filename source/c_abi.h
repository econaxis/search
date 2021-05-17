#include <fstream>
#include <vector>
#include "DocIDFilePair.h"

struct RustDIFP;


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
};

