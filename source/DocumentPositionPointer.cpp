#include "DocumentPositionPointer.h"
#include <ostream>
#include <sstream>


std::ostream &operator<<(std::ostream &os, std::vector<DocumentPositionPointer> vec) {
    std::ostringstream buffer;
    buffer << "[";
    for (const auto &doc_pointer : vec) {
        buffer << doc_pointer.document_id << ":" << doc_pointer.document_position << ", ";
    }
    buffer.seekp(-2, std::ios_base::cur);
    buffer << "]\n";
    os << buffer.str();
    return os;
}
