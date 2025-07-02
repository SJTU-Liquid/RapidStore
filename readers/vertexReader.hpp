#ifndef DRIVER_READER_VERTEX_READER_HPP
#define DRIVER_READER_VERTEX_READER_HPP

#include <fstream>
#include <random>
#include "reader.hpp"
#include <iostream>

namespace driver::graph { class WeightedEdge; } // forward decl.

namespace driver::reader {
class vertexReader : virtual public Reader  {
private:
    std::fstream m_handle; // internal file handle
public:
    /**
     * Read the edge list from the given file
     * @param path the source of the file
     */
    vertexReader(const std::string& path);
    ~vertexReader();
    bool is_directed() const;

    // interface
    bool read(uint64_t &vertex) override;
};
}

#endif //DRIVER_READER_VERTEX_READER_HPP