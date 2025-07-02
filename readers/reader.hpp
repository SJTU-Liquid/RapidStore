#ifndef DRIVER_READER_READER_HPP
#define DRIVER_READER_READER_HPP

#include <memory>
#include "../utils/error_type.hpp"
#include "../graph/edge.hpp"

using namespace driver::graph;

namespace driver::reader {
    enum class readerType;    

    /*
    ** An abstract type that returns corresponding reader type
    */
    class Reader {
        // disable copy ctors
        Reader(const Reader&) = delete;
        Reader& operator=(const Reader&) = delete;

    public:
        static std::unique_ptr<Reader> open(const std::string& path, readerType type, bool weighted = false);

        Reader();
        virtual bool read(driver::graph::weightedEdge& edge) {return false;};
        virtual bool read(uint64_t &vertex) {return false;};
        virtual bool is_directed() const = 0;
    };

    /*
    ** enum type 
    */
    enum class readerType {
        edgeList,
        vertexList
    };
}

//DRIVER_READER_READER_HPP
#endif