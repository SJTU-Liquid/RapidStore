#ifndef DRIVER_READER_EDGELISTREADER_HPP
#define DRIVER_READER_EDGELISTREADER_HPP

#include <fstream>
#include <random>
#include "reader.hpp"
#include <iostream>

namespace driver::reader {
    class edgeListReader : public Reader {
    private:
        std::fstream m_handle; // internal file handle
        const bool m_is_weighted; // whether we are reading a weighted graph
    public:
        /**
        * Read the edge list from the given file
        * @param path the source of the file
        * @param is_weighted whether the input file is weighted (file.wel) or not (file.el)
        */
        
        edgeListReader(const std::string& path, bool is_weighted);
        ~edgeListReader();

        // interface
        bool read(driver::graph::weightedEdge& edge) override;
        bool is_directed() const override;
    };
}

#endif //DRIVER_READER_EDGELISTREADER_HPP