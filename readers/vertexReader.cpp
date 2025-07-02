#include "vertexReader.hpp"
#include "reader.hpp"
#include <sstream>
#include <iostream>
#include <fstream>

namespace driver::reader {
    vertexReader::vertexReader(const std::string& path) {
        m_handle.open(path, std::ios::in);
        if (!m_handle.good()){
            std::cerr<<("Cannot open file %s", path.c_str());
            std::cout << "cannot open" << std::endl;
            std::cout << path;
        }
    }

    vertexReader::~vertexReader(){
        m_handle.close();
    }

    /**
    * Reads a vertex from the file stream.
    *
    * @param vertex - A reference to a uint64_t. This value will be set to the next valid vertex ID read from the file.
    *
    * @return True if a vertex ID is successfully read and parsed from the file, and the 'vertex' parameter is set. False if the end of the file has been reached.
    *
    * This function attempts to read a line from the file, and parse a vertex ID from that line.
    * If a line from the file cannot be parsed correctly (for example, if the line is empty or starts with '#'),
    * then the function skips that line and moves on to the next one.
    * If the function successfully reads and parses a vertex ID from the file, it sets the value of the 'vertex' parameter and returns true.
    * If the function reaches the end of the file, it returns false.
    */
    bool vertexReader::read(uint64_t& vertex) {
        std::string line;
        while (std::getline(m_handle, line)) {
            
            if (line.empty() || line[0] == '#') {
                continue;
            }

            std::istringstream ss(line);
            
            if (!(ss >> vertex)) {
                std::cerr << "Invalid line: " << line << std::endl;
                continue;
            } 
            
            return true;
        }

        return false;
    }

    bool vertexReader::is_directed() const {
        return false;
    }

}
