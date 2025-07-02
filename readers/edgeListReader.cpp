#include "edgeListReader.hpp"
#include "reader.hpp"
#include <sstream>
#include <fstream>
#include <iostream>
#include <random>
#include <ctime>

namespace driver::reader {
    edgeListReader::edgeListReader(const std::string& path, bool weighted) : m_is_weighted(weighted){
        m_handle.open(path, std::ios::in);
        if (!m_handle.good()) {
            std::cerr<<("Cannot open file %s", path.c_str());
        }
        m_handle.seekg(0, std::ios::beg);
    }

    edgeListReader::~edgeListReader(){
        m_handle.close();
    }

    bool edgeListReader::is_directed() const{
        return true;
    }

    /**
    * Reads an edge from the file stream.
    *
    * @param edge - A reference to a driver::graph::weightedEdge object. This object will be set to the next valid edge read from the file.
    *
    * @return True if an edge is successfully read and parsed from the file, and the 'edge' parameter is set. False if the end of the file has been reached.
    *
    * This function attempts to read a line from the file, and parse two node (or edge) IDs and a weight from that line.
    * If a line from the file cannot be parsed correctly (for example, if the line is empty, starts with '#', or does not contain enough elements),
    * then the function skips that line and moves on to the next one.
    * If the function successfully reads and parses an edge from the file, it sets the value of the 'edge' parameter and returns true.
    * If the function reaches the end of the file, it returns false.
    *
    * Note that if the file stream is not weighted, a random weight will be assigned to the edge.
    */
    
    bool edgeListReader::read(driver::graph::weightedEdge& edge) {
        std::string line;
        while (std::getline(m_handle, line)) {
            if (line.empty() || line[0] == '#') {
                continue;
            }

            std::istringstream ss(line);
            uint64_t sourceId, destId;
            double weight = random() / ((double) RAND_MAX);
            
            if (m_is_weighted){
                if (!(ss >> sourceId)  || !(ss >> destId) || !(ss >> weight)) {
                    std::cerr << "Invalid line: " << line << std::endl;
                    continue;
                }
            }

            else {
            if (!(ss >> sourceId) || !(ss >> destId)) {
                    std::cerr << "Invalid line: " << line << std::endl;
                    continue;
                } 
                std::srand(static_cast<unsigned int>(std::time(nullptr)));
                weight = static_cast<double>(std::rand()) / RAND_MAX;
            }

            edge.set_edge(sourceId, destId, weight);
            return true;
        }
        return false;
    }


}
