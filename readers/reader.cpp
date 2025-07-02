#include "edgeListReader.hpp"
#include "vertexReader.hpp"
#include "reader.hpp"
#include <memory>

using namespace std;
namespace driver::reader {
    Reader::Reader() {}

    std::unique_ptr<Reader> Reader::open(const std::string& path, readerType type = readerType::edgeList, bool weighted){
        switch (type) {
            case readerType::edgeList:
                return std::make_unique<edgeListReader>(path, weighted);
            case readerType::vertexList:
                return std::make_unique<vertexReader>(path);
        }
    }
}