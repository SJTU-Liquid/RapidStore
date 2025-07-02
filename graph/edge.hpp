#ifndef EDGE_HPP
#define EDGE_HPP
#include <memory>

namespace driver::graph {
    
    class weightedEdge {
    public:
        weightedEdge();
        weightedEdge(uint64_t source, uint64_t destination, double weight);
        weightedEdge(uint64_t source, uint64_t destination);
        void set_edge(uint64_t source, uint64_t destination, double weight);
        void set_edge(weightedEdge& edge);
        // void print_edge() const;
        // void print_edge(const std::string& filename) const;

        uint64_t source;
        uint64_t destination;
        double weight;

        bool operator==(const weightedEdge& rhs) const;
        bool operator!=(const weightedEdge& rhs) const;
        bool operator<(const weightedEdge& rhs) const;
    };

}
#endif