#include "edge.hpp"
#include <iostream>
#include <fstream>
#include <memory>

namespace driver::graph {
    // weightedEdge
    weightedEdge::weightedEdge() :source(0), destination(0), weight(0.0) {}

    weightedEdge::weightedEdge(uint64_t source, uint64_t destination, double weight) : source(source), destination(destination), weight(weight){}

    weightedEdge::weightedEdge(uint64_t source, uint64_t destination) : source(source), destination(destination), weight(-1.0) {}

    void weightedEdge::set_edge(uint64_t source, uint64_t destination, double weight) {
        this->source = source;
        this->destination = destination;
        this->weight = weight;
    }

    void weightedEdge::set_edge(weightedEdge& edge) {
        this->source = edge.source;
        this->destination = edge.destination;
        this->weight = edge.weight;
    }

    bool weightedEdge::operator==(const weightedEdge& edge) const {
        return (this->source == edge.source && this->destination == edge.destination);
    }

    bool weightedEdge::operator!=(const weightedEdge& edge) const {
        return (this->source != edge.source || this->destination != edge.destination);
    }

    bool weightedEdge::operator<(const weightedEdge& edge) const {
        return (this->source < edge.source || (this->source == edge.source && this->destination < edge.destination));
    }
}

