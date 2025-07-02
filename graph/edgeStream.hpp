#ifndef EDGE_STREAM_HPP
#define EDGE_STREAM_HPP

#include <vector>
#include <string>
#include "edge.hpp"
#include "readers/reader.hpp"
#include "readers/edgeListReader.hpp"

namespace driver {
    namespace graph {
        class edgeStream {
        private:
            std::vector<driver::graph::weightedEdge> edge_stream;
            int stream_size;
            int index;

        public:
            edgeStream() {};
            void load_stream(std::string file_name);
            void permute_stream();
            void sort();
            void remove_duplicates();
            bool get_next_edge(driver::graph::weightedEdge&);
            weightedEdge & operator[](int index);
            int get_size() const;
            int get_current_index() const;
            void reset_index();
            void reorder_and_partition(bool);
        };
    } // namespace graph
} // namespace driver

#endif // EDGE_STREAM_HPP