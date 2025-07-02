#include <random>
#include <chrono>
#include <algorithm>
#include <iostream>
#include "edgeStream.hpp"

namespace driver {
    namespace graph {
        void edgeStream::load_stream(std::string file_path) {
            auto edge_reader = driver::reader::Reader::open(file_path, driver::reader::readerType::edgeList);
            driver::graph::weightedEdge edge;
            while(edge_reader->read(edge)) {
                this->edge_stream.push_back(edge);
            }
        }

        void edgeStream::permute_stream() {
            unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
            std::shuffle(this->edge_stream.begin(), this->edge_stream.end(), std::default_random_engine(seed));
        }

        void edgeStream::sort() {
            std::sort(this->edge_stream.begin(), this->edge_stream.end());
        }

        void edgeStream::remove_duplicates() {
            sort();
            this->edge_stream.erase(std::unique(this->edge_stream.begin(), this->edge_stream.end()), this->edge_stream.end());
        }

        bool edgeStream::get_next_edge(driver::graph::weightedEdge& edge) {
            if (this->index == this->edge_stream.size()) {
                return false;
            }

            edge.set_edge(this->edge_stream[this->index++]);
            return true;
        }
        
        driver::graph::weightedEdge & edgeStream::operator[](int index) {
            return this->edge_stream[index];
        }

        int edgeStream::get_size() const {
            return this->edge_stream.size();
        }

        int edgeStream::get_current_index() const {
            return this->index;
        }

        void edgeStream::reset_index() {
            this->index = 0;
        }

        void edgeStream::reorder_and_partition(bool high_degree_partition) {
            std::unordered_map<int, int> degree_map;
            for (const auto& edge : this->edge_stream) {
                degree_map[edge.source]++;
                degree_map[edge.destination]++;
            }

            std::sort(this->edge_stream.begin(), this->edge_stream.end(),
                      [&degree_map, high_degree_partition](const weightedEdge& edge1, const weightedEdge& edge2) {
                          int degree1 = std::max(degree_map[edge1.source], degree_map[edge1.destination]);
                          int degree2 = std::max(degree_map[edge2.source], degree_map[edge2.destination]);
                          return high_degree_partition ? degree1 > degree2 : degree1 < degree2;
                      });

            int num_edges_to_pick = static_cast<int>(this->edge_stream.size() * 0.10);
            std::vector<weightedEdge> picked_edges(this->edge_stream.begin(), this->edge_stream.begin() + num_edges_to_pick);

            picked_edges.insert(picked_edges.end(), this->edge_stream.begin() + num_edges_to_pick, this->edge_stream.end());

            this->edge_stream = std::move(picked_edges);
            this->reset_index();  
            remove_duplicates();
        }
        


    } // namespace graph
} // namespace driver