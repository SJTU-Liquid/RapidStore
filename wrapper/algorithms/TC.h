#ifndef TC_HPP
#define TC_HPP

#include <iostream>
#include <memory>
#include <string>
#include <random>
#include <vector>
#include <utility>
#include <csignal>
#include <atomic>
#include <chrono>
#include <thread>
#include <omp.h>
#include <mutex>
#include "utils/log/log.h"

template <class F, class S>
class TriangleCounting {
    F & m_method;
    S m_snapshot;

public:
    TriangleCounting(F & method, S snapshot);
    ~TriangleCounting();

    uint64_t run_tc();
};

template <class F, class S>
TriangleCounting<F, S>::TriangleCounting(F & method, S snapshot) 
    :  m_method(method), m_snapshot(snapshot) {}

template <class F, class S>
TriangleCounting<F, S>::~TriangleCounting() {}
#define SEARCH_THRESHOLD 10
template <class F, class S>
uint64_t TriangleCounting<F, S>::run_tc() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto num_vertices = wrapper::snapshot_vertex_count(m_snapshot);
    uint64_t num_triangles = 0;
    uint64_t num_triangles_from_intersect = 0;
    // std::vector<std::pair<uint64_t, uint64_t>> edges;
    for (uint64_t i = 0; i < num_vertices; i++) {
//        if(i % 1000 == 0) {
//            std::cout << "TC: " << i << " / " << num_vertices << std::endl;
//        }

        auto degree_src = wrapper::snapshot_degree(m_snapshot, i);
        auto get_edges = [&] (uint64_t dst, double weight) {
            if (dst < i) {
                auto degree_dst = wrapper::snapshot_degree(m_snapshot, dst);
                if (degree_src > degree_dst * SEARCH_THRESHOLD) {
                    // search all edges of dest in src
                    auto search = [&](uint64_t d, double wright) {
                        if (wrapper::snapshot_has_edge(m_snapshot, i, d)) {
                            num_triangles += 1;
                        }
                    };
                    wrapper::snapshot_edges(m_snapshot, dst, search, false);
                } else if (degree_src * SEARCH_THRESHOLD < degree_dst) {
                    // search all edges of src in dest
                    auto search = [&](uint64_t d, double wright) {
                        if (wrapper::snapshot_has_edge(m_snapshot, dst, d)) {
                            num_triangles += 1;
                        }
                    };
                    wrapper::snapshot_edges(m_snapshot, i, search, false);
                } else {
                    auto res = wrapper::snapshot_intersect(m_snapshot, i, dst);
                    num_triangles_from_intersect += res;
                }
            }
        };

        wrapper::snapshot_edges(m_snapshot, i, get_edges, false);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Gapbs TC took " << duration.count() << " milliseconds" << std::endl;
    log_info("TC: %ld milliseconds", duration.count());
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 100000000);

    std::cout << "triangle count: " << num_triangles_from_intersect / 3 << std::endl;
    std::cout << "triangle count: " << num_triangles << std::endl;
    return num_triangles;
}


#endif // TC HPP
