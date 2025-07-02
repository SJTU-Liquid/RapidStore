#ifndef TC_OPT_HPP
#define TC_OPT_HPP

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
class TriangleCounting_optimized {
    F & m_method;
    S m_snapshot;

public:
    TriangleCounting_optimized(F & method, S snapshot);
    ~TriangleCounting_optimized();

    uint64_t run_tc();
};

template <class F, class S>
TriangleCounting_optimized<F, S>::TriangleCounting_optimized(F & method, S snapshot) 
    :  m_method(method), m_snapshot(snapshot) {}

template <class F, class S>
TriangleCounting_optimized<F, S>::~TriangleCounting_optimized() {}

template <class F, class S>
uint64_t TriangleCounting_optimized<F, S>::run_tc() {
    auto start = std::chrono::high_resolution_clock::now();
    
    auto num_vertices = wrapper::snapshot_vertex_count(m_snapshot);
    uint64_t num_triangles = 0;
    // std::vector<std::pair<uint64_t, uint64_t>> edges;
    for (uint64_t n1 = 0; n1 < num_vertices; n1++) {
        if(n1 % 10 == 0) {
            std::cout << "TC: " << n1 << " / " << num_vertices << std::endl;
        }
        std::vector<uint64_t> m_neighbors;
        auto get_edges = [&] (uint64_t n2, double w2) {
            if (n2 > n1) return;
            m_neighbors.push_back(n2);
            
            uint64_t marker = 0;
            auto get_intersection = [&] (uint64_t n3, double w3) {
                if (n3 > n2) return;
                if (n3 > m_neighbors[marker]) {
                    do {
                        marker++;
                    } while (marker < m_neighbors.size() && n3 > m_neighbors[marker]);
                }

                if (n3 == m_neighbors[marker]) {
                    num_triangles += 1;
                    marker++;
                }
            };
            wrapper::snapshot_edges(m_snapshot, n2, get_intersection, false);
        };
        wrapper::snapshot_edges(m_snapshot, n1, get_edges, false);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Gapbs TC took " << duration.count() << " milliseconds" << std::endl;
    log_info("TC: %ld milliseconds", duration.count());
//    std::cout << "triangle count: " << num_triangles << std::endl;
    return num_triangles;
}


#endif // TC HPP
