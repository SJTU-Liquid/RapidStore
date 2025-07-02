#include "WCC.hpp"
#include <iostream>
#include "interfaces/interfaces.hpp"
#include "readers/reader.hpp"
#include "readers/edgeListReader.hpp"
#include "graph/edge.hpp"
#include "graph/edgeStream.hpp"
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

namespace driver {
    namespace algorithm {
        wccExperiments::wccExperiments(const int num_threads, const int granularity, std::shared_ptr<driver::interface::graphalyticsInterface> interface) 
            : m_num_threads(num_threads), m_granularity(granularity), m_interface(interface), m_snapshot(m_interface->get_shared_snapshot()) {}
        
        wccExperiments::~wccExperiments() {}

        std::unique_ptr<uint64_t[]> wccExperiments::wcc() {
            const uint64_t num_vertices = m_snapshot->vertex_count();
            std::unique_ptr<uint64_t[]> ptr_components { new uint64_t[num_vertices] };
            uint64_t * comp = ptr_components.get();

            #pragma omp parallel for
            for (uint64_t i = 0; i < num_vertices; i++) {
                comp[i] = i;
            }

            bool change = true;
            std::vector<std::thread> threads;
            uint64_t chunk_size = (num_vertices + m_num_threads - 1) / m_num_threads;
            while(change) {
                change = false;
                
                for (int i = 0; i < m_num_threads; i++) {
                    threads.emplace_back(std::thread([this, &change, &comp, num_vertices, chunk_size](int thread_id) {
                        this->m_interface->init_thread(thread_id);
                        auto snapshot_local = this->m_snapshot->clone();
                        uint64_t start = chunk_size * thread_id;
                        uint64_t end = std::min(start + chunk_size, num_vertices);
                        
                        for (uint64_t u = start; u < end; u++) {
                            snapshot_local->edges(u, [comp, &change, u, num_vertices](uint64_t v, double w) {
                                uint64_t comp_u = comp[u];
                                uint64_t comp_v = comp[v];
                                if (comp_u == comp_v) {
                                    return;
                                }

                                uint64_t high_comp = std::max(comp_u, comp_v);
                                uint64_t low_comp = std::min(comp_u, comp_v);
                                
                                if (high_comp >= num_vertices || low_comp >= num_vertices) return;
                                if (high_comp == comp[high_comp]) {
                                    change = true;
                                    comp[high_comp] = low_comp;
                                }

                                return;
                            });
                        }

                        delete snapshot_local;
                        this->m_interface->end_thread(thread_id);
                        
                    }, i));
                }

                for (auto & thread : threads) {
                    thread.join();
                }

                threads.clear();
                
                #pragma omp parallel for
                for (uint64_t i = 0; i < num_vertices; i++) {
                    while(comp[i] != comp[comp[i]]) {
                        comp[i] = comp[comp[i]];
                    }
                }
            }

            return ptr_components;
        }

        void wccExperiments::run_wcc(std::vector<std::pair<uint64_t, int64_t>> & external_ids) {
            auto start = std::chrono::high_resolution_clock::now();

            std::unique_ptr<uint64_t[]> ptr_components = wcc();
            
            std::vector<std::thread> threads;
            auto num_vertices = m_snapshot->vertex_count();
            
            uint64_t chunk_size = (num_vertices + m_num_threads - 1) / m_num_threads;
            external_ids.resize(num_vertices);

            m_interface->set_max_threads(m_num_threads);
            for (int i = 0; i < m_num_threads; i++) {
                threads.emplace_back(std::thread([this, &ptr_components, chunk_size, &external_ids] (int thread_id) {
                    m_interface->init_thread(thread_id);
                    uint64_t start = thread_id * chunk_size;
                    uint64_t end = std::min(start + chunk_size, m_snapshot->vertex_count());
                    
                    auto snapshot_local = m_snapshot->clone();

                    for (int64_t u = start; u < end; u++) {
                        external_ids[u] = std::make_pair(snapshot_local->physical2logical(u), snapshot_local->physical2logical(ptr_components[u]));
                    }

                    delete snapshot_local;
                    m_interface->end_thread(thread_id);
                }, i));
            }

            for (auto& thread : threads) {
                thread.join();
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            std::cout << "Gapbs WCC took " << duration.count() << " milliseconds" << std::endl;

            // for (auto &pair : external_ids) {
            //     std::cout << pair.first << " " << pair.second << std::endl;
            // }
        }

    } // namespace algorithm
} // namespace driver
