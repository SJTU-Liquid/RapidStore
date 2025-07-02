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
#include "pageRank.hpp"

namespace driver {
    namespace algorithm {
        pageRankExperiments::pageRankExperiments(const int num_threads, const int granularity, const uint64_t num_iterations, const double damping_factor, std::shared_ptr<driver::interface::graphalyticsInterface> interface) 
            : m_num_threads(num_threads), m_granularity(granularity), m_num_iterations(num_iterations), m_damping_factor(damping_factor), m_interface(interface), m_snapshot(m_interface->get_shared_snapshot()) {
                omp_set_num_threads(num_threads);
            }
        pageRankExperiments::~pageRankExperiments() {}

        std::unique_ptr<double[]> pageRankExperiments::page_rank() {
            const uint64_t num_vertices = m_snapshot->vertex_count();

            const double init_score = 1.0 / num_vertices;
            const double base_score = (1.0 - m_damping_factor) / num_vertices;

            std::unique_ptr<double[]> ptr_scores {new double[num_vertices]()};
            double * scores = ptr_scores.get();
            #pragma omp parallel for
            for (uint64_t v = 0; v < num_vertices; v++) {
                scores[v] = init_score;
            }
            gapbs::pvector<double> outgoing_contrib(num_vertices, 0.0);

            for (uint64_t iter = 0; iter < m_num_iterations; iter++) {
                std::vector<double> dangling_sums(m_num_threads, 0.0);
                double dangling_sum = 0.0;

                uint64_t chunk_size = (num_vertices + m_num_threads - 1) / m_num_threads;
                std::vector<std::thread> threads;
                try {
                    m_interface->set_max_threads(m_num_threads);
                    for (int i = 0; i < m_num_threads; i++) {
                        threads.emplace_back(std::thread([this, &dangling_sums, &outgoing_contrib, chunk_size, num_vertices, &scores] (int thread_id) {
                            this->m_interface->init_thread(thread_id);
                            auto snapshot_local = this->m_snapshot->clone();
                            uint64_t start = thread_id * chunk_size;
                            uint64_t end = std::min(start + chunk_size, num_vertices);

                            for (uint64_t v = start; v < end; v++) {
                                uint64_t out_degree = snapshot_local->degree(v, false);
                                if (out_degree == 0) {
                                    dangling_sums[thread_id] += scores[v];
                                } else {
                                    outgoing_contrib[v] = scores[v] / out_degree;
                                }
                            }

                            delete snapshot_local;
                            this->m_interface->end_thread(thread_id);
                        }, i));
                    }

                    for (auto &t : threads) {
                        t.join();
                    }

                    threads.clear();
                    
                    for (int i = 0; i < m_num_threads; i++) {
                        dangling_sum += dangling_sums[i];
                    }

                    dangling_sum /= num_vertices;

                    for (int i = 0; i < m_num_threads; i++) {
                        threads.emplace_back(std::thread([this, &dangling_sum, &outgoing_contrib, chunk_size, num_vertices, &scores, &base_score] (int thread_id) {
                            this->m_interface->init_thread(thread_id);
                            auto snapshot_local = this->m_snapshot->clone();
                            uint64_t start = thread_id * chunk_size;
                            uint64_t end = std::min(start + chunk_size, num_vertices);
                            
                            for (uint64_t v = start; v < end; v++) {
                                double incoming_totol = 0.0;
                                snapshot_local->edges(v, [&](uint64_t src, double w) {
                                    incoming_totol += outgoing_contrib[src];
                                }, false);
                                
                                scores[v] = base_score + m_damping_factor * (incoming_totol + dangling_sum);
                            }

                            delete snapshot_local;
                            this->m_interface->end_thread(thread_id);
                        }, i));
                    }

                    for (auto &t : threads) {
                        t.join();
                    }
                } catch (std::exception &e) {
                    std::cerr << "Exception: " << e.what() << std::endl;
                    throw e;
                }

            }
            return ptr_scores;
        }

        void pageRankExperiments::run_page_rank(std::vector<std::pair<uint64_t, double>> & external_ids) {
            auto start = std::chrono::high_resolution_clock::now();
            auto scores = page_rank();

            std::vector<std::thread> threads;
            
            auto num_vertices = m_snapshot->vertex_count();
            
            uint64_t chunk_size = (num_vertices + m_num_threads - 1) / m_num_threads;
            external_ids.resize(num_vertices);

            m_interface->set_max_threads(m_num_threads);
            for (int i = 0; i < m_num_threads; i++) {
                threads.emplace_back(std::thread([this, &scores, chunk_size, &external_ids] (int thread_id) {
                    m_interface->init_thread(thread_id);
                    uint64_t start = thread_id * chunk_size;
                    uint64_t end = std::min(start + chunk_size, m_snapshot->vertex_count());
                    
                    auto snapshot_local = m_snapshot->clone();

                    for (int64_t u = start; u < end; u++) {
                        external_ids[u] = std::make_pair(snapshot_local->physical2logical(u), scores[u]);
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
            std::cout << "Gapbs Page Rank took " << duration.count() << " milliseconds" << std::endl;

            // for (auto &pair : external_ids) {
            //     std::cout << pair.first << " " << pair.second << std::endl;
            // }
        }
    } // namespace algorithm
} // namespace driver
