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
#include <limits>
#include "SSSP.hpp"

namespace driver {
    namespace algorithm {
        ssspExperiments::ssspExperiments(const int num_threads, const int granularity, const double delta, std::shared_ptr<driver::interface::graphalyticsInterface> interface)
            : m_num_threads(num_threads), m_granularity(granularity), m_delta(delta), m_interface(interface), m_snapshot(interface->get_shared_snapshot()) {}
        
        ssspExperiments::~ssspExperiments() {}

        gapbs::pvector<double> ssspExperiments::sssp(uint64_t source) {
            const uint64_t num_vertices = m_snapshot->vertex_count();
            const uint64_t num_edges = m_snapshot->edge_count();
            const size_t kMaxBin = std::numeric_limits<size_t>::max()/2;

            // Initialize distances to inf
            gapbs::pvector<double> dist(num_vertices, std::numeric_limits<double>::infinity());
            dist[source] = 0;
            gapbs::pvector<uint64_t> frontier(num_edges);

            size_t shared_indexes[2] = {0, kMaxBin};
            size_t frontier_tails[2] = {1, 0};

            frontier[0] = source;

            std::vector<std::vector<uint64_t>> local_bins(0);
            
            size_t iter = 0;
            while(shared_indexes[iter & 1] != kMaxBin) {
                std::vector<std::thread> threads;
                size_t &curr_bin_index = shared_indexes[iter & 1];
                size_t &next_bin_index = shared_indexes[(iter + 1) & 1];
                size_t &curr_frontier_tail = frontier_tails[iter & 1];
                size_t &next_frontier_tail = frontier_tails[(iter + 1) & 1];

                size_t chunk_size = (curr_frontier_tail + m_num_threads - 1) / m_num_threads;
                m_interface->set_max_threads(m_num_threads);
                for (int i = 0; i < m_num_threads; i++) {
                    threads.emplace_back(std::thread([&] (int thread_id) {
                        this->m_interface->init_thread(thread_id);
                        
                        auto snapshot_local = this->m_snapshot->clone();

                        size_t start = thread_id * chunk_size;
                        size_t end = std::min((thread_id + 1) * chunk_size, curr_frontier_tail);

                        for (size_t i = start; i < end; i++) {
                            uint64_t u = frontier[i];

                            if (dist[u] >= m_delta * static_cast<double>(curr_bin_index)) {
                                snapshot_local->edges(u, [this, &dist, &local_bins, u](uint64_t v, double w){
                                    if (v >= dist.size() || u >= dist.size()) return;
                                    double old_dist = dist[v];
                                    double new_dist = dist[u] + 1;

                                    if (new_dist < old_dist) {
                                        bool changed_dist = true;
                                        while(!gapbs::compare_and_swap(dist[v], old_dist, new_dist)) {
                                            old_dist = dist[v];
                                            if (new_dist >= old_dist) {
                                                changed_dist = false;
                                                break;
                                            }
                                        }

                                        if (changed_dist) {
                                            size_t bin_index = static_cast<size_t>(new_dist / m_delta);
                                            std::lock_guard<std::mutex> lock(m_mutex);
                                            if (bin_index >= local_bins.size()) {
                                                local_bins.resize(bin_index + 1);
                                            }
                                            local_bins[bin_index].push_back(v);
                                        }
                                    }

                                }, false);
                            }
                        }

                        delete snapshot_local;
                        this->m_interface->end_thread(thread_id);
                    }, i));
                }

                for (auto &thread : threads) {
                    thread.join();
                }


                for (size_t i = curr_bin_index; i < local_bins.size(); i++) {
                    if (!local_bins[i].empty()) {
                        #pragma omp critical
                        next_bin_index = std::min(next_bin_index, i);
                        break;
                    }
                }

                curr_bin_index = kMaxBin;
                curr_frontier_tail = 0;

                if (next_bin_index < local_bins.size()) {
                    size_t copy_start = gapbs::fetch_and_add(next_frontier_tail, local_bins[next_bin_index].size());
                    copy(local_bins[next_bin_index].begin(), local_bins[next_bin_index].end(), frontier.data() + copy_start);
                    local_bins[next_bin_index].resize(0);
                }

                iter++;
            }
            return dist;
        }

        void ssspExperiments::run_sssp(uint64_t source, std::vector<std::pair<uint64_t, double>> & external_ids) {
            // m_snapshot = m_interface->get_shared_snapshot();
            source = m_snapshot->logical2physical(source);
            auto start = std::chrono::high_resolution_clock::now();
            auto dist = sssp(source);

            std::vector<std::thread> threads;
            
            auto num_vertices = m_snapshot->vertex_count();
            
            uint64_t chunk_size = (num_vertices + m_num_threads - 1) / m_num_threads;
            external_ids.resize(num_vertices);

            m_interface->set_max_threads(m_num_threads);
            for (int i = 0; i < m_num_threads; i++) {
                threads.emplace_back(std::thread([this, &dist, chunk_size, &external_ids] (int thread_id) {
                    m_interface->init_thread(thread_id);
                    uint64_t start = thread_id * chunk_size;
                    uint64_t end = std::min(start + chunk_size, m_snapshot->vertex_count());
                    
                    auto snapshot_local = this->m_snapshot->clone();

                    for (int64_t u = start; u < end; u++) {
                        external_ids[u] = std::make_pair(snapshot_local->physical2logical(u), dist[u]);
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
            std::cout << "Gapbs SSSP took " << duration.count() << " milliseconds" << std::endl;

            // for (auto &external_id : external_ids) {
            //     std::cout << external_id.first << " " << external_id.second << std::endl;
            // }
            
        }

    } // namespace algorithm
} // namespace driver
