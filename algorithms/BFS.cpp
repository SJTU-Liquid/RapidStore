#include "BFS.hpp"

#include <iostream>
#include "interfaces/interfaces.hpp"
#include "readers/reader.hpp"
#include "readers/edgeListReader.hpp"
#include "graph/edge.hpp"
#include <memory>
#include <string>
#include <random>
#include <vector>
#include <utility>
#include <csignal>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include "graph/edgeStream.hpp"

namespace driver {
    namespace algorithm {
        bfsExperiments::bfsExperiments(const int num_threads, const int granularity, const int alpha, const int beta, std::shared_ptr<driver::interface::graphalyticsInterface> interface)
            : m_num_threads(num_threads), m_granularity(granularity), m_alpha(alpha), m_beta(beta), m_interface(interface), m_snapshot(m_interface->get_shared_snapshot()) {
                omp_set_num_threads(num_threads);
            }

        bfsExperiments::~bfsExperiments() {}

        gapbs::pvector<std::atomic<int64_t>> bfsExperiments::init_distances() {
            const uint64_t N = m_snapshot->vertex_count();
            gapbs::pvector<std::atomic<int64_t>> distances(N);

            std::atomic<uint64_t> vertex_checked = 0;
            std::vector<std::thread> threads;
            uint64_t size = this->m_interface->vertex_count();
            uint64_t chunk_size = (size + m_num_threads - 1) / m_num_threads;
            try {
                m_interface->set_max_threads(m_num_threads);
                for (int i = 0; i < m_num_threads; i++) {
                    threads.emplace_back(std::thread([this, &vertex_checked, &distances, size, chunk_size] (int thread_id) {
                        this->m_interface->init_thread(thread_id);
                        auto snapshot_local = this->m_snapshot->clone();
                        uint64_t start = thread_id * chunk_size;
                        uint64_t end = start + chunk_size;
                        if (end > size) end = size;

                        for (uint64_t i = start; i < end; i++) {
                            uint64_t out_degree = snapshot_local->degree(i, false);
                            // uint64_t out_degree = 0;
                            distances[i] = out_degree != 0 ? -out_degree : -1;
                        }

                        delete snapshot_local;
                        this->m_interface->end_thread(thread_id);
                    }, i));
                }
            }
            catch (const std::exception& e) {
                std::cerr << "Exception in init_distances: " << e.what() << std::endl;
                throw;
            }

            for (auto& thread : threads) {
                thread.join();
            }

            return distances;
        }

        void bfsExperiments::QueueToBitmap(const gapbs::SlidingQueue<int64_t> &queue, gapbs::Bitmap &bm) {
            #pragma omp parallel for
            for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
                int64_t u = *q_iter;
                bm.set_bit_atomic(u);
            }
        }

        void bfsExperiments::BitmapToQueue(const int64_t size, const gapbs::Bitmap &bm, gapbs::SlidingQueue<int64_t> &queue) {
            const int64_t N = size;

            #pragma omp parallel
            {
                gapbs::QueueBuffer<int64_t> lqueue(queue);
                #pragma omp for
                for (int64_t n = 0; n < N; n++) {
                    if (bm.get_bit(n)) lqueue.push_back(n);
                }
                lqueue.flush();
            }
            queue.slide_window();
        }


        int64_t bfsExperiments::TDStep(gapbs::pvector<std::atomic<int64_t>>& distances, int64_t distance, gapbs::SlidingQueue<int64_t>& queue) {
            std::vector<int64_t> results(m_num_threads, 0);
            uint64_t scout_count = 0;
            std::vector<std::thread> threads;

            uint64_t chunk_size = (queue.size() + m_num_threads - 1) / m_num_threads;

            try {
                m_interface->set_max_threads(m_num_threads);

                for (uint64_t i = 0; i < m_num_threads; i++) {
                    threads.emplace_back(std::thread([this, &distances, distance, &scout_count, &queue, chunk_size, &results] (int thread_id) {
                        m_interface->init_thread(thread_id);
                        uint64_t start = thread_id * chunk_size;
                        uint64_t end = start + chunk_size;
                        
                        if (end > queue.size()) end = queue.size();
                        
                        if (start < end) {
                            gapbs::QueueBuffer<int64_t> lqueue(queue);
                            
                            auto snapshot_local = m_snapshot->clone();
                            
                            for (auto q_iter = queue.begin() + start; q_iter != queue.end(); q_iter++) {
                                int64_t u = *q_iter;
                                
                                snapshot_local->edges(u, [&distances, distance, &lqueue, &results, thread_id](uint64_t destination, double w){
                                    int64_t curr_val = distances[destination];

                                    if (curr_val < 0 && distances[destination].compare_exchange_strong(curr_val, distance)) {
                                        lqueue.push_back(destination);
                                        results[thread_id] += -curr_val;
                                    }
                                    
                                }, false);
                            }

                            // std::unique_lock<std::mutex> lock(m_mutex);
                            lqueue.flush();
                            delete snapshot_local;
                        }
                        // std::unique_lock<std::mutex> unlock(m_mutex);
                        
                        m_interface->end_thread(thread_id);

                    }, i));
                }

                for (auto& thread : threads) {
                    thread.join();
                }

                for (auto& result : results) {
                    scout_count += result;
                }

            } catch (const std::exception& e) {
                std::cerr << "Exception in TDStep: " << e.what() << std::endl;
                throw e;
            }

            return scout_count;
        }

        int64_t bfsExperiments::BUStep(gapbs::pvector<std::atomic<int64_t>>& distances, int64_t distance, gapbs::Bitmap &front, gapbs::Bitmap &next) {
            const uint64_t N = m_snapshot->vertex_count();
            int64_t awake_count = 0;
            std::vector<uint64_t> results(m_num_threads, 0);
            std::vector<std::thread> threads;
            next.reset();

            uint64_t chunk_size = (N + m_num_threads - 1) / m_num_threads;

            try {
                m_interface->set_max_threads(m_num_threads);
                
                for (int i = 0; i < m_num_threads; i++) {
                    threads.emplace_back(std::thread([this, &distances, distance, &front, &next, chunk_size, &awake_count, &results, N] (int thread_id) {
                        m_interface->init_thread(thread_id);
                        uint64_t start = thread_id * chunk_size;
                        uint64_t end = std::min(start + chunk_size, N);
                        auto snapshot_local = m_snapshot->clone();
                        for (uint64_t u = start; u < end; u++) {
                            if (distances [u] < 0) {
                                
                                bool done = false;
                                snapshot_local->edges(u, [u, &distances, distance, &front, &next, &done, &results, &thread_id](uint64_t destination, double w){
                                    if (destination > distances.size()) return;
                                    if (front.get_bit(destination)) {
                                        distances[u] = distance;
                                        results[thread_id]++;
                                        next.set_bit(u);
                                        done = true;
                                    }
                                }, false);
                            }
                        }

                        delete snapshot_local;
                        m_interface->end_thread(thread_id);
                    }, i));
                }

                for (auto& thread : threads) {
                    thread.join();
                }

                for (auto& result : results) {
                    awake_count += result;
                }

            } catch (const std::exception& e) {
                std::cerr << "Exception in BUStep: " << e.what() << std::endl;
                throw e;
            }
            return awake_count;
        }

        gapbs::pvector<std::atomic<int64_t>> bfsExperiments::bfs(uint64_t source) {
            gapbs::pvector<std::atomic<int64_t>> distances = init_distances();
            distances[source] = 0;

            gapbs::SlidingQueue<int64_t> queue(m_snapshot->vertex_count());
            queue.push_back(source);
            queue.slide_window();

            gapbs::Bitmap curr(m_snapshot->vertex_count());
            curr.reset();
            gapbs::Bitmap front(m_snapshot->vertex_count());
            front.reset();

            int64_t edges_to_check = m_snapshot->edge_count();
            int64_t scout_count = m_snapshot->degree(source, false);
            int64_t distance = 1;

            while(!queue.empty()) {
                if (scout_count > edges_to_check / m_alpha) {
                    int64_t awake_count, old_awake_count;
                    QueueToBitmap(queue, front);
                    awake_count = queue.size();
                    queue.slide_window();

                    do {
                        old_awake_count = awake_count;
                        awake_count = BUStep(distances, distance, front, curr);
                        front.swap(curr);
                        distance++;
                    } while ((awake_count >= old_awake_count) || (awake_count > m_snapshot->vertex_count() / m_beta));
                    BitmapToQueue(m_snapshot->vertex_count(), front, queue);
                    scout_count = 1;

                } else {
                    edges_to_check -= scout_count;
                    scout_count = TDStep(distances, distance, queue);
                    queue.slide_window();
                    distance++;
                }
            }
            
            return distances;
        }

        void bfsExperiments::run_gapbs_bfs(uint64_t source, std::vector<std::pair<uint64_t , int64_t>> & external_ids) {
            uint64_t physical_source = m_snapshot->logical2physical(source);
            auto start = std::chrono::high_resolution_clock::now();

            auto distances = bfs(physical_source);

            std::vector<std::thread> threads;
            uint64_t chunk_size = (m_snapshot->vertex_count() + m_num_threads - 1) / m_num_threads;
            external_ids.reserve(m_interface->vertex_count());

            m_interface->set_max_threads(m_num_threads);
            for (int i = 0; i < m_num_threads; i++) {
                threads.emplace_back(std::thread([this, &distances, chunk_size, &external_ids] (int thread_id) {
                    m_interface->init_thread(thread_id);
                    uint64_t start = thread_id * chunk_size;
                    // int end = std::min(start + chunk_size, m_snapshot->vertex_count());
                    uint64_t size = m_interface->vertex_count();
                    uint64_t end = start + chunk_size;
                    if (end > size) end = size;
                    
                    auto snapshot_local = m_snapshot->clone();

                    for (int64_t u = start; u < end; u++) {
                        external_ids[u] = std::make_pair(snapshot_local->physical2logical(u), distances[u].load());
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
            std::cout << "Gapbs BFS took " << duration.count() << " milliseconds" << std::endl;
            
            // for (auto pair : external_ids) {
            //     std::cout << pair.first << " " << pair.second << std::endl;
            // }
            std::cout << external_ids.size();
        }

    } // namespace bfsExperiments
} // namespace driver
