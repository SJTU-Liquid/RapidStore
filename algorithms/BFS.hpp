#ifndef BFS_HPP
#define BFS_HPP

#include <iostream>
#include "interfaces/interfaces.hpp"
#include "readers/reader.hpp"
#include "readers/edgeListReader.hpp"
#include "graph/edge.hpp"
#include "graph/edgeStream.hpp"
#include "libraries/sortledton/third-party/gapbs.h"
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
        class bfsExperiments {
            const int m_num_threads;
            const int m_granularity;
            const int m_alpha;
            const int m_beta;
            std::mutex m_mutex;

            std::shared_ptr<driver::interface::graphalyticsInterface> m_interface;
            std::shared_ptr<driver::interface::graphalyticsInterface::snapshot> m_snapshot;
            
        public:
            bfsExperiments(const int num_threads, const int granularity, const int alpha, const int beta, std::shared_ptr<driver::interface::graphalyticsInterface> interface);
            ~bfsExperiments();
            
            void run_gapbs_bfs(uint64_t src, std::vector<std::pair<uint64_t , int64_t>> & external_ids);
            
        private:
            gapbs::pvector<std::atomic<int64_t>> init_distances();
            gapbs::pvector<std::atomic<int64_t>> bfs(uint64_t source);
            int64_t BUStep(gapbs::pvector<std::atomic<int64_t>>& distances, int64_t distance, gapbs::Bitmap &front, gapbs::Bitmap &next);
            int64_t TDStep(gapbs::pvector<std::atomic<int64_t>>& distances, int64_t distance, gapbs::SlidingQueue<int64_t>& queue);
            void BitmapToQueue(const int64_t size, const gapbs::Bitmap &bm, gapbs::SlidingQueue<int64_t> &queue);
            void QueueToBitmap(const gapbs::SlidingQueue<int64_t> &queue, gapbs::Bitmap &bm);
        };
    } // namespace algorithms
} // namespace driver

#endif // BFS_HPP
