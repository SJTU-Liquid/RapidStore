#ifndef PAGE_RANK_HPP
#define PAGE_RANK_HPP

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
        class pageRankExperiments {
            const int m_num_threads;
            const int m_granularity;
            const uint64_t m_num_iterations;
            const double m_damping_factor;

            std::shared_ptr<driver::interface::graphalyticsInterface> m_interface;
            std::shared_ptr<driver::interface::graphalyticsInterface::snapshot> m_snapshot;

        public:
            pageRankExperiments(const int num_threads, const int granularity, const uint64_t num_iterations, const double damping_factor, std::shared_ptr<driver::interface::graphalyticsInterface> interface);
            ~pageRankExperiments();

            void run_page_rank(std::vector<std::pair<uint64_t, double>> & external_ids);
        private:
            std::unique_ptr<double[]> page_rank();
        };
    } // namespace algorithm
} // namespace driver

#endif // PAGE_RANK_HPP
