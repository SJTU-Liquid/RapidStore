#ifndef SSSP_HPP
#define SSSP_HPP

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
        class ssspExperiments {
            const int m_num_threads;
            const int m_granularity;

            double m_delta;
            std::mutex m_mutex;

            std::shared_ptr<driver::interface::graphalyticsInterface> m_interface;
            std::shared_ptr<driver::interface::graphalyticsInterface::snapshot> m_snapshot;
        public:
            ssspExperiments(const int num_threads, const int granularity, const double delta, std::shared_ptr<driver::interface::graphalyticsInterface> interface);
            ~ssspExperiments();

            void run_sssp(uint64_t source, std::vector<std::pair<uint64_t, double>> & external_ids);
        private:
            gapbs::pvector<double> sssp(uint64_t source);
        };
    } // namespace algorithm
} // namespace driver

#endif // SSSP_HPP
