#ifndef WCC_HPP
#define WCC_HPP

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
        class wccExperiments {
            const int m_num_threads;
            const int m_granularity;

            std::shared_ptr<driver::interface::graphalyticsInterface> m_interface;
            std::shared_ptr<driver::interface::graphalyticsInterface::snapshot> m_snapshot;

        public:
            wccExperiments(const int num_threads, const int granularity, std::shared_ptr<driver::interface::graphalyticsInterface> interface);
            ~wccExperiments();

            void run_wcc(std::vector<std::pair<uint64_t, int64_t>> & external_ids);

        private:
            std::unique_ptr<uint64_t[]> wcc();
            
        };

    } // namespace algorithm
} // namespace driver

#endif // WCC_HPP
