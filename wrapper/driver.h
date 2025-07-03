#pragma once
#include <random>
#include <string>
#include <future>
#include <queue>
#include <vector>
#include <thread>
#include <pthread.h>
#include <condition_variable>
#include <sys/types.h>
#include <unistd.h>
#include <linux/perf_event.h>
#include <asm/unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>

#include "utils/log/log.h"
#include "algorithms/BFS.h"
#include "algorithms/SSSP.h"
#include "algorithms/PR.h"
#include "algorithms/WCC.h"
#include "algorithms/TC.h"
#include "ittnotify.h"

typedef std::pair<double, vertexID> pdv;

#define DEBUG
#define COUT_DEBUG_FORCE(msg) { std::cout << __FUNCTION__ << msg << std::endl; }
#ifdef DEBUG
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

void bind_thread_to_core(std::thread &t, int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}

void generate_path_type(std::string & path, operationType type);

class Barrier {
public:
    explicit Barrier(std::size_t count) : count(count), waiting(0) {}

    void arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mtx);
        ++waiting;
        if (waiting == count) {
            waiting = 0;
            cv.notify_all();
        } else {
            cv.wait(lock, [this] { return waiting == 0; });
        }
    }

private:
    std::mutex mtx;
    std::condition_variable cv;
    std::size_t count;
    std::size_t waiting;
};


template <class F, class S>
class Driver {
private:    
    F& m_method;
    int m_num_threads;
    std::string m_workload_dir;
    std::string m_output_dir;
    const DriverConfig& m_config;

    void read_stream(const std::string & stream_path, std::vector<operation> & stream);
    void initialize_graph(std::vector<operation>* stream);
    void execute_insert_delete(const std::string & target_path, const std::string & output_path);
    void execute_batch_insert(const std::string &target_path, const std::string &output_path);
    void execute_insert_real_ldbc(const std::string & target_path);
    void execute_update(const std::string & target_path, const std::string & output_path, int repeat_times);
    void execute_microbenchmarks(const std::string & target_path, const std::string & output_path, operationType op_type, int num_threads);
    void execute_concurrent(const std::string & target_path, const std::string & output_path, operationType type);
    void execute_query();
    void execute_mixed_reader_writer(const std::string & target_path, const std::string & target_path2, const std::string & output_path);
    void execute_qos(const std::string & target_path_search, const std::string target_path_scan, const std::string & output_path);
    void bfs(const S & snapshot, int thread_id, vertexID source, std::vector<vertexID> & result);
    void sssp(const S & snapshot, int thread_id, vertexID source, std::vector<double> & result);
    void wcc(const S & snapshot, int thread_id, std::vector<int> & result);
    void page_rank(const S & snapshot, int thread_id, double damping_factor, int num_iterations, std::vector<double> & result, std::vector<uint64_t> & degree_list);

public:
    Driver(F &method, const DriverConfig & config);
    ~Driver() = default;
    void execute(operationType type, targetStreamType ts_type);
};

template <class F, class S>
Driver<F, S>::Driver(F &method, const DriverConfig & config) : m_method(method), m_config(config), m_workload_dir(config.workload_dir), m_output_dir(config.output_dir), m_num_threads(config.insert_delete_num_threads) {}

template <class F, class S>
void Driver<F, S>::read_stream(const std::string & stream_path, std::vector<operation> & stream) {
    std::ifstream file(stream_path, std::ios::binary);
    if (file.is_open()) {
        file.seekg(0, std::ios::end);
        size_t fileSize = file.tellg();
        file.seekg(0, std::ios::beg);

        size_t numElements = fileSize / sizeof(operation);

        stream.resize(numElements);

        file.read(reinterpret_cast<char*>(stream.data()), numElements * sizeof(operation));
    }
    file.close();
}
int parseLine(char* line){
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char* p = line;
    while (*p <'0' || *p > '9') p++;
    line[i-3] = '\0';
    i = atoi(p);
    return i;
}
int getValue(){
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}


template <class F, class S>
void Driver<F, S>::initialize_graph(std::vector<operation>* stream) {
    auto initial_path = m_workload_dir + "/initial_stream_insert_general.stream";
//    auto initial_path = m_workload_dir + "/target_stream_insert_full.stream";
    read_stream(initial_path, *stream);
    auto real_stream = new std::vector<wrapper::PUU>();
    real_stream->reserve(stream->size());
    for(auto edge:*stream) {
        real_stream->push_back({edge.e.source, edge.e.destination});
    }
    delete stream;
    uint64_t num_threads = m_config.insert_delete_num_threads;
    std::cout << "num threads: " << num_threads << std::endl;
    uint64_t chunk_size = (real_stream->size() + num_threads - 1) / num_threads;

    std::vector<double> thread_time(num_threads);
    std::vector<std::vector<double>> thread_check_point(num_threads);

    wrapper::set_max_threads(m_method, num_threads + m_config.reader_threads + 32);

    auto start_global = std::chrono::high_resolution_clock::now();
    auto thread_function = [this, &real_stream, chunk_size, &thread_time, &thread_check_point](int thread_id) {
        wrapper::init_thread(m_method, thread_id);
        auto start_time = std::chrono::high_resolution_clock::now();

        uint64_t start = thread_id * chunk_size;
        uint64_t end = std::min(start + chunk_size, real_stream->size());
        for (uint64_t j = start; j < end; j++) {
//            if ((j - start) % 100000 == 0) {
//                std::cout << "Inserting edge " << j - start << "/ " << end - start << std::endl;
//            }
            auto edge = real_stream->at(j);
            wrapper::insert_edge(m_method, edge.first, edge.second, 0.0);
//            if(!wrapper::has_edge(m_method, edge.first, edge.second)) {
//                wrapper::has_edge(m_method, edge.first, edge.second);
//                abort();
//            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        thread_time[thread_id] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
        wrapper::end_thread(m_method, thread_id);
    };

    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; i++) {
        futures.push_back(std::async(std::launch::async, thread_function, i));
    }

    for (auto& future : futures) {
        future.get();
    }

    auto end_global = std::chrono::high_resolution_clock::now();
    auto duration_global = std::chrono::duration_cast<std::chrono::nanoseconds>(end_global - start_global);

    log_info("global duration: %ld", duration_global.count());
//    double global_speed = static_cast<double>(target_stream.size()) / duration_global.count() * 1000000.0;
    double global_speed = static_cast<double>(real_stream->size()) / duration_global.count() * 1000000.0;

    log_info("global speed: %.6lf", global_speed);
    delete real_stream;

}


template <class F, class S>
void Driver<F, S>::execute_insert_delete(const std::string &target_path, const std::string &output_path) {
    std::vector<operation> target_stream;
    const uint64_t total_edge_num = 1 << 22;
    read_stream(target_path, target_stream);
//    uint64_t target_degree = m_config.insert_delete_checkpoint_size;
//    uint64_t target_vertex_num = total_edge_num / target_degree;
//    target_stream.resize(total_edge_num);
//    for(uint64_t v = 0; v < target_vertex_num; v++) {
//        for (uint64_t i = 0; i < target_degree; i++) {
//            target_stream[v * target_degree + i] = operation{operationType::INSERT, {v, i, 0.0}};
//        }
//    }
////     shuffle
//    std::shuffle(target_stream.begin(), target_stream.end(), std::mt19937(std::random_device()()));

    uint64_t num_threads = m_config.insert_delete_num_threads;
    std::cout << "num threads: " << num_threads << std::endl;
    std::cout << "target degree: " << target_stream.size() << std::endl;
//    std::cout << "reader threads: " <<  m_config.reader_threads << std::endl;
    uint64_t chunk_size = (target_stream.size() + num_threads - 1) / num_threads;

    std::vector<double> thread_time(num_threads);
    std::vector<std::vector<double>> thread_check_point(num_threads);

    wrapper::set_max_threads(m_method, num_threads);

    int value_before = getValue();
    auto start_global = std::chrono::high_resolution_clock::now();

    auto thread_function = [this, &target_stream, chunk_size, &thread_time, &thread_check_point](int thread_id) {
        wrapper::init_thread(m_method, thread_id);
        auto start_time = std::chrono::high_resolution_clock::now();

        uint64_t start = thread_id * chunk_size;
        uint64_t end = std::min(start + chunk_size, target_stream.size());

        for (uint64_t j = start; j < end; j++) {
            if (j % 10000 == 0 && thread_id == 0) {
                std::cout << "Inserting edge " << j - start << "/ " << end - start << std::endl;
            }
            operation &op = target_stream[j];
            auto edge = op.e;
            wrapper::insert_edge(m_method, edge.source, edge.destination, edge.weight);
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        thread_time[thread_id] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

        wrapper::end_thread(m_method, thread_id);
    };

    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; i++) {
        futures.push_back(std::async(std::launch::async, thread_function, i));
    }

    for (auto& future : futures) {
        future.get();
    }
    auto end_global = std::chrono::high_resolution_clock::now();
    auto duration_global = std::chrono::duration_cast<std::chrono::nanoseconds>(end_global - start_global);

    log_info("global duration: %ld", duration_global.count());
//    double global_speed = static_cast<double>(target_stream.size()) / duration_global.count() * 1000000.0;
    double global_speed = static_cast<double>(target_stream.size()) / duration_global.count() * 1000000.0;

    log_info("global speed: %.6lf", global_speed);

//    int value_after = getValue();
//    log_info("memory usage: %d kb", value_after- value_before);
}


template <class F, class S>
void Driver<F, S>::execute_batch_insert(const std::string &target_path, const std::string &output_path) {
    std::vector<operation> target_stream;
    read_stream(target_path, target_stream);

    uint64_t num_threads = m_config.insert_delete_num_threads;
    uint64_t batch_size = m_config.insert_delete_checkpoint_size;
    uint64_t chunk_size = (target_stream.size() + num_threads - 1) / num_threads;

    std::vector<double> thread_time(num_threads);

    wrapper::set_max_threads(m_method, -32);

    auto start_global = std::chrono::high_resolution_clock::now();

    auto thread_function = [this, &target_stream, batch_size, chunk_size, &thread_time, &start_global](int thread_id) {
        wrapper::init_thread(m_method, thread_id);
        auto start_time = std::chrono::high_resolution_clock::now();

        uint64_t start = thread_id * chunk_size;
        uint64_t end = std::min(start + chunk_size, target_stream.size());
        uint64_t batch_end = start;
        uint64_t log_size = batch_size >= 1024 ? batch_size : 1024;

        for (uint64_t batch_start = start; batch_start < end; batch_start = batch_end) {
            batch_end = std::min(batch_start + batch_size, end);
//            if((batch_start % chunk_size) % log_size == 0) {
//                std::cout << "Inserting edge " << (batch_start % chunk_size) << "/ " << chunk_size << std::endl;
//            }
            wrapper::run_batch_edge_update(m_method, target_stream, batch_start, batch_end, operationType::INSERT);
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        wrapper::end_thread(m_method, thread_id);
    };

    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; i++) {
        futures.push_back(std::async(std::launch::async, thread_function, i));
    }

    for (auto& future : futures) {
        future.get();
    }

    auto end_global = std::chrono::high_resolution_clock::now();
    auto duration_global = std::chrono::duration_cast<std::chrono::nanoseconds>(end_global - start_global);

    log_info("global duration: %ld", duration_global.count());
    double global_speed = static_cast<double>(target_stream.size()) / duration_global.count() * 1000000.0;

    log_info("global speed: %.6lf", global_speed);
}


template <class F, class S>
void Driver<F, S>::execute_insert_real_ldbc(const std::string & target_path) {
    auto real_stream = new std::vector<wrapper::PUU>();
    auto initial_path = m_workload_dir + "/initial_stream_insert_general.stream";
    auto stream = new std::vector<operation>();
    read_stream(initial_path, *stream);
    for(auto edge: *stream) {
        real_stream->push_back({edge.e.source, edge.e.destination});
    }
    delete stream;
    std::cout << real_stream->size() << std::endl;
    // second part
    initial_path = m_workload_dir + "/target_stream_insert_general.stream";
    stream = new std::vector<operation>();
    read_stream(initial_path, *stream);
    for(auto edge: *stream) {
        real_stream->push_back({edge.e.source, edge.e.destination});
    }
    delete stream;
    std::cout << real_stream->size() << std::endl;

    uint64_t num_threads = m_config.insert_delete_num_threads;
    std::cout << "num threads: " << num_threads << std::endl;
    uint64_t chunk_size = (real_stream->size() + num_threads - 1) / num_threads;

    std::vector<double> thread_time(num_threads);
    std::vector<std::vector<double>> thread_check_point(num_threads);

    wrapper::set_max_threads(m_method, num_threads + m_config.reader_threads + 32);

    auto start_global = std::chrono::high_resolution_clock::now();
    auto thread_function = [this, &real_stream, chunk_size, &thread_time, &thread_check_point](int thread_id) {
        wrapper::init_thread(m_method, thread_id);
        auto start_time = std::chrono::high_resolution_clock::now();

        uint64_t start = thread_id * chunk_size;
        uint64_t end = std::min(start + chunk_size, real_stream->size());
        for (uint64_t j = start; j < end; j++) {
            if ((j - start) % 10000 == 0 && thread_id == 0) {
                std::cout << "Inserting edge " << j - start << "/ " << end - start << std::endl;
            }
            auto edge = real_stream->at(j);
            wrapper::insert_edge(m_method, edge.first, edge.second, 0.0);
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        thread_time[thread_id] = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
        wrapper::end_thread(m_method, thread_id);
    };

    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; i++) {
        futures.push_back(std::async(std::launch::async, thread_function, i));
    }

    for (auto& future : futures) {
        future.get();
    }

    auto end_global = std::chrono::high_resolution_clock::now();
    auto duration_global = std::chrono::duration_cast<std::chrono::nanoseconds>(end_global - start_global);

    log_info("global duration: %ld", duration_global.count());
//    double global_speed = static_cast<double>(target_stream.size()) / duration_global.count() * 1000000.0;
    double global_speed = static_cast<double>(real_stream->size()) / duration_global.count() * 1000000.0;

    log_info("global speed: %.6lf", global_speed);
    delete real_stream;
}

template <class F, class S>
void Driver<F, S>::execute_update(const std::string & target_path, const std::string & output_path, int repeat_times) {
    // Use execute_microbenchmarks() below.
}

// Note: function for update evaluation
//template <class F, class S>
//void Driver<F, S>::execute_microbenchmarks(const std::string & target_path, const std::string & output_path, operationType op_type, int num_threads) {
//    std::vector<operation> target_stream;
//    read_stream(target_path, target_stream);
//    auto initial_size = target_stream.size();
//    for (int i = 0; i < m_config.repeat_times; i++) {
//        for (int j = 0; j < initial_size; j++) {
//            target_stream.push_back(target_stream[j]);
//        }
//    }
//    if (target_stream.size() == 0) return;
//
//    std::vector<std::thread> threads;
//    uint64_t chunk_size = (target_stream.size() + num_threads - 1) / num_threads;
//    std::vector<double> thread_time(num_threads, 0.0);
//    std::vector<double> thread_speed(num_threads, 0.0);
//    wrapper::set_max_threads(m_method, num_threads);
//
//    uint64_t check_point_size = m_config.mb_checkpoint_size;
//    std::vector<std::vector<double>> thread_check_point(num_threads);
//    auto start = std::chrono::high_resolution_clock::now();
////    auto snapshot = wrapper::get_shared_snapshot(m_method);
//
//    std::vector<uint64_t> thread_sum(num_threads, 0);
//    std::atomic<uint64_t> scan_neighbor_size(0);
//    std::atomic<uint64_t> dest_sum(0);
//
//    std::cout << "Warning: The micro-benchmark has been changed to update exp." << std::endl;
//    std::cout << "Iteration num: " << m_config.insert_delete_checkpoint_size << std::endl;
//
//    auto worker = [this, &target_stream, chunk_size, &thread_time, &thread_speed, &thread_sum, &dest_sum, &thread_check_point, check_point_size](int thread_id) {
//        uint64_t start = thread_id * chunk_size;
//        uint64_t size = target_stream.size();
//        uint64_t end = start + chunk_size;    // TOOD debug
//        if (end > size) end = size;
//
//        auto start_time = std::chrono::high_resolution_clock::now();
//        wrapper::init_thread(m_method, thread_id);
////        auto snapshot_local = wrapper::snapshot_clone(snapshot);
//
//        uint64_t sum = 0;
//        uint64_t valid_sum = 0;
//
//        try {
//            for(int i = 0; i < m_config.insert_delete_checkpoint_size; i ++) {
//                for (uint64_t j = start; j < end; j++) {
//                    if ((j - start) % 100000 == 0) {
//                        std::cout << "Deleting edge " << j - start << "/ " << end - start << std::endl;
//                    }
//                    const operation& op = target_stream[j];
//                    const weightedEdge& edge = op.e;
//
//                    wrapper::remove_edge(m_method, edge.source, edge.destination);
//                }
//                for (uint64_t j = start; j < end; j++) {
//                    if ((j - start) % 100000 == 0) {
//                        std::cout << "Inserting edge " << j - start << "/ " << end - start << std::endl;
//                    }
//                    const operation& op = target_stream[j];
//                    const weightedEdge& edge = op.e;
//                    wrapper::insert_edge(m_method, edge.source, edge.destination);
//                }
//            }
//        } catch (...) {}
//
//        auto end_time = std::chrono::high_resolution_clock::now();
//        wrapper::end_thread(m_method, thread_id);
//    };
//
//    for (int i = 0; i < num_threads; i++) {
//        threads.emplace_back(worker, i);
//        bind_thread_to_core(threads[i], i % std::thread::hardware_concurrency());
//    }
//
//    for (auto& thread : threads) {
//        thread.join();
//    }
//
//    auto end = std::chrono::high_resolution_clock::now();
//    uint64_t duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
//
//    double global_speed = static_cast<double>(target_stream.size() * m_config.insert_delete_checkpoint_size * 2) / duration * 1000000.0;
//    log_info("global speed: %.6lf", global_speed);
//
//}

// Note: function for search/scan evaluation
template <class F, class S>
void Driver<F, S>::execute_microbenchmarks(const std::string & target_path, const std::string & output_path, operationType op_type, int num_threads) {
    std::vector<operation> target_stream;
    read_stream(target_path, target_stream);
    auto initial_size = target_stream.size();
    for (int i = 0; i < m_config.repeat_times; i++) {
        for (int j = 0; j < initial_size; j++) {
            target_stream.push_back(target_stream[j]);
        }
    }
    if (target_stream.size() == 0) {
        std::cout << "Empty stream" << std::endl;
        return;
    }


    std::vector<std::thread> threads;
    uint64_t chunk_size = (target_stream.size() + num_threads - 1) / num_threads;
    std::vector<double> thread_time(num_threads, 0.0);
    std::vector<double> thread_speed(num_threads, 0.0);
    wrapper::set_max_threads(m_method, num_threads);

    uint64_t check_point_size = m_config.mb_checkpoint_size;
    std::vector<std::vector<double>> thread_check_point(num_threads);
    auto start = std::chrono::high_resolution_clock::now();
    auto snapshot = wrapper::get_shared_snapshot(m_method);

    std::vector<uint64_t> thread_sum(num_threads, 0);
    std::atomic<uint64_t> scan_neighbor_size(0);
    std::atomic<uint64_t> dest_sum(0);

    auto worker = [this, &target_stream, &snapshot, chunk_size, &thread_time, &thread_speed, &thread_sum, &dest_sum, &thread_check_point, check_point_size](int thread_id) {
        uint64_t start = thread_id * chunk_size;
        uint64_t size = target_stream.size();
        uint64_t end = start + chunk_size;
        if (end > size) end = size;

        auto start_time = std::chrono::high_resolution_clock::now();
        wrapper::init_thread(m_method, thread_id);
        auto snapshot_local = wrapper::snapshot_clone(snapshot);

        uint64_t sum = 0;
        uint64_t valid_sum = 0;

        auto cb = [thread_id, &sum, &valid_sum](vertexID destination, double weight) {
            valid_sum += destination;
            sum += 1;
        };

        try {
            for (uint64_t j = start; j < end; j++) {
                const operation& op = target_stream[j];
                const driver::graph::weightedEdge& edge = op.e;

                switch (op.type) {
                    case operationType::GET_VERTEX:
                        wrapper::snapshot_has_vertex(snapshot_local, edge.source);
                        break;
                    case operationType::GET_EDGE: {
                        sum += wrapper::snapshot_has_edge(snapshot_local, edge.source, edge.destination);
                        break;
                    }
                    case operationType::GET_WEIGHT:
                        wrapper::snapshot_get_weight(snapshot_local, edge.source, edge.destination);
                        break;
                    case operationType::GET_NEIGHBOR:
                        wrapper::snapshot_get_neighbors_addr(snapshot_local, edge.source);
                        break;
                    case operationType::SCAN_NEIGHBOR:
                    {
                        wrapper::snapshot_edges(snapshot_local, edge.source, cb, true);
                        break;
                    }
                    default:
                        throw std::runtime_error("Invalid operation type in target stream\n");
                }

                if ((j + 1) % check_point_size == 0) {
                    auto mid_time = std::chrono::high_resolution_clock::now();
                    thread_check_point[thread_id].push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(mid_time - start_time).count());
                }
            }
        } catch (...) {}

        auto end_time = std::chrono::high_resolution_clock::now();
        wrapper::end_thread(m_method, thread_id);

        std::cout << sum << ' ' << valid_sum << std::endl;

        double time = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
        thread_time[thread_id] = time;

        auto global_type = target_stream[0].type;
        if (global_type == operationType::GET_VERTEX || global_type == operationType::GET_EDGE || global_type == operationType::GET_WEIGHT || global_type == operationType::GET_NEIGHBOR) {
            thread_speed[thread_id] = static_cast<double>(end - start) / time * 1000000.0;
        } else if (global_type == operationType::SCAN_NEIGHBOR) {
            thread_speed[thread_id] = static_cast<double>(sum) / time * 1000000.0;
        }

        thread_sum[thread_id] = sum;
        dest_sum.fetch_add(valid_sum, std::memory_order_relaxed);
    };

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker, i);
        bind_thread_to_core(threads[i], i % std::thread::hardware_concurrency());
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    uint64_t duration_sum = 0;

    auto global_type = target_stream[0].type;
    double global_speed;
    double average_speed;

    for (int i = 0; i < num_threads; i++) {
//        log_info("thread %d time: %.6lf", i, thread_time[i]);
        duration_sum += thread_time[i];
    }

//    log_info("global duration: %.6lf", duration);

    if (global_type == operationType::GET_VERTEX || global_type == operationType::GET_EDGE || global_type == operationType::GET_WEIGHT || global_type == operationType::GET_NEIGHBOR) {
        for (int i = 0; i < num_threads; i++) {
//            log_info("thread %d speed: %.6lf keps", i, thread_speed[i]);
        }
        global_speed = static_cast<double>(target_stream.size()) / duration * 1000000.0;
        average_speed = static_cast<double>(target_stream.size()) / duration_sum * 1000000.0;
    } else {
        for (int i = 0; i < num_threads; i++) {
            log_info("thread %d speed: %.6lf", i, thread_speed[i]);
            scan_neighbor_size.fetch_add(thread_sum[i], std::memory_order_relaxed);
        }
        global_speed = static_cast<double>(scan_neighbor_size.load()) / duration * 1000000.0;
        average_speed = static_cast<double>(scan_neighbor_size.load()) / duration_sum * 1000000.0;
    }

    log_info("global speed: %.6lf", global_speed);
    log_info("average speed: %.6lf", average_speed);
}


template <class F, class S>
void Driver<F, S>::execute_concurrent(const std::string & target_path, const std::string & output_path, operationType type) {
    // Deprecated‌
}

template <class F, class S>
void Driver<F, S>::execute_query() {
    FILE *log_file;
    auto snapshot = wrapper::get_shared_snapshot(m_method);
    
    for (auto &num_threads : m_config.query_num_threads) {
        for (operationType op_type : m_config.query_operation_types) {
            std::string target_path = m_workload_dir + "/target_stream_";
            std::string output_path = m_output_dir + "/output_" + std::to_string(num_threads) + "_";
            
            generate_path_type(output_path, op_type);

            log_file = fopen(output_path.c_str(), "w");
            if (log_file == NULL) {
                std::cerr << "unable to open file" << std::endl;
                exit(0);
            }
            log_set_fp(log_file);
            
            try {
                if (op_type == operationType::BFS) {
                    std::vector<std::pair<uint64_t, int64_t>> external_ids;
                    bfsExperiments<F, S> bfs(num_threads, m_config.alpha, m_config.beta, m_method, snapshot);
                    bfs.run_gapbs_bfs(m_config.bfs_source, external_ids);
                }

                else if (op_type == operationType::SSSP) {
                    std::vector<std::pair<uint64_t, double>> external_ids;
                    ssspExperiments<F, S> sssp(num_threads, m_config.delta, m_method, snapshot);
                    sssp.run_sssp(m_config.sssp_source, external_ids);
                }

                else if (op_type == operationType::PAGE_RANK) {
                    std::vector<std::pair<uint64_t, double>> external_ids;
                    pageRankExperiments<F, S> pr(num_threads, m_config.num_iterations, m_config.damping_factor, m_method, snapshot);
                    pr.run_page_rank(external_ids);
                }

                else if (op_type == operationType::WCC) {
                    std::vector<std::pair<uint64_t, int64_t>> external_ids;
                    wccExperiments<F, S> wcc(num_threads, m_method, snapshot);
                    wcc.run_wcc(external_ids);
                }

                else if (op_type == operationType::TC) {
                    TriangleCounting<F, S> tc(m_method, snapshot);
                    tc.run_tc();
                } 

//                else if (op_type == operationType::TC_OP) {
//                    TriangleCounting_optimized<F, S> tc(m_method, snapshot);
//                    tc.run_tc();
//                }

                else {
                    throw std::runtime_error("Invalid operation type\n");
                }
            }
            catch (std::exception & e) {
                std::cerr << e.what() << std::endl;
                std::cerr << "error" << std::endl;
            }
            fclose(log_file);
        }
    }
}

template <class F, class S>
void Driver<F, S>::bfs(const S & snapshot,int thread_id, vertexID source, std::vector<vertexID> & result) {
    std::queue<vertexID> bfs_queue;
    bfs_queue.push(source);
    bool * visited;
    visited = new bool [wrapper::size(snapshot)];
    memset(visited, 0, sizeof(bool) * wrapper::size(snapshot));
    const int INF = std::numeric_limits<int>::max();
    for (auto & node : result) node = INF;
    result[source] = 0;

    while(!bfs_queue.empty()) {
        vertexID cur_src = bfs_queue.front();
        visited[cur_src] = true;
        bfs_queue.pop();
        vertexID level = result[cur_src] + 1;

        std::function<uint64_t (vertexID, double)> cb = [visited, level, &bfs_queue, &result](vertexID destination, double weight) -> uint64_t {
            if(!visited[destination]) {
                visited[destination] = true;
		        bfs_queue.push(destination);
            	result[destination] = level;
	        }
            return 0;
        };

        wrapper::snapshot_edges(snapshot, cur_src, cb, false);
    }
    delete visited;
}

template <class F, class S>
void Driver<F, S>::sssp(const S & snapshot,int thread_id, vertexID source, std::vector<double> & result) {
    std::priority_queue<pdv, std::vector<pdv>, std::greater<pdv>> sssp_queue;
    sssp_queue.push({0, source});

    const double INF = std::numeric_limits<double>::max();
    for (auto & res : result) res = INF;
    result[source] = 0;
    
    while(!sssp_queue.empty()) {
        vertexID cur_src = sssp_queue.top().second;
        double cur_dist = sssp_queue.top().first;
        sssp_queue.pop();

        if (cur_dist > result[cur_src]) continue;

        std::function<uint64_t (vertexID, double)> cb = [cur_src, cur_dist, &result, &sssp_queue](vertexID destination, double weight) -> uint64_t {
            double next_dist = cur_dist + weight;

            if (next_dist < result[destination]) {
                result[destination] = next_dist;
                sssp_queue.push({next_dist, destination});
            }
            return 0;
        };
        
        wrapper::snapshot_edges(snapshot, cur_src, cb, true);
    }
}

class UnionFind {
public:
    std::vector<vertexID> root;

    UnionFind(vertexID size) : root(size) {
        for (vertexID i = 0; i < size; i++) {
            root[i] = i;
        }
    }

    int find(vertexID x) {
        if (x == root[x]) {
            return x;
        }
        return root[x] = find(root[x]);
    }

    void unite(vertexID x, vertexID y) {
        vertexID rootX = find(x);
        vertexID rootY = find(y);
        if (rootX != rootY) {
            root[rootY] = rootX;
        }
    }
};

template <class F, class S>
void Driver<F, S>::wcc(const S & snapshot,int thread_id, std::vector<int> & result) {
    vertexID size = wrapper::snapshot_vertex_count(snapshot);
    UnionFind uf(size);

    for (vertexID source = 0; source < size; source++) {
        std::function<uint64_t (vertexID, double)> cb = [source, &uf](vertexID destination, double weight) -> uint64_t {
            uf.unite(source, destination);
            return 0;
        };

        wrapper::snapshot_edges(snapshot, source, cb, false);

        result[source] = -1;
    }

    vertexID component_cnt = 0;
    for (vertexID i = 0; i < size; i++) {
        vertexID root = uf.find(i);
        if (result[root] == -1) {
            result[root] = component_cnt++;
        }
        result[i] = result[root];
    }
}
static long perf_event_open(struct perf_event_attr *hw_event, pid_t pid,
                            int cpu, int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}
template <class F, class S>
void Driver<F, S>::page_rank(const S &snapshot, int thread_id,
                             double damping_factor, int num_iterations,
                             std::vector<double> &result,
                             std::vector<uint64_t> &degree_list) {
    uint64_t size = wrapper::snapshot_vertex_count(snapshot);
    std::vector<double> outgoing_contrib(size);
    const double init_score = 1.0 / size;
    const double base_score = (1.0 - damping_factor) / size;
    double dangling_sum = 0.0;

    for (vertexID source = 0; source < size; source++) {
        result[source] = init_score;
    }

    for (int iter = 0; iter < num_iterations; iter++) {
        for (vertexID source = 0; source < size; source++) {
            vertexID degree = degree_list[source];
            if (degree == 0) {
                dangling_sum += result[source];
            } else {
                outgoing_contrib[source] = result[source] / degree;
            }
        }

        dangling_sum /= size;

        auto start = std::chrono::high_resolution_clock::now();

        // before
        for (vertexID source = 0; source < size; source++) {
            double incoming_total = 0.0;
            auto cb = [&](uint64_t dst, double w) {
                incoming_total += outgoing_contrib[dst];
            };
            wrapper::snapshot_edges(snapshot, source, cb, false);

            result[source] =
                    base_score + damping_factor * (incoming_total + dangling_sum);
        }
        auto end = std::chrono::high_resolution_clock::now();
        double duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                        .count();
//        log_info("read thread %d check point: %.6lf", iter, duration);
    }
}


// Small lookups + Batch update: for read, sortledton cannot finish a round of lookup before finishing the batch update.
// template <class F, class S>
// void Driver<F, S>::execute_mixed_reader_writer(const std::string & target_path, const std::string & target_path2, const std::string & output_path) {
//     std::cout << "mixed reader writer, thread num: " << m_config.writer_threads << " " << m_config.reader_threads << std::endl;
//     std::vector<operation> target_stream;
//     std::vector<operation> target_stream2;
//     read_stream(target_path, target_stream);
//     read_stream(target_path2, target_stream2);
// //    std::shuffle(target_stream.begin(), target_stream.end(), std::mt19937(std::random_device()()));
// //    wrapper::set_max_threads(m_method, m_num_threads);
//     wrapper::set_max_threads(m_method, -33);    // NOTE special value because of the not-full incompatibility between system and the evaluation framework
//     pthread_barrier_t barrier;
//     pthread_barrier_init(&barrier, nullptr, 32);

//     std::vector<std::thread> writer_threads;
//     uint64_t chunk_size = m_config.writer_threads == 0 ? 0 : (target_stream.size() + m_config.writer_threads - 1) / m_config.writer_threads;
//     std::vector<double> thread_time(m_config.writer_threads);
//     std::vector<double> thread_speed(m_config.writer_threads);
//     uint64_t batch_size = m_config.insert_delete_checkpoint_size;
//     std::vector<std::vector<double>> thread_check_point(m_config.writer_threads);

//     std::vector<std::thread> reader_threads;
//     std::vector<std::vector<double>> reader_execution_time(m_config.reader_threads);

//     auto snapshot = wrapper::get_shared_snapshot(m_method);

//     std::atomic<uint64_t> reader_count{0};
//         reader_threads.emplace_back(
//                 std::thread([this, &reader_execution_time, &snapshot, &reader_count, &target_stream2](int thread_id) {
//                 wrapper::init_thread(m_method, thread_id);
//                 auto snapshot_local = wrapper::snapshot_clone(snapshot);

//                 uint64_t sum = 0;
//                 uint64_t valid_sum = 0;
//                 int round = 0;
//                 while(true) {
//                     auto start_time = std::chrono::high_resolution_clock::now();
//                     for (uint64_t j = 0; j < target_stream2.size(); j++) {
//                         const operation& op = target_stream2[j];
//                         const driver::graph::weightedEdge& edge = op.e;
//                         sum += wrapper::snapshot_has_edge(snapshot_local, edge.source, edge.destination);
//                     }
//                     if(round == 0) {
//                         auto end_time = std::chrono::high_resolution_clock::now();
//                         std::cout << sum << ' ' << valid_sum << std::endl;
//                         double duration = std::chrono::duration_cast<std::chrono::microseconds>(
//                                 end_time - start_time).count();
//                         double global_speed = static_cast<double>(target_stream2.size()) / duration * 1000.0;
//                         log_info("global speed: %.6lf", global_speed);
//                     }
//                     round += 1;
//                 }

//             }, m_config.writer_threads));

//     auto start = std::chrono::high_resolution_clock::now();

//     auto start_global = std::chrono::high_resolution_clock::now();
//     for (int i = 0; i < m_config.writer_threads; i++) {
//         writer_threads.emplace_back(std::thread([this, &target_stream, chunk_size, batch_size] (int thread_id) {
//             wrapper::init_thread(m_method, thread_id);
//             int round = 0;
//             uint64_t start = thread_id * chunk_size;
//             uint64_t end = std::min(start + chunk_size, target_stream.size());
//             uint64_t batch_end = start;
//             uint64_t log_size = batch_size >= 1024 ? batch_size : 1024;

//             for (uint64_t batch_start = start; batch_start < end; batch_start = batch_end) {
//                 batch_end = std::min(batch_start + batch_size, end);
//                 wrapper::run_batch_edge_update(m_method, target_stream, batch_start, batch_end, operationType::INSERT);
//             }

// //            std::cout << "Updating" << std::endl;
//         }, i));
//         bind_thread_to_core(writer_threads[i], i % std::thread::hardware_concurrency());
//     }
//     for (auto &thread: writer_threads) {
//         thread.join();
//     }
//     auto end_global = std::chrono::high_resolution_clock::now();
//     auto duration_global = std::chrono::duration_cast<std::chrono::nanoseconds>(end_global - start_global);
//     double global_speed = static_cast<double>(target_stream.size()) / duration_global.count() * 1000000.0;
// //    log_info("global duration: %ld", duration_global.count());
//     log_info("global speed: %.6lf", global_speed);
//     abort();

//     for (auto &thread: reader_threads) {
//         thread.join();
//     }
// }

//  Small lookups + Batch update: for write
//template <class F, class S>
//void Driver<F, S>::execute_mixed_reader_writer(const std::string & target_path, const std::string & target_path2, const std::string & output_path) {
//    std::cout << "mixed reader writer, thread num: " << m_config.writer_threads << " " << m_config.reader_threads << std::endl;
//    std::vector<operation> target_stream;
//    std::vector<operation> target_stream2;
//    read_stream(target_path, target_stream);
//    read_stream(target_path2, target_stream2);
//    wrapper::set_max_threads(m_method, m_num_threads);
//    pthread_barrier_t barrier;
//    pthread_barrier_init(&barrier, nullptr, 32);
//
//    std::vector<std::thread> writer_threads;
//    uint64_t chunk_size = m_config.writer_threads == 0 ? 0 : (target_stream.size() + m_config.writer_threads - 1) / m_config.writer_threads;
//    std::vector<double> thread_time(m_config.writer_threads);
//    std::vector<double> thread_speed(m_config.writer_threads);
//    uint64_t batch_size = m_config.insert_delete_checkpoint_size;
//    std::vector<std::vector<double>> thread_check_point(m_config.writer_threads);
//
//    std::vector<std::thread> reader_threads;
//    std::vector<std::vector<double>> reader_execution_time(m_config.reader_threads);
//
//    auto snapshot = wrapper::get_shared_snapshot(m_method);
//
//    for (int i = 0; i < m_config.writer_threads; i++) {
//        writer_threads.emplace_back(std::thread([this, &target_stream, chunk_size, batch_size] (int thread_id) {
//            wrapper::init_thread(m_method, thread_id);
//            int round = 0;
//            uint64_t start = thread_id * chunk_size;
//            uint64_t end = std::min(start + chunk_size, target_stream.size());
//            uint64_t batch_end = start;
//            uint64_t log_size = batch_size >= 1024 ? batch_size : 1024;
//            while(true) {
//                for (uint64_t batch_start = start; batch_start < end; batch_start = batch_end) {
//                    batch_end = std::min(batch_start + batch_size, end);
//                    wrapper::run_batch_edge_update(m_method, target_stream, batch_start, batch_end,
//                                                   operationType::INSERT);
//                }
//                for (uint64_t batch_start = start; batch_start < end; batch_start = batch_end) {
//                    batch_end = std::min(batch_start + batch_size, end);
//                    wrapper::run_batch_edge_update(m_method, target_stream, batch_start, batch_end,
//                                                   operationType::DELETE);
//                }
//                if(thread_id == 0)
//                    std::cout << "Updating" << std::endl;
//            }
//        }, i));
//        bind_thread_to_core(writer_threads[i], i % std::thread::hardware_concurrency());
//    }
//    auto start = std::chrono::high_resolution_clock::now();
//
//    auto start_global = std::chrono::high_resolution_clock::now();
//
//    reader_threads.emplace_back(
//        std::thread([this, &reader_execution_time, &snapshot, &target_stream2](int thread_id) {
//            wrapper::init_thread(m_method, thread_id);
//            auto snapshot_local = wrapper::snapshot_clone(snapshot);
//
//            uint64_t sum = 0;
//            uint64_t valid_sum = 0;
//            auto start_time = std::chrono::high_resolution_clock::now();
//            for (uint64_t j = 0; j < target_stream2.size(); j++) {
//                const operation& op = target_stream2[j];
//                const driver::graph::weightedEdge& edge = op.e;
//                sum += wrapper::snapshot_has_edge(snapshot_local, edge.source, edge.destination);
//            }
//            auto end_time = std::chrono::high_resolution_clock::now();
//            std::cout << sum << ' ' << valid_sum << std::endl;
//            double duration = std::chrono::duration_cast<std::chrono::microseconds>(
//                    end_time - start_time).count();
//            double global_speed = static_cast<double>(target_stream2.size()) / duration * 1000.0;
//            log_info("global speed: %.6lf", global_speed);
//
//            wrapper::end_thread(m_method, thread_id);
//            std::cout << sum << ' ' << valid_sum << std::endl;
//    }, m_config.writer_threads));
//    for (auto &thread: reader_threads) {
//        thread.join();
//    }
//    abort();
//
//    for (auto &thread: writer_threads) {
//        thread.join();
//    }
//}

// For read performance with writers - Note: disable the properties to the memory bandwidth contention
template <class F, class S>
void Driver<F, S>::execute_mixed_reader_writer(const std::string & target_path, const std::string & target_path2, const std::string & output_path) {
   std::cout << "mixed reader writer, thread num: " << m_config.writer_threads << " " << m_config.reader_threads << std::endl;
   std::vector<operation> target_stream;
   std::vector<operation> target_stream2;
   read_stream(target_path, target_stream);
   read_stream(target_path2, target_stream2);
   std::shuffle(target_stream.begin(), target_stream.end(), std::mt19937(std::random_device()()));
   wrapper::set_max_threads(m_method, m_num_threads);
   pthread_barrier_t barrier;
   pthread_barrier_init(&barrier, nullptr, 32);

   std::vector<std::thread> writer_threads;
   uint64_t chunk_size = m_config.writer_threads == 0 ? 0 : (target_stream.size() + m_config.writer_threads - 1) / m_config.writer_threads;
   std::vector<double> thread_time(m_config.writer_threads);
   std::vector<double> thread_speed(m_config.writer_threads);
   uint64_t check_point_size = m_config.insert_delete_checkpoint_size;
   std::vector<std::vector<double>> thread_check_point(m_config.writer_threads);

   std::vector<std::thread> reader_threads;
   std::vector<std::vector<double>> reader_execution_time(m_config.reader_threads);

   auto snapshot = wrapper::get_shared_snapshot(m_method);
   uint64_t num_vertices = wrapper::snapshot_vertex_count(snapshot);
   std::vector<uint64_t> degree_list(num_vertices);
   for (uint64_t i = 0; i < num_vertices; i++) degree_list[i] = wrapper::degree(m_method, i);

   auto start = std::chrono::high_resolution_clock::now();
   for (int i = 0; i < m_config.writer_threads; i++) {
       writer_threads.emplace_back(std::thread([this, &target_stream, chunk_size] (int thread_id) {
           wrapper::init_thread(m_method, thread_id);
           uint64_t start = thread_id * chunk_size;
           uint64_t size = target_stream.size();
           uint64_t end = start + chunk_size;
           if (end > size) end = size;
           int round = 0;
           do {
               for (uint64_t j = start; j < end; j++) {
                   if ((j - start) == 250000 && thread_id == 0 && round == 0) {
                       std::cout << "Updating edge " << (j - start) << "/ " << (end - start) << std::endl;
                   }
                   operation &op = target_stream[j];
                   auto edge = op.e;
                   wrapper::remove_edge(m_method, edge.source, edge.destination);
                   wrapper::insert_edge(m_method, edge.source, edge.destination);
               }
//                std::cout << "Updating" << std::endl;
           } while (true);
       }, i));
       bind_thread_to_core(writer_threads[i], i % std::thread::hardware_concurrency());
   }

       sleep(2);
       __itt_resume();
   std::atomic<uint64_t> reader_count{0};
   for (int i = 0; i < m_config.reader_threads; i++) {
       reader_threads.emplace_back(
               std::thread([this, &reader_execution_time, &snapshot, &degree_list, &reader_count](int thread_id) {
                   wrapper::init_thread(m_method, thread_id);
                   auto snapshot_local = wrapper::snapshot_clone(snapshot);

                   auto start_time = std::chrono::high_resolution_clock::now();
                   std::vector<double> result(wrapper::snapshot_vertex_count(snapshot_local));
                   page_rank(snapshot_local, thread_id, m_config.damping_factor, m_config.num_iterations,
                             result, degree_list);

                   wrapper::end_thread(m_method, thread_id);
                   auto end_time = std::chrono::high_resolution_clock::now();
                   double duration = std::chrono::duration_cast<std::chrono::microseconds>(
                           end_time - start_time).count();
                   log_info("read thread %d check point: %.6lf", reader_count++, duration);
               }, i + m_config.writer_threads));
   }

   for (auto &thread: reader_threads) {
       thread.join();
   }
   abort();

   __itt_pause();
   for (auto &thread: writer_threads) {
       thread.join();
   }
}

// For write performance with readers, Note: disable the properties to minimize the memory bandwidth contention. To measure the memory bandwidth accurately, we fork the process, devide the cache and measure it with Intel Vtune lib.
//template <class F, class S>
//void Driver<F, S>::execute_mixed_reader_writer(const std::string & target_path, const std::string & target_path2, const std::string & output_path) {
//    std::cout << "mixed reader writer, thread num: " << m_config.writer_threads << " " << m_config.reader_threads << std::endl;
//    std::vector<operation> target_stream;
//    std::vector<operation> target_stream2;
//    read_stream(target_path, target_stream);
//    read_stream(target_path2, target_stream2);
//    wrapper::set_max_threads(m_method, m_num_threads);
//    pthread_barrier_t barrier;
//    pthread_barrier_init(&barrier, nullptr, 32);
//
//    std::vector<std::thread> writer_threads;
//    uint64_t chunk_size = m_config.writer_threads == 0 ? 0 : (target_stream.size() + m_config.writer_threads - 1) / m_config.writer_threads;
//    std::vector<double> thread_time(m_config.writer_threads);
//    std::vector<double> thread_speed(m_config.writer_threads);
//    uint64_t check_point_size = m_config.insert_delete_checkpoint_size;
//    std::vector<std::vector<double>> thread_check_point(m_config.writer_threads);
//
//    std::vector<std::thread> reader_threads;
//    std::vector<std::vector<double>> reader_execution_time(m_config.reader_threads);
//
//    int pipefd[2];
//
//    if (pipe(pipefd) == -1) {
//        perror("pipe");
//        return;
//    }
//    auto snapshot = wrapper::get_shared_snapshot(m_method);
//    uint64_t num_vertices = wrapper::snapshot_vertex_count(snapshot);
//    std::vector<uint64_t> degree_list(num_vertices);
//    for (uint64_t i = 0; i < num_vertices; i++) degree_list[i] = wrapper::degree(m_method, i);
//
//    auto pid = fork();
//
//    if(pid == 0) {
//        close(pipefd[1]);
//        char buffer;
//        std::cout << "child" << std::endl;
//        read(pipefd[0], &buffer, 1);
//        close(pipefd[0]);
//
//        for (int i = 0; i < m_config.reader_threads; i++) {
//            reader_threads.emplace_back(
//                    std::thread([this, &reader_execution_time, &snapshot, &degree_list](int thread_id) {
//                        auto start_time = std::chrono::high_resolution_clock::now();
//                        wrapper::init_thread(m_method, thread_id);
//                        auto snapshot_local = wrapper::snapshot_clone(snapshot);
//                        sleep(thread_id - m_config.writer_threads);
//                        std::cout << "Reader " << (thread_id - m_config.writer_threads) << " set." << std::endl;
//                        while(true) {
//                            std::vector<double> result(wrapper::snapshot_vertex_count(snapshot_local));
//                            page_rank(snapshot_local, thread_id, m_config.damping_factor, m_config.num_iterations,
//                                      result, degree_list);
//                            std::cout << "PR finished." << std::endl;
//                        }
//                    }, i + m_config.writer_threads));
//        }
//
//        for (auto &thread: reader_threads) {
//            thread.join();
//        }
//        std::cout << "Reader end" << std::endl;
//    } else {
//        close(pipefd[0]);
//        std::cout << "parent: " << pid << std::endl;
//        char c = 'c';
//        std::string command = "echo " + std::to_string(pid) + " | sudo tee /sys/fs/resctrl/rdt_group_demo/tasks";
//        int ret = system(command.c_str());
//        if (ret != 0) {
//            std::cerr << "Failed to execute command: " << command << std::endl;
//        }
//        auto snapshot_local = wrapper::snapshot_clone(snapshot);
//        write(pipefd[1], &c, 1);
//        close(pipefd[1]);
//        sleep(m_config.reader_threads + 2);
//        __itt_resume();
//        auto start = std::chrono::high_resolution_clock::now();
//        for (int i = 0; i < m_config.writer_threads; i++) {
//            writer_threads.emplace_back(std::thread([this, &target_stream, chunk_size] (int thread_id) {
//                wrapper::init_thread(m_method, thread_id);
//                uint64_t start = thread_id * chunk_size;
//                uint64_t size = target_stream.size();
//                uint64_t end = start + chunk_size;
//                if (end > size) end = size;
//
//                for(int i = 0; i < m_config.insert_delete_checkpoint_size; i++) {
//                    for (uint64_t j = start; j < end; j++) {
//                        if (j % 100000 == 0 && thread_id == 0) {
//                            std::cout << "Insert edge " << j - start << "/ " << end - start << std::endl;
//                        }
//                        operation &op = target_stream[j];
//                        auto edge = op.e;
//                        wrapper::insert_edge(m_method, edge.source, edge.destination);
//                    }
//                }
//            }, i));
//        }
//
//        for (auto &thread: writer_threads) {
//            thread.join();
//        }
//        __itt_pause();
//
//        auto end = std::chrono::high_resolution_clock::now();
//        double duration = std::chrono::duration_cast<std::chrono::microseconds>(
//                end - start).count();
//        auto global_speed = static_cast<double>(target_stream.size()) / duration * 1000000.0;
//
//        log_info("global speed: %.6lf", global_speed);
//
//        // kill the read
////        command = "kill -9 " + std::to_string(pid);
////        ret = system(command.c_str());
////        if (ret != 0) {
////            std::cerr << "Failed to execute command: " << command << std::endl;
////        }
//
//
//        int status;
//        waitpid(pid, &status, 0);
//        if (WIFEXITED(status)) {
//            std::cout << "Child exited with status: " << WEXITSTATUS(status) << std::endl;
//        } else if (WIFSIGNALED(status)) {
//            std::cout << "Child was killed by signal: " << WTERMSIG(status) << std::endl;
//        }
//    }
//}


template <class F, class S>
void Driver<F, S>::execute_qos(const std::string & target_path_search, const std::string target_path_scan, const std::string & output_path) {
    // Deprecated‌
}


void generate_path_ts(std::string & path, targetStreamType ts_type) {
    if (ts_type == targetStreamType::FULL) {
        path += "full.stream";
    }
    else if (ts_type == targetStreamType::GENERAL) {
        path += "general.stream";
    }
    else if (ts_type == targetStreamType::HIGH_DEGREE) {
        path += "high_degree.stream";
    }
    else if (ts_type == targetStreamType::LOW_DEGREE) {
        path += "low_degree.stream";
    }
    else if (ts_type == targetStreamType::UNIFORM) {
        path += "uniform.stream";
    }
    else if (ts_type == targetStreamType::BASED_ON_DEGREE) {
        path += "based_on_degree.stream";
    }
    else {
        throw std::runtime_error("Invalid target stream type\n");
    }
}

void generate_path_type(std::string & path, operationType type) {
    if (type == operationType::INSERT) {
        path += "insert_";
    }
    else if (type == operationType::DELETE) {
        path += "delete_";
    }
    else if (type == operationType::UPDATE) {
        path += "update.stream";
    }
    else if (type == operationType::GET_VERTEX) {
        path += "get_vertex_";
    }
    else if (type == operationType::GET_EDGE) {
        path += "get_edge_";
    }
    else if (type == operationType::GET_WEIGHT) {
        path += "get_weight_";
    }
    else if (type == operationType::SCAN_NEIGHBOR) {
        path += "scan_neighbor_";
    }
    else if (type == operationType::GET_NEIGHBOR) {
        path += "get_neighbor_";
    }
    else if (type == operationType::BFS) {
        path += "bfs.stream";
    }
    else if (type == operationType::SSSP) {
        path += "sssp.stream";
    }
    else if (type == operationType::PAGE_RANK) {
        path += "page_rank.stream";
    }
    else if (type == operationType::WCC) {
        path += "wcc.stream";
    }
    else if (type == operationType::TC) {
        path += "tc.stream";
    }
    else if (type == operationType::TC_OP) {
        path += "tc_op.stream";
    }
    else if (type == operationType::MIXED) {
        path += "mixed.stream";
    }
    else if (type == operationType::QOS) {
        path += "qos_";
    }
    else {
        throw std::runtime_error("Invalid operation type\n");
    }
}

template <class F, class S>
void Driver<F, S>::execute(operationType type, targetStreamType ts_type) {
    std::string vertex_path = m_workload_dir + "/initial_stream_insert_vertex.stream";
    // std::string initial_path = m_workload_dir + "/initial_stream_";
    // std::string target_path = m_workload_dir + "/target_stream_";
    // std::string output_path = m_output_dir + "/output_" + std::to_string(m_num_threads) + "_";
    std::string initial_path, target_path, output_path, target_path_search, target_path_scan;
    std::vector<operation> vertex_stream;
    double mem_total = getValue();
    read_stream(vertex_path, vertex_stream);
    std::vector<vertexID> vertices;
    for (auto op : vertex_stream) vertices.push_back(op.e.source);
    auto mem1 = getValue();
    wrapper::run_batch_vertex_update(m_method, vertices, 0, vertices.size());
    mem_total += getValue() - mem1;
    std::cout << "Total Mem: " << (int)mem_total << std::endl;

    auto initial_stream = new std::vector<operation>();
    FILE *log_file;

    if (type == operationType::QUERY) {
        initial_path = m_workload_dir + "/target_stream_insert_full.stream";
        read_stream(initial_path, *initial_stream);
        mem1 = getValue();
        initialize_graph(initial_stream);
//        execute_insert_real_ldbc(m_workload_dir); // for real ldbc insertion
        mem_total += getValue() - mem1;
        std::cout << "Total Mem: " << (int)mem_total << std::endl;  // NOTE: Aspen do not have property, we manually compute and add its size to Aspen's consumption.
        execute_query();
        return;
    }

    switch (type) {
        case operationType::INSERT: 
        case operationType::DELETE:
            initial_path = m_workload_dir + "/initial_stream_";
            target_path = m_workload_dir + "/target_stream_";
            output_path = m_output_dir + "/output_" + std::to_string(m_config.insert_delete_num_threads) + "_";
            generate_path_type(initial_path, type);
            generate_path_type(target_path, type);
            generate_path_type(output_path, type);

            generate_path_ts(initial_path, ts_type);
            generate_path_ts(target_path, ts_type);
            generate_path_ts(output_path, ts_type);

//            log_file = fopen(output_path.c_str(), "w");
//            if (log_file == NULL) {
//                // create log file
//                log_file = fopen(output_path.c_str(), "w");
//            }
//            log_set_fp(log_file);

            read_stream(initial_path, *initial_stream);
//            initialize_graph(initial_stream);
            mem1 = getValue();
             __itt_resume();
//            execute_insert_real_ldbc(target_path);
            execute_insert_delete(target_path, output_path);
//            execute_batch_insert(target_path, output_path);
            __itt_pause();

            mem_total += getValue() - mem1;
            std::cout << "Total Mem: " << (int)mem_total << std::endl;
//            fclose(log_file);
            break;
        
        case operationType::UPDATE:
            initial_path = m_workload_dir + "/initial_stream_";
            target_path = m_workload_dir + "/target_stream_";
            output_path = m_output_dir + "/output_" + std::to_string(m_config.update_num_threads) + "_";
            generate_path_type(initial_path, type);
            generate_path_type(target_path, type);
            generate_path_type(output_path, type);

            read_stream(initial_path, *initial_stream);
            initialize_graph(initial_stream);

            execute_update(target_path, output_path, m_config.update_repeat_times);
            break;

        case operationType::GET_VERTEX: 
        case operationType::GET_EDGE:
        case operationType::GET_WEIGHT:
        case operationType::SCAN_NEIGHBOR: 
        case operationType::GET_NEIGHBOR: {
            mem1 = getValue();
            initialize_graph(initial_stream);
            mem_total += getValue() - mem1;
            std::cout << "Total Mem: " << (int) mem_total << std::endl;
//            for (int i = 0; i < vertex_stream.size(); i++) {
//                if(i % 100000 == 0) {
//                    std::cout << "checking: " << i << " / " << vertex_stream.size() << std::endl;
//                }
//                auto cur_vertex = vertex_stream.at(i).e.source;
//                auto sum = 0;
//                auto cb = [&sum](vertexID destination, double weight) {
//                    sum += 1;
//                };
//                wrapper::snapshot_edges(snapshot, cur_vertex, cb, false);
//                if (sum != wrapper::snapshot_degree(snapshot, cur_vertex)) {
//                    std::cout << "error: " << cur_vertex << std::endl;
//                    abort();
//                }
//
//            }
//            execute_query();
            for (auto &operationType: m_config.mb_operation_types) {
                for (auto &num_threads: m_config.microbenchmark_num_threads) {
                    for (auto target_type: m_config.mb_ts_types) {
                        ts_type = target_type;

                        target_path = m_workload_dir + "/target_stream_";
                        output_path = m_output_dir + "/output_" + std::to_string(num_threads) + "_";
                        generate_path_type(target_path, operationType);
                        generate_path_ts(target_path, ts_type);

                        generate_path_type(output_path, operationType);
                        generate_path_ts(output_path, ts_type);

//                        log_file = fopen(output_path.c_str(), "w");
//                        if (log_file == NULL) {
//                            // create log file
//                            log_file = fopen(output_path.c_str(), "w");
//                        }
//                        log_set_fp(log_file);

                        try {

                            execute_microbenchmarks(target_path, output_path, operationType, num_threads);
                            // __itt_pause();
                        }
                        catch (std::exception &e) {
                            std::cerr << e.what() << std::endl;
                            std::cerr << "error" << std::endl;
                        }
//                        fclose(log_file);

                    }
                }
            }
            break;
        }

        case operationType::MIXED : {
            initial_path = m_workload_dir + "/initial_stream_insert_general.stream";
            target_path = m_workload_dir + "/target_stream_";
            output_path = m_output_dir + "/output_" + std::to_string(m_config.writer_threads) + "_";
//            generate_path_type(initial_path, operationType::INSERT);
//            generate_path_type(target_path, operationType::INSERT);
            generate_path_type(target_path, operationType::INSERT);

//            generate_path_ts(initial_path, targetStreamType::GENERAL);
            generate_path_ts(target_path, targetStreamType::GENERAL);

            generate_path_type(output_path, type);

            read_stream(initial_path, *initial_stream);
            initialize_graph(initial_stream);

            for (auto & operationType : m_config.mb_operation_types) {
                for (auto & num_threads : m_config.microbenchmark_num_threads) {
                    for (auto target_type : m_config.mb_ts_types) {
                        ts_type = target_type;

                        auto target_path2 = m_workload_dir + "/target_stream_";
                        generate_path_type(target_path2, operationType);
                        generate_path_ts(target_path2, ts_type);

                        execute_mixed_reader_writer(target_path, target_path2, output_path);
                    }
                }
            }


            break;
        }
        
        case operationType::QOS :
            initial_path = m_workload_dir + "/initial_stream_insert_full.stream";
            read_stream(initial_path, *initial_stream);
            initialize_graph(initial_stream);

            target_path_search = m_workload_dir + "/target_stream_";
            generate_path_type(target_path_search, operationType::GET_EDGE);
            generate_path_ts(target_path_search, ts_type);

            target_path_scan = m_workload_dir + "/target_stream_";
            generate_path_type(target_path_scan, operationType::GET_EDGE);
            generate_path_ts(target_path_scan, ts_type);

            output_path = m_output_dir + "/output_";
            generate_path_type(output_path, operationType::QOS);
            generate_path_ts(output_path, ts_type);
            

            log_file = fopen(output_path.c_str(), "w");
            if (log_file == NULL) {
            std::cerr << "unable to open file" << std::endl;
            exit(0);
            }
            log_set_fp(log_file);

            execute_qos(target_path_search, target_path_scan, output_path);
            
            fclose(log_file);
            break;


        default:
            throw std::runtime_error("Invalid operation type\n");
    }
}
