#ifndef TYPES_HPP
#define TYPES_HPP
#include "../graph/edge.hpp"
#include <memory>
#include <string>
#include <vector>

typedef uint64_t vertexID;
typedef uint8_t label;

enum class operationType {
    // write operations
    INSERT,
    DELETE,
    INSERT_VERTEX,
    UPDATE,
    // read operations
    GET_VERTEX,
    GET_EDGE,
    GET_WEIGHT,
    SCAN_NEIGHBOR,
    GET_NEIGHBOR,
    PHYSICAL2LOGICAL,
    LOGICAL2PHYSICAL,
    // analytic operations 
    BFS,
    PAGE_RANK,
    SSSP,
    TC,
    TC_OP,
    WCC,
    QUERY,
    MIXED, 
    QOS
};

struct operation {
    operationType type;
    driver::graph::weightedEdge e;
};

enum class targetStreamType {
    FULL,
    GENERAL,
    HIGH_DEGREE,
    LOW_DEGREE,
    UNIFORM,
    BASED_ON_DEGREE
};

struct Config {
    double timestamp_rate;
    int seed;
    uint64_t num_search{1000000};
    bool test_version_chain{false};
    bool enable_bloom_filter{false};

    Config(double ts_rate) : timestamp_rate(ts_rate) {}
    Config() : timestamp_rate(0.0) {}
};

struct EdgeDriverConfig {
    std::string workload_dir;
    std::string output_dir;
    operationType workload_type{operationType::INSERT};
    targetStreamType target_stream_type{targetStreamType::FULL};

    double timestamp_rate{0.8};
    double initial_graph_rate{0.8};
    std::vector<int> element_sizes;
    int seed;

    uint64_t num_search{1000000};
    uint64_t num_scan{1000000};
    int repeat_times{0};
    bool test_version_chain{false};
    bool is_real_graph{false};
    
    explicit EdgeDriverConfig() {}
};

struct DriverConfig {
    std::string workload_dir;
    std::string output_dir;
    operationType workload_type;
    targetStreamType target_stream_type;

    // insert / delete config
    uint64_t insert_delete_checkpoint_size{10000};
    int insert_delete_num_threads{1};
    int insert_batch_size{100000};

    //update config
    uint64_t update_checkpoint_size{10000};
    int update_num_threads{1};
    int update_repeat_times{10};

    // microbenchmark config
    int repeat_times{0};
    uint64_t mb_checkpoint_size{10000};
    std::vector<int> microbenchmark_num_threads;
    std::vector<operationType> mb_operation_types;
    std::vector<targetStreamType> mb_ts_types;

    // query
    std::vector<int> query_num_threads;
    std::vector<operationType> query_operation_types;

    // bfs
    int alpha{15};
    int beta{18};
    uint64_t bfs_source{0};

    // sssp
    double delta{2.0};
    uint64_t sssp_source{0};

    // pr
    int num_iterations{10};
    double damping_factor{0.85};

    // mixed
    int writer_threads{1};
    int reader_threads{1};

    //qos
    int num_threads_search{8};
    int num_threads_scan{20};
};


template <typename T>
struct Iterator {
    T iterator;
    Iterator(const T & it) : iterator(it) {}

    bool is_valid() {
        return iterator.valid();
    }

    Iterator& operator++() {
        ++iterator;
        return (*this);
    }

    uint64_t operator*() {
        return iterator->dest;
    }
};
#endif // TYPES_HPP
