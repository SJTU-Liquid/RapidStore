#pragma once

#include "libraries/NeoGraph/include/neo_index.h"
#include "libraries/NeoGraph/include/neo_transaction.h"
#include "libraries/NeoGraph/include/neo_snapshot.h"

#include "../../libraries/NeoGraph/utils/types.h"
#include "../../libraries/NeoGraph/utils/config.h"
#include "graph/edge.hpp"
#include "graph/edgeStream.hpp"
#include "utils/commandLineParser.hpp"
#include "types.hpp"

#include "wrapper.h"
#include "driver.h"

using namespace container;

class Neo_Graph_Wrapper {
private:
    TransactionManager tm;
    const bool m_is_directed;
    const bool m_is_weighted;
public:
    // Constructor
    explicit Neo_Graph_Wrapper(bool is_directed = true, bool is_weighted = true, int block_size = 1024)
            :tm(is_directed, is_weighted), m_is_weighted(is_weighted), m_is_directed(is_directed) {}

    Neo_Graph_Wrapper(const Neo_Graph_Wrapper &) = delete;

    Neo_Graph_Wrapper &operator=(const Neo_Graph_Wrapper &) = delete;

    ~Neo_Graph_Wrapper() = default;

    std::shared_ptr<Neo_Graph_Wrapper> create_update_interface(const std::string &graph_type);

    void load(const std::string &path, driver::reader::readerType type);

    // Multi-thread
    void set_max_threads(int max_threads);

    void init_thread(int thread_id);

    void end_thread(int thread_id);

    // Graph operations
    [[nodiscard]] bool is_directed() const;

    [[nodiscard]] bool is_weighted() const;

    [[nodiscard]] bool is_empty() const;

    [[nodiscard]] bool has_vertex(uint64_t vertex) const;

    [[nodiscard]] bool has_edge(driver::graph::weightedEdge edge) const;

    [[nodiscard]] bool has_edge(uint64_t source, uint64_t destination) const;

    [[nodiscard]] bool has_edge(uint64_t source, uint64_t destination, double weight) const;

    [[nodiscard]] uint64_t degree(uint64_t vertex) const;

    [[nodiscard]] double get_weight(uint64_t source, uint64_t destination) const;

#if VERTEX_PROPERTY_NUM >= 1
    [[nodiscard]] Property_t get_vertex_property(uint64_t vertex, uint8_t property_id) const;
#endif
#if VERTEX_PROPERTY_NUM > 1
    void get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
#if EDGE_PROPERTY_NUM >= 1
    [[nodiscard]] Property_t get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const;
#endif
#if EDGE_PROPERTY_NUM > 1
    void get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif

    [[nodiscard]] uint64_t logical2physical(uint64_t vertex) const;

    [[nodiscard]] uint64_t physical2logical(uint64_t physical) const;

    [[nodiscard]] uint64_t vertex_count() const;

    [[nodiscard]] uint64_t edge_count() const;

    void get_neighbors(uint64_t vertex, std::vector<uint64_t> &) const;

    void get_neighbors(uint64_t vertex, std::vector<std::pair<uint64_t, double>> &) const;

    bool insert_vertex(uint64_t vertex, Property_t* property = nullptr);

#if VERTEX_PROPERTY_NUM >= 1
    bool set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property);

    bool set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property);
#endif

    bool insert_edge(uint64_t source, uint64_t destination);

    bool insert_edge(uint64_t source, uint64_t destination, double weight);

    bool insert_edge(uint64_t source, uint64_t destination, Property_t* property);

#if EDGE_PROPERTY_NUM >= 1
    bool set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property);
#endif

    bool remove_vertex(uint64_t vertex);

    bool remove_edge(uint64_t source, uint64_t destination);

    bool run_batch_vertex_update(std::vector<uint64_t> &vertices, int start, int end);

    bool run_batch_vertex_update(std::vector<uint64_t> &vertices, std::vector<Property_t*>* properties, int start, int end);

    bool run_batch_edge_update(std::vector<std::pair<uint64_t, uint64_t>> &edges, int start, int end, operationType type);

    bool run_batch_edge_update(std::vector<operation> &edges, int start, int end, operationType type);

    void clear();

    // Snapshot Related
    class Snapshot {
    private:
        const uint64_t m_num_vertices;
        const uint64_t m_num_edges;
        NeoSnapshot snapshot;

    public:
        explicit Snapshot(const TransactionManager &tm) :
                                                                             m_num_vertices(tm.vertex_count()),
                                                                             m_num_edges(tm.edge_count()),
                                                                             snapshot{&tm} {
//                                                                             snapshot{tm.index_impl, tm.global_timestamp, true} {
        }

        Snapshot(const Snapshot &other) = default;

        Snapshot &operator=(const Snapshot &it) = delete;

        ~Snapshot() = default;

        [[nodiscard]] auto clone() const {
//            std::cout << "Snapshot cloned with timestamp" << std::endl;
            return std::make_shared<Snapshot>(*this);
        }

        [[nodiscard]] uint64_t size() const;

        [[nodiscard]] inline static uint64_t physical2logical(uint64_t physical) {
            return physical;
        }

        [[nodiscard]] inline static uint64_t logical2physical(uint64_t logical) {
            return logical;
        }

        [[nodiscard]] uint64_t degree(uint64_t, bool logical = false) const;

        [[nodiscard]] bool has_vertex(uint64_t vertex) const;

        [[nodiscard]] bool has_edge(driver::graph::weightedEdge edge) const;

        [[nodiscard]] bool has_edge(uint64_t source, uint64_t destination) const;

        [[nodiscard]] bool has_edge(uint64_t source, uint64_t destination, double weight) const;

        [[nodiscard]] double get_weight(uint64_t source, uint64_t destination) const;

#if VERTEX_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_vertex_property(uint64_t vertex, uint8_t property_id) const;
#endif
#if VERTEX_PROPERTY_NUM > 1
        void get_multi_vertex_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
#if EDGE_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const;
#endif
#if EDGE_PROPERTY_NUM > 1
        void get_multi_edge_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
        [[nodiscard]] uint64_t vertex_count() const;

        [[nodiscard]] uint64_t edge_count() const;

        [[nodiscard]] void* get_neighbor_addr(uint64_t index) const;

        uint64_t intersect(uint64_t src1, uint64_t src2) const;

        void intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) const;

        void edges(uint64_t index, std::vector<uint64_t> &neighbors) const;

        template<typename F>
        void edges(uint64_t index, F&& callback) const;

        void edges(uint64_t index, std::vector<uint64_t> &neighbors, bool logical) const;

        template<typename F>
        void edges(uint64_t index, F&& callback, bool logical) const;
    };

    [[nodiscard]] std::unique_ptr<Snapshot> get_unique_snapshot() const;

    [[nodiscard]] std::shared_ptr<Snapshot> get_shared_snapshot() const;

    // Functions for debug purpose
    static std::string repl() {
        return std::string{"Vector_Sorted_Array_Wrapper"};
    }
};