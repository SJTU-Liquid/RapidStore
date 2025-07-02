#pragma once

#include <utility>
#include <vector>
#include <cstdint>
#include <algorithm>
#include <atomic>
#include <iostream>
#include <tbb/parallel_sort.h>
#include "utils/config.h"
#include "utils/types.h"
#include "utils/helper.h"
#include "neo_index.h"
#include "neo_reader_trace.h"
#include "../../../types/types.hpp"

using PUU = std::pair<uint64_t, uint64_t>;
struct WeightedEdge{
    PUU e;
    double weight;
};

namespace container {
    using PRR = std::pair<RangeElement, RangeElement>;
    struct ReadTransaction;
    struct WriteTransaction;
    struct LightWriteTransaction;

    struct TransactionManager {
        std::atomic<uint64_t> write_timestamp {0};
        std::atomic<uint64_t> read_timestamp {0};
        NeoGraphIndex* index_impl;
        uint64_t m_vertex_count{};
        uint64_t m_edge_count{};
        bool is_directed;
        bool is_weighted;

        explicit TransactionManager(bool is_directed, bool is_weighted);

        ~TransactionManager();

        [[nodiscard]] uint64_t vertex_count() const;

        [[nodiscard]] uint64_t edge_count() const;

        [[nodiscard]] uint64_t get_write_timestamp();

        [[nodiscard]] uint64_t get_read_timestamp() const;

        void finish_commit(uint64_t timestamp);

        [[nodiscard]] WriteTransaction* get_write_transaction();

        [[nodiscard]] LightWriteTransaction* get_light_write_transaction(WriterTraceBlock* tracer = nullptr);

        [[nodiscard]] ReadTransaction* get_read_transaction() const;


#if VERTEX_PROPERTY_NUM >= 1 || EDGE_PROPERTY_NUM >= 1
        [[nodiscard]] auto get_write_property_transaction();
#endif
    };

    struct ReadTransaction {
        NeoGraphIndex* index_impl;
        ReaderTraceBlock* trace_block;
        uint64_t timestamp;
        const uint64_t m_vertex_count;
        const uint64_t m_edge_count;

        // Functions
        ReadTransaction(NeoGraphIndex* index_impl, const TransactionManager* tm);

        [[nodiscard]] uint64_t vertex_count() const;

        [[nodiscard]] uint64_t edge_count() const;

        [[nodiscard]] bool has_vertex(uint64_t vertex) const;

        [[nodiscard]] bool has_edge(uint64_t source, uint64_t destination) const;

        [[nodiscard]] uint64_t get_degree(uint64_t source) const;

        void get_neighbor(uint64_t src, std::vector<RangeElement> &neighbor) const;

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
        void intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) const;

        uint64_t intersect(uint64_t src1, uint64_t src2) const;

        [[nodiscard]] void* get_neighbor_ptr(uint64_t vertex) const;

        template<typename F>
        void edges(uint64_t src, F&&callback) const;

        void get_vertices(std::vector<uint64_t> &vertices);

        bool commit();
    };

    /// Write transaction
    struct WriteTransaction {
        NeoGraphIndex* index_impl;
        TransactionManager* tm;
        WriterTraceBlock* trace_block;
        uint64_t timestamp;

        std::vector<uint64_t> *vertex_insert_vec{};
#if VERTEX_PROPERTY_NUM >= 1
        std::vector<Property_t*> *vertex_property_insert_vec{};
#endif
        std::vector<PRR> *edge_insert_vec{};
#if EDGE_PROPERTY_NUM >= 1
        std::vector<Property_t* > *edge_property_insert_vec{};
#endif
        std::vector<uint64_t> *vertex_remove_vec{};
        std::vector<PRR> *edge_remove_vec{};

        std::vector<uint64_t> *locks_to_acquire{};

        // Functions
        WriteTransaction(NeoGraphIndex* index_impl, TransactionManager* tm);
        ~WriteTransaction();

        void insert_vertex(uint64_t vertex, Property_t* property);

        void insert_vertex(Property_t* property);

        /// Note: the src. and dest. must been inserted transaction where the edge is inserted
        void insert_edge(uint64_t source, uint64_t destination, Property_t* property);

        void remove_vertex(uint64_t vertex);

        void remove_edge(uint64_t source, uint64_t destination);

        /// Hazard! This function is not thread-safe
        void clear();

        bool commit(bool vertex_batch_update = false, bool edge_batch_update = false);

        void abort();

    };

    /// Write transaction
    struct LightWriteTransaction {
        WriterTraceBlock* trace_block;
        TransactionManager* tm;
        uint32_t timestamp;
        bool external_tracer;
        std::vector<PUU>* edges{};
        std::vector<PUU>* edges_to_delete{};
        std::vector<WeightedEdge>* edges_to_update{};
//        std::tuple<std::vector<PUU>*, uint64_t, uint64_t> insert_set{};

        // Functions
        LightWriteTransaction(TransactionManager* tm, WriterTraceBlock* trace_block = nullptr);
        ~LightWriteTransaction();

        void insert_edge(uint64_t source, uint64_t destination, Property_t* property);

        void insert_edge(std::vector<PUU>* edges, uint64_t start, uint64_t end);

        static void insert_edge(uint64_t source, uint64_t destination, Property_t* property, bool is_directed, TransactionManager* tm, WriterTraceBlock* tracer) {
            if(is_directed) {
                auto tree = tm->index_impl->lock(source >> VERTEX_GROUP_BITS);
                if (!tree) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return;
                }
                tree->insert_edge(source, destination, property, tracer);
                auto timestamp = tm->get_write_timestamp();
                tree->commit_version(timestamp);
                tm->m_edge_count += 1;
                tm->finish_commit(timestamp);
                tree->gc(tracer);
                tree->writer_lock.unlock();
            } else if ((source >> VERTEX_GROUP_BITS) != (destination >> VERTEX_GROUP_BITS)) {
                if(source > destination) {
                    std::swap(source, destination);
                }
                auto tree1 = tm->index_impl->lock(source >> VERTEX_GROUP_BITS);
                auto tree2 = tm->index_impl->lock(destination >> VERTEX_GROUP_BITS);
                if (!tree1 || !tree2) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return;
                }
                tree1->insert_edge(source, destination, property, tracer);
                auto timestamp = tm->get_write_timestamp();
                tree1->commit_version(timestamp);
                tree2->insert_edge(destination, source, property, tracer);
                tree2->commit_version(timestamp);

                tm->finish_commit(timestamp);
                tree1->gc(tracer);
                tree2->gc(tracer);
                tree1->writer_lock.unlock();
                tree2->writer_lock.unlock();
                tm->m_edge_count += 2;
            } else {
                if(source > destination) {
                    std::swap(source, destination);
                }
                auto tree = tm->index_impl->lock(source >> VERTEX_GROUP_BITS);
                if (!tree) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return;
                }
                tree->insert_edge(source, destination, property, tracer);
                auto timestamp = tm->get_write_timestamp();
                tree->commit_version(timestamp);
                tree->gc(tracer);
                tree->insert_edge(destination, source, property, tracer);
                tree->commit_version(timestamp);
                tree->gc(tracer);

                tm->finish_commit(timestamp);
                tree->writer_lock.unlock();
                tm->m_edge_count += 2;
            }
        }

        static void remove_edge(uint64_t source, uint64_t destination, bool is_directed, TransactionManager* tm, WriterTraceBlock* tracer) {
            if(is_directed) {
                auto tree = tm->index_impl->lock(source >> VERTEX_GROUP_BITS);
                if (!tree) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return;
                }
                tree->insert_edge(source, destination, nullptr, tracer);
                auto timestamp = tm->get_write_timestamp();
                tree->commit_version(timestamp);
                tm->m_edge_count -= 1;
                tm->finish_commit(timestamp);
                tree->gc(tracer);
                tree->writer_lock.unlock();
            } else if ((source >> VERTEX_GROUP_BITS) != (destination >> VERTEX_GROUP_BITS)) {
                if(source > destination) {
                    std::swap(source, destination);
                }
                auto tree1 = tm->index_impl->lock(source >> VERTEX_GROUP_BITS);
                auto tree2 = tm->index_impl->lock(destination >> VERTEX_GROUP_BITS);
                if (!tree1 || !tree2) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return;
                }
                tree1->remove_edge(source, destination, tracer);
                auto timestamp = tm->get_write_timestamp();
                tree1->commit_version(timestamp);
                tree2->remove_edge(destination, source, tracer);
                tree2->commit_version(timestamp);

                tm->finish_commit(timestamp);
                tree1->gc(tracer);
                tree2->gc(tracer);
                tree1->writer_lock.unlock();
                tree2->writer_lock.unlock();
                tm->m_edge_count -= 2;
            } else {
                if(source > destination) {
                    std::swap(source, destination);
                }
                auto tree = tm->index_impl->lock(source >> VERTEX_GROUP_BITS);
                if (!tree) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return;
                }
                tree->remove_edge(source, destination, tracer);
                auto timestamp = tm->get_write_timestamp();
                tree->commit_version(timestamp);
                tree->gc(tracer);
                tree->remove_edge(destination, source, tracer);
                tree->commit_version(timestamp);
                tree->gc(tracer);

                tm->finish_commit(timestamp);
                tree->writer_lock.unlock();
                tm->m_edge_count -= 2;
            }
        }

        /// Note: the src. and dest. must been inserted transaction where the edge is inserted
        void remove_edge(uint64_t source, uint64_t destination);

        /// Note: the src. and dest. must been inserted transaction where the edge is inserted
        void update_edge(uint64_t source, uint64_t destination, double property);

        bool commit(bool vertex_batch_update = false, bool edge_batch_update = false);

        void abort();
    };

#if VERTEX_PROPERTY_NUM >= 1 || EDGE_PROPERTY_NUM >= 1
    struct WritePropertyTransaction {
        NeoGraphIndex *index_impl;
        const uint64_t timestamp;

        std::vector<std::tuple<uint64_t, uint8_t, Property_t>> *vertex_property_set_vec{};
        std::vector<std::tuple<uint64_t, uint8_t, std::string&&>> *vertex_string_property_set_vec{};
        std::vector<std::tuple<PUU, uint8_t , Property_t>> *edge_property_set_vec{};

        WritePropertyTransaction(NeoGraphIndex* index_impl, uint64_t timestamp);

        ~WritePropertyTransaction();
#if VERTEX_PROPERTY_NUM >= 1
        void vertex_property_set(uint64_t vertex, uint8_t property_id, Property_t property);

        void vertex_string_property_set(uint64_t vertex, uint8_t property_id, std::string&& property);
#endif
#if EDGE_PROPERTY_NUM >= 1
        void edge_property_set(uint64_t src, uint64_t  dest, uint8_t property_id, Property_t property);
#endif
        bool commit();

        void abort();
    };
#endif
}