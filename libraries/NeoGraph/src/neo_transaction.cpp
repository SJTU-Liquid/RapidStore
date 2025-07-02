#include "include/neo_transaction.h"
#include "../../../types/types.hpp"


//--------------------------------IMPLEMENTATIONS------------------------------------------
// TransactionManager
namespace container {
    std::stack<uint64_t> reusable_vertex_pool{};
    TransactionManager::TransactionManager(bool is_directed, bool is_weighted): is_directed(is_directed), is_weighted(is_weighted) {
        index_impl = new NeoGraphIndex();
    }

    TransactionManager::~TransactionManager() {
        delete index_impl;
    }

    uint64_t TransactionManager::vertex_count() const {
        return m_vertex_count;
    }

    uint64_t TransactionManager::edge_count() const {
        return m_edge_count;
    }

    uint64_t TransactionManager::get_write_timestamp() {
        // update +1 and return the new value, return the new value
        return write_timestamp.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    void TransactionManager::finish_commit(uint64_t timestamp) {
        // read_timestamp + 1 only when read_timestamp = timestamp - 1, CAS
        auto target = timestamp - 1;
        while(!read_timestamp.compare_exchange_weak(target, timestamp, std::memory_order_relaxed)) {
        }
    }

    uint64_t TransactionManager::get_read_timestamp() const {
        return read_timestamp;
    }

    WriteTransaction* TransactionManager::get_write_transaction() {
        return new WriteTransaction(this->index_impl, this);
    }

    LightWriteTransaction* TransactionManager::get_light_write_transaction(WriterTraceBlock* tracer) {
        return new LightWriteTransaction(this, tracer);
    }

    ReadTransaction* TransactionManager::get_read_transaction() const {
        return new ReadTransaction(this->index_impl, this);
    }

#if VERTEX_PROPERTY_NUM >= 1 || EDGE_PROPERTY_NUM >= 1
    auto TransactionManager::get_write_property_transaction() {
        return WritePropertyTransaction(this->index_impl, ++write_timestamp);
    }
#endif

}

// ReadTransaction
namespace container {
    ReadTransaction::ReadTransaction(NeoGraphIndex* index_impl, const TransactionManager* tm): index_impl(index_impl), m_vertex_count(tm->vertex_count()), m_edge_count(tm->edge_count()) {
        trace_block = reader_register();
        timestamp = tm->get_read_timestamp();
        set_timestamp(trace_block, timestamp);
    }

    uint64_t ReadTransaction::vertex_count() const {
        return m_vertex_count;
    }

    uint64_t ReadTransaction::edge_count() const {
        return m_edge_count;
    }

    bool ReadTransaction::has_vertex(uint64_t vertex) const {
        return index_impl->has_vertex(vertex, timestamp);
    }

    bool ReadTransaction::has_edge(uint64_t source, uint64_t destination) const {
        return index_impl->has_edge(source, destination, timestamp);
    }

    uint64_t ReadTransaction::get_degree(uint64_t source) const {
        return index_impl->get_degree(source, timestamp);
    }

    void ReadTransaction::get_neighbor(uint64_t src, std::vector<RangeElement> &neighbor) const {
        index_impl->get_neighbor(src, neighbor, timestamp);
    }

#if VERTEX_PROPERTY_NUM >= 1
    Property_t ReadTransaction::get_vertex_property(uint64_t vertex, uint8_t property_id) const {
        return index_impl->get_vertex_property(vertex, property_id, timestamp);
    }
#endif
#if VERTEX_PROPERTY_NUM > 1
    void ReadTransaction::get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
        index_impl->get_vertex_multi_property(vertex, property_ids, res, timestamp);
    }
#endif
#if EDGE_PROPERTY_NUM >= 1
    Property_t ReadTransaction::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const {
        return index_impl->get_edge_property(src, dest, property_id, timestamp);
    }
#endif
#if EDGE_PROPERTY_NUM > 1
    void ReadTransaction::get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
        index_impl->get_edge_multi_property(src, dest, property_ids, res, timestamp);
    }
#endif

    void* ReadTransaction::get_neighbor_ptr(uint64_t vertex) const {
        return index_impl->get_neighbor_addr(vertex, timestamp);
    }

    void ReadTransaction::intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) const {
        index_impl->intersect(src1, src2, result, timestamp);
    }

    uint64_t ReadTransaction::intersect(uint64_t src1, uint64_t src2) const {
        return index_impl->intersect(src1, src2, timestamp);
    }

    template<typename F>
    void ReadTransaction::edges(uint64_t src, F&&callback) const {
        index_impl->edges(src, callback, timestamp);
    }

    void ReadTransaction::get_vertices(std::vector<uint64_t> &vertices) {
        index_impl->get_vertices(vertices, timestamp);
    }

    bool ReadTransaction::commit() {
        reader_unregister(trace_block);
        return true;
    }
}

// WriteTransaction
namespace container {
    WriteTransaction::WriteTransaction(NeoGraphIndex* index_impl, TransactionManager* tm): index_impl(index_impl), tm(tm) {
        trace_block = writer_register();
        locks_to_acquire = new std::vector<uint64_t>();
        edge_insert_vec = new std::vector<PRR>();
#if EDGE_PROPERTY_NUM >= 1
        edge_property_insert_vec = new std::vector<Property_t*>();
#endif
        vertex_insert_vec = new std::vector<uint64_t>();
#if VERTEX_PROPERTY_NUM >= 1
        vertex_property_insert_vec = new std::vector<Property_t*>();
#endif
        // not usual to remove vertex and edge so we don't reserve for them
        edge_remove_vec = nullptr;
        vertex_remove_vec = nullptr;
    }

    WriteTransaction::~WriteTransaction() {
        delete locks_to_acquire;
        delete vertex_insert_vec;
#if VERTEX_PROPERTY_NUM >= 1
        delete vertex_property_insert_vec;
#endif
        delete edge_insert_vec;
#if EDGE_PROPERTY_NUM >= 1
        delete edge_property_insert_vec;
#endif
        delete vertex_remove_vec;
        delete edge_remove_vec;
        writer_unregister(trace_block);
    }

    void WriteTransaction::insert_vertex(uint64_t vertex, Property_t* property) {
        vertex_insert_vec->push_back(vertex);
        locks_to_acquire->push_back(vertex >> VERTEX_GROUP_BITS);
        tm->m_vertex_count += 1;    // TODO: not thread-safe, debug only
#if VERTEX_PROPERTY_NUM >= 1
        vertex_property_insert_vec->push_back(property);
#endif
    }

    void WriteTransaction::insert_vertex(Property_t* property) {
        uint64_t vertex = 0;
        if(!reusable_vertex_pool.empty()) {
            vertex = reusable_vertex_pool.top();
            reusable_vertex_pool.pop();
        } else  {
            vertex = tm->m_vertex_count;
        }

        vertex_insert_vec->push_back(vertex);
        locks_to_acquire->push_back(vertex >> VERTEX_GROUP_BITS);
        tm->m_vertex_count += 1;    // TODO: not thread-safe, debug only
#if VERTEX_PROPERTY_NUM >= 1
        vertex_property_insert_vec->push_back(property);
#endif
    }

    /// Note: the src. and dest. must been inserted transaction where the edge is inserted
    void WriteTransaction::insert_edge(uint64_t source, uint64_t destination, Property_t* property) {
        edge_insert_vec->emplace_back(source, destination);
        locks_to_acquire->push_back(source >> VERTEX_GROUP_BITS);
        tm->m_edge_count += 2;    // TODO: not thread-safe, debug only
#if EDGE_PROPERTY_NUM >= 1
        edge_property_insert_vec->push_back(property);
#endif
    }

    void WriteTransaction::remove_vertex(uint64_t vertex) {
        // Simple Implementation
        if(vertex_remove_vec == nullptr) {
            vertex_remove_vec = new std::vector<uint64_t>();
        }
        for(auto i = 0; i < tm->m_vertex_count; i++) {
            locks_to_acquire->push_back(i >> VERTEX_GROUP_BITS);
        }
        vertex_remove_vec->emplace_back(vertex);
    }

    void WriteTransaction::remove_edge(uint64_t source, uint64_t destination) {
        if(edge_remove_vec == nullptr) {
            edge_remove_vec = new std::vector<PRR>();
        }
        locks_to_acquire->push_back(source >> VERTEX_GROUP_BITS);
        edge_remove_vec->emplace_back(source, destination);
    }

    /// Hazard! This function is not thread-safe
    void WriteTransaction::clear() {
        index_impl->clear();
    }

    bool WriteTransaction::commit(bool vertex_batch_update, bool edge_batch_update) {
        // acquire locks
        if(vertex_remove_vec != nullptr && (vertex_remove_vec->size() > 1 || vertex_insert_vec != nullptr || !edge_insert_vec->empty() || edge_remove_vec != nullptr)) {
            // vertex remove must be executed by a pure txn.
            return false;
        }
        std::sort(locks_to_acquire->begin(), locks_to_acquire->end());
        locks_to_acquire->erase(std::unique(locks_to_acquire->begin(), locks_to_acquire->end()), locks_to_acquire->end());
        for(auto &lock: *locks_to_acquire) {
            index_impl->lock(lock);
        }

        // delete vertex
        if(vertex_remove_vec != nullptr) {
            for (auto &vertex: *vertex_remove_vec) {
                if (!index_impl->remove_vertex(vertex, tm->is_directed, trace_block)) {
                    return false;   // degree not match, restart
                }
            }
            for (auto &vertex: *vertex_remove_vec) {
                reusable_vertex_pool.push(vertex);
            }
        }

        if(vertex_batch_update) {
#if VERTEX_PROPERTY_NUM >= 1
            vec_sort<uint64_t, Property_t *>(*vertex_insert_vec, *vertex_property_insert_vec);
            vertex_insert_vec->erase(std::unique(vertex_insert_vec->begin(), vertex_insert_vec->end()),
                                     vertex_insert_vec->end());
            vertex_property_insert_vec->erase(
                    std::unique(vertex_property_insert_vec->begin(), vertex_property_insert_vec->end()),
                    vertex_property_insert_vec->end());
#else
            std::sort(vertex_insert_vec->begin(), vertex_insert_vec->end());
            vertex_insert_vec->erase(std::unique(vertex_insert_vec->begin(), vertex_insert_vec->end()),
                                     vertex_insert_vec->end());
#endif
        }

        if(edge_batch_update) {
#if EDGE_PROPERTY_NUM >= 1
            vec_sort<PRR, Property_t *>(*edge_insert_vec, *edge_property_insert_vec);
//            auto unique_ptr = std::unique(edge_insert_vec->begin(), edge_insert_vec->end());
//            uint64_t unique_distance = std::distance(edge_insert_vec->begin(), unique_ptr);
//            edge_insert_vec->erase(unique_ptr, edge_insert_vec->end());
//            edge_property_insert_vec->erase(edge_property_insert_vec->begin() + unique_distance, edge_property_insert_vec->end());
#else
            std::sort(edge_insert_vec->begin(), edge_insert_vec->end());
            edge_insert_vec->erase(std::unique(edge_insert_vec->begin(), edge_insert_vec->end()),
                                   edge_insert_vec->end());
#endif
        }

        // insert vertex
#if VERTEX_PROPERTY_NUM >= 1
        if(vertex_batch_update) {
            if (!index_impl->insert_vertex_batch(vertex_insert_vec->data(), vertex_property_insert_vec->data(), vertex_insert_vec->size(), timestamp)) {
                throw std::runtime_error("insert vertex failed");
            }
        } else {
            for (size_t i = 0; i < vertex_insert_vec->size(); i++) {
                if (!index_impl->insert_vertex(vertex_insert_vec->at(i), vertex_property_insert_vec->at(i), timestamp)) {
                    throw std::runtime_error("insert vertex failed");
                }
            }
        }
#else
        if(vertex_batch_update) {
            if (!index_impl->insert_vertex_batch(vertex_insert_vec->data(), nullptr, vertex_insert_vec->size(), trace_block)) {
                return false;
            }
        } else {
            for (size_t i = 0; i < vertex_insert_vec->size(); i++) {
                if (!index_impl->insert_vertex(vertex_insert_vec->at(i), nullptr, trace_block)) {
                    return false;
                }
            }
        }
#endif

        timestamp = tm->get_write_timestamp();
        // insert edge
#if EDGE_PROPERTY_NUM != 0
        if(!edge_batch_update || edge_insert_vec->size() <= BATCH_UPDATE_ENABLE_THRESHOLD) {
            int64_t last_direction = -1;
            for (auto i = 0; i < edge_insert_vec->size(); i++) {
//                std::cout << edge_insert_vec->at(i).first << " " << edge_insert_vec->at(i).second << std::endl;
                if(last_direction == edge_insert_vec->at(i).first >> VERTEX_GROUP_BITS) {
                    index_impl->commit(edge_insert_vec->at(i).first >> VERTEX_GROUP_BITS, timestamp);
                    index_impl->gc(edge_insert_vec->at(i).first >> VERTEX_GROUP_BITS, trace_block);
                } else {
                    last_direction = edge_insert_vec->at(i).first >> VERTEX_GROUP_BITS;
                }
                if(!index_impl->insert_edge(edge_insert_vec->at(i).first, edge_insert_vec->at(i).second, edge_property_insert_vec->at(i), trace_block)) {
                    return false;
                }
            }
        } else {
            index_impl->insert_edge_batch(edge_insert_vec->data(), edge_property_insert_vec->data(), edge_insert_vec->size(), trace_block);
        }
#else
        if(!edge_batch_update || edge_insert_vec->size() <= BATCH_UPDATE_ENABLE_THRESHOLD) {
            for (auto i = 0; i < edge_insert_vec->size(); i++) {
                if(!index_impl->insert_edge(edge_insert_vec->at(i).first, edge_insert_vec->at(i).second, nullptr, trace_block)) {
                    return false;
                }
            }
        } else {
            index_impl->insert_edge_batch(edge_insert_vec->data(), nullptr, edge_insert_vec->size(), trace_block);
        }
#endif
        // delete edge
        if(edge_remove_vec != nullptr) {
            for (auto &edge: *edge_remove_vec) {
                if (!index_impl->remove_edge(edge.first, edge.second, trace_block)) {
                    return false;
                }
            }
        }

        auto trees = new std::vector<NeoTree*>(locks_to_acquire->size());
        for(uint64_t i = 0; i < locks_to_acquire->size(); i++) {
            trees->at(i) = index_impl->commit(locks_to_acquire->at(i), timestamp);
        }
        tm->finish_commit(timestamp);
        for(uint64_t i = 0; i < locks_to_acquire->size(); i++) {
            trees->at(i)->gc(trace_block);
            trees->at(i)->writer_lock.unlock();
        }
        delete trees;
        return true;
    }

    void WriteTransaction::abort() {
        // TODO rollback is not implemented
        std::cout << "write txn aborted" << std::endl;
    }

#if VERTEX_PROPERTY_NUM >= 1 || EDGE_PROPERTY_NUM >= 1
    WritePropertyTransaction::WritePropertyTransaction(NeoGraphIndex* index_impl, uint64_t timestamp): index_impl(index_impl), timestamp(timestamp) {
        vertex_property_set_vec = new std::vector<std::tuple<uint64_t, uint8_t, Property_t>>();
        vertex_string_property_set_vec = new std::vector<std::tuple<uint64_t, uint8_t, std::string&&>>{};
        edge_property_set_vec = new std::vector<std::tuple<PUU, uint8_t, Property_t>>();
    }

    WritePropertyTransaction::~WritePropertyTransaction() {
        delete vertex_property_set_vec;
        delete vertex_string_property_set_vec;
        delete edge_property_set_vec;
    }
#if VERTEX_PROPERTY_NUM >= 1
    void WritePropertyTransaction::vertex_property_set(uint64_t vertex, uint8_t property_id, Property_t property) {
        vertex_property_set_vec->emplace_back(vertex, property_id, property);
    }

    void WritePropertyTransaction::vertex_string_property_set(uint64_t vertex, uint8_t property_id, std::string&& property) {
        vertex_string_property_set_vec->emplace_back(vertex, property_id, std::move(property));
    }

#endif

#if EDGE_PROPERTY_NUM >= 1
    void WritePropertyTransaction::edge_property_set(uint64_t src, uint64_t  dest, uint8_t property_id, Property_t property) {
        edge_property_set_vec->emplace_back(std::make_pair(src, dest), property_id, property);
    }
#endif

    bool WritePropertyTransaction::commit() {
#if VERTEX_PROPERTY_NUM >= 1
        for(auto &vertex: *vertex_property_set_vec) {
            if (!index_impl->set_vertex_property(std::get<0>(vertex), std::get<1>(vertex), std::get<2>(vertex), timestamp)) {
                throw std::runtime_error("set vertex property failed");
            }
        }

        for(auto &vertex: *vertex_string_property_set_vec) {
            if (!index_impl->set_vertex_string_property(std::get<0>(vertex), std::get<1>(vertex), std::move(std::get<2>(vertex)), timestamp)) {
                throw std::runtime_error("set vertex string property failed");
            }
        }
#endif
#if EDGE_PROPERTY_NUM >= 1
        for(auto &edge: *edge_property_set_vec) {
            if (!index_impl->set_edge_property(std::get<0>(edge).first, std::get<0>(edge).second, std::get<1>(edge), std::get<2>(edge), timestamp)) {
                throw std::runtime_error("set edge property failed");
            }
        }
#endif
        return true;
    }

    void WritePropertyTransaction::abort() {
        std::cout << "write property txn aborted" << std::endl;
    }
#endif
}

namespace container {
    // Functions
    LightWriteTransaction::LightWriteTransaction(TransactionManager* tm, WriterTraceBlock* trace_block): tm(tm), timestamp(0), trace_block(trace_block), edges(new std::vector<PUU>()) {
        if(!trace_block) {
            this->trace_block = writer_register();
            external_tracer = false;
        } else {
            external_tracer = true;
        }
    }

    LightWriteTransaction::~LightWriteTransaction() {
        if(!external_tracer) {
            writer_unregister(trace_block);
        }
        delete edges;
        delete edges_to_delete;
        delete edges_to_update;
    }

    /// Note: the src. and dest. must been inserted transaction where the edge is inserted
    void LightWriteTransaction::insert_edge(uint64_t source, uint64_t destination, Property_t* property) {
        edges->emplace_back(source, destination);
    }

    void LightWriteTransaction::insert_edge(std::vector<PUU>* edges, uint64_t start, uint64_t end) {
//        insert_set = std::make_tuple(edges, start, end);
    }

    void LightWriteTransaction::remove_edge(uint64_t source, uint64_t destination) {
        if(edges_to_delete == nullptr) {
            edges_to_delete = new std::vector<PUU>();
        }
        edges_to_delete->emplace_back(source, destination);
    }

    void LightWriteTransaction::update_edge(uint64_t source, uint64_t destination, double property) {
        if(edges_to_update == nullptr) {
            edges_to_update = new std::vector<WeightedEdge>();
        }
        edges_to_update->emplace_back(WeightedEdge{PUU{source, destination}, property});
    }


    bool LightWriteTransaction::commit(bool vertex_batch_update, bool edge_batch_update) {
        for(uint64_t idx = 0; idx < edges->size(); idx++) {
            auto edge = edges->at(idx);
            auto tree = tm->index_impl->lock(edge.first >> VERTEX_GROUP_BITS);
            if (!tree) {
                std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                return false;
            }
            tree->insert_edge(edge.first, edge.second, nullptr, trace_block);
            timestamp = tm->get_write_timestamp();
            tree->commit_version(timestamp);
            tm->m_edge_count += 1;
            tm->finish_commit(timestamp);
            tree->gc(trace_block);
            tree->writer_lock.unlock();
        }
        if(edges_to_delete) {
            for(uint64_t idx = 0; idx < edges_to_delete->size(); idx++) {
//                if (idx % 1000000 == 0) {
//                    std::cout << "Deleting edge " << idx << "/ " << edges_to_delete->size() << std::endl;
//                }
                auto edge = edges_to_delete->at(idx);
                auto tree = tm->index_impl->lock(edge.first >> VERTEX_GROUP_BITS);
                if (!tree) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return false;
                }
                tree->remove_edge(edge.first, edge.second, trace_block);
                timestamp = tm->get_write_timestamp();
                tree->commit_version(timestamp);
                tm->m_edge_count -= 1;
                tm->finish_commit(timestamp);
                tree->gc(trace_block);
                tree->writer_lock.unlock();
            }
        }
        if(edges_to_update) {
            for(uint64_t idx = 0; idx < edges_to_update->size(); idx++) {
//                if (idx % 100000 == 0) {
//                    std::cout << "Update edge " << idx << "/ " << edges_to_update->size() << std::endl;
//                }
                auto edge = edges_to_update->at(idx);
                auto tree = tm->index_impl->lock(edge.e.first >> VERTEX_GROUP_BITS);
                if (!tree) {
                    std::cout << "Warning: LightWriteTransaction::commit(): No tree" << std::endl;
                    return false;
                }
#if EDGE_PROPERTY_NUM > 0
                tree->set_edge_property(edge.e.first, edge.e.second, 0, edge.weight, trace_block);
#endif
                timestamp = tm->get_write_timestamp();
                tree->commit_version(timestamp);
                tm->finish_commit(timestamp);
                tree->gc(trace_block);
                tree->writer_lock.unlock();
            }
        }
        return true;
    }

    void LightWriteTransaction::abort() {
        std::cout << "light write txn aborted" << std::endl;
    }
}