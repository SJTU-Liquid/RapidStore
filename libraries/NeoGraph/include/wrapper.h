#pragma once
#include "graph/edge.hpp"
#include "graph/edgeStream.hpp"
#include "utils/types.h"

// Functions for debug purpose
namespace wrapper {
    // Functions for debug purpose
    template<class W>
    std::string wrapper_repl(W &w) {
        return w.repl();
    }

    // Init
    template<class W>
    void set_max_threads(W &w, int max_threads){
        w.set_max_threads(max_threads);
    }
    template<class W>
    void init_thread(W &w, int thread_id) {
        w.init_thread(thread_id);
    }
    template<class W>
    void end_thread(W &w, int thread_id) {
        w.end_thread(thread_id);
    }

    // Graph Operations
    template<class W>
    bool is_directed(W &w) {
        return w.is_directed();
    }

    template<class W>
    bool is_weighted(W &w) {
        return w.is_weighted();
    }

    template<class W>
    bool is_empty(W &w) {
        return w.is_empty();
    }

    template<class W>
    bool has_vertex(W &w, uint64_t vertex) {
        return w.has_vertex(vertex);
    }

    template<class W>
    bool has_edge(W &w, driver::graph::weightedEdge edge) {
        return w.has_edge(edge);
    }

    template<class W>
    bool has_edge(W &w, uint64_t source, uint64_t destination) {
        return w.has_edge(source, destination);
    }

    template<class W>
    bool has_edge(W &w, uint64_t source, uint64_t destination, double weight) {
        return w.has_edge(source, destination);
    }

    template<class W>
    uint64_t degree(W &w, uint64_t vertex) {
        return w.degree(vertex);
    }

    template<class W>
    double get_weight(W &w, uint64_t source, uint64_t destination) {
        return w.get_weight(source, destination);
    }
#if VERTEX_PROPERTY_NUM != 0
    template<class W>
    container::Property_t get_vertex_property(W &w, uint64_t vertex, uint64_t property_id) {
        return w.get_vertex_property(vertex, property_id);
    }

    template<class W>
    void get_vertex_multi_property(W &w, uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<container::Property_t>& res) {
        return w.get_vertex_multi_property(vertex, property_ids, res);
    }
#endif

#if EDGE_PROPERTY_NUM != 0
    template<class W>
    container::Property_t get_edge_property(W &w, uint64_t src, uint64_t dest, uint8_t property_id) {
        return w.get_edge_property(src, dest, property_id);
    }

    template<class W>
    void get_edge_multi_property(W &w, uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<container::Property_t>& res) {
        w.get_edge_multi_property(src, dest, property_ids, res);
    }
#endif

    template<class W>
    uint64_t logical2physical(W &w, uint64_t logical) {
        return w.logical2physical(logical);
    }

    template<class W>
    uint64_t physical2logical(W &w, uint64_t physical) {
        return w.physical2logical(physical);
    }

    template<class W>
    uint64_t vertex_count(W &w) {
        return w.vertex_count();
    }

    template<class W>
    uint64_t edge_count(W &w) {
        return w.edge_count();
    }

    template<class W>
    void get_neighbors(W &w, uint64_t vertex, std::vector<uint64_t> &neighbors) {
        w.get_neighbors(vertex, neighbors);
    }

    template<class W>
    void get_neighbors(W &w, uint64_t vertex, std::vector<std::pair<uint64_t, double>> &neighbors) {
        w.get_neighbors(vertex, neighbors);
    }

    template<class W>
    void intersect(W &w, uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) {
        w.intersect(src1, src2, result);
    }

    template<class W>
    bool insert_vertex(W &w, uint64_t vertex) {
        return w.insert_vertex(vertex);
    }

#if VERTEX_PROPERTY_NUM != 0
    template<class W>
    bool set_vertex_property(W &w, uint64_t vertex, uint64_t property_id, container::Property_t property) {
        return w.set_vertex_property(vertex, property_id, property);
    }
#endif

    template<class W>
    bool insert_edge(W &w, uint64_t source, uint64_t destination, container::Property_t* property) {
        return w.insert_edge(source, destination, property);
    }

    template<class W>
    bool insert_edge(W &w, uint64_t source, uint64_t destination) {
        return w.insert_edge(source, destination);
    }

#if EDGE_PROPERTY_NUM != 0
    template<class W>
    bool set_edge_property(W &w, uint64_t source, uint64_t destination, uint64_t property_id, container::Property_t property) {
        return w.set_edge_property(source, destination, property_id, property);
    }
#endif

    template<class W>
    bool remove_vertex(W &w, uint64_t vertex) {
        return w.remove_vertex(vertex);
    }

    template<class W>
    bool remove_edge(W &w, uint64_t source, uint64_t destination) {
        return w.remove_edge(source, destination);
    }

    template<class W>
    bool run_batch_vertex_update(W &w, std::vector<uint64_t> & vertices, int start, int end) {
        return w.run_batch_vertex_update(vertices, nullptr, start, end);
    }

    template<class W>
    bool run_batch_edge_update(W &w, std::vector<std::pair<uint64_t, uint64_t>> &edges, int start, int end) {
        return w.run_batch_edge_update(edges, start, end);
    }

    template<class W>
    void clear(W &w) {
        w.clear();
    }

    // Snapshot Related Operations
    template<class W>
    auto get_unique_snapshot(W &w) {
        return w.get_unique_snapshot();
    }

    template<class W>
    auto get_shared_snapshot(W &w) {
        return w.get_shared_snapshot();
    }

    template<class S>
    auto snapshot_clone(S &s) {
        return s->clone();
    }

    template<class S>
    uint64_t size(S &s) {
        return s->m_size();
    }

    template<class S>
    uint64_t snapshot_physical2logical(S &s, uint64_t physical) {
        return s->physical2logical(physical);
    }

    template<class S>
    uint64_t snapshot_logical2physical(S &s, uint64_t logical) {
        return s->logical2physical(logical);
    }

    template<class S>
    uint64_t snapshot_degree(S &s, uint64_t source, bool logical = false) {
        return s->degree(source, logical);
    }

    template<class S>
    bool snapshot_has_vertex(S &s, uint64_t vertex) {
        return s->has_vertex(vertex);
    }

    template<class S>
    bool snapshot_has_edge(S &s, driver::graph::weightedEdge edge) {
        return s->has_edge(edge);
    }

    template<class S>
    bool snapshot_has_edge(S &s, uint64_t source, uint64_t destination) {
        return s->has_edge(source, destination);
    }

    template<class S>
    bool snapshot_has_edge(S &s, uint64_t source, uint64_t destination, double weight) {
        return s->has_edge(source, destination);
    }

    template<class S>
    double snapshot_get_weight(S &s, uint64_t source, uint64_t destination) {
        return s->get_weight(source, destination);
    }

    template<class S>
    container::Property_t snapshot_get_vertex_property(S &s, uint64_t vertex, uint64_t property_id) {
        return s->get_vertex_property(vertex, property_id);
    }

    template<class S>
    void snapshot_get_vertex_multi_property(S &s, uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<container::Property_t>& res) {
        return s->get_vertex_multi_property(vertex, property_ids, res);
    }

    template<class S>
    container::Property_t snapshot_get_edge_property(S &s, uint64_t src, uint64_t dest, uint8_t property_id) {
        return s->get_edge_property(src, dest, property_id);
    }

    template<class S>
    void snapshot_get_edge_multi_property(S &s, uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<container::Property_t>& res) {
        s->get_edge_multi_property(src, dest, property_ids, res);
    }

    template<class S>
    uint64_t snapshot_vertex_count(S &s) {
        return s->m_vertex_count();
    }

    template<class S>
    uint64_t snapshot_edge_count(S &s) {
        return s->m_edge_count();
    }

    template<class S>
    void* snapshot_get_neighbors_addr(S &s, uint64_t vertex) {
        return s->get_neighbor_addr(vertex);
    }

    template<class S>
    void snapshot_edges(S &s, uint64_t index, std::vector<uint64_t>&neighbors, bool logical) {
        s->edges(index, neighbors);
    }

    template<class S, typename F>
    void snapshot_edges(S &s, uint64_t index, F&& callback, bool logical) {
        s->edges(index, callback);
    }

    template<class S>
    void snapshot_intersect(S &s, uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) {
        s->intersect(src1, src2, result);
    }
}

