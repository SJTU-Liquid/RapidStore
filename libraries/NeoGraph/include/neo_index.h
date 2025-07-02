#pragma once

#include <iostream>
#include <algorithm>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <chrono>
#include <cstring>
#include <immintrin.h>
#include <stack>
#include <tbb/concurrent_map.h>
#include <tbb/task_group.h>

#include "../utils/types.h"
#include "neo_tree.h"
//#include "../utils/art_new/include/art.h"
#include "../utils/c_art/include/art.h"

// Definition
namespace container {
    struct NeoGraphIndex {
        std::vector<std::unique_ptr<NeoTree>> *forest;

        NeoGraphIndex();

        ~NeoGraphIndex();

        static inline uint64_t gen_tree_direction(uint64_t val) {
            return val >> VERTEX_GROUP_BITS;
        }

        NeoTree* lock(uint64_t direction);

        void unlock(uint64_t direction);

        [[nodiscard]] bool has_vertex(uint64_t vertex, uint64_t timestamp) const;

        [[nodiscard]] bool has_edge(uint64_t src, uint64_t dest, uint64_t timestamp) const;

        [[nodiscard]] uint64_t get_degree(uint64_t src, uint64_t timestamp) const;

        void get_vertices(std::vector<uint64_t> &vertices, uint64_t timestamp) const;

        bool get_neighbor(uint64_t src, std::vector<RangeElement> &neighbor, uint64_t timestamp) const;

        void intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result, uint64_t timestamp) const;

        [[nodiscard]] uint64_t intersect(uint64_t src1, uint64_t src2, uint64_t timestamp) const;

        [[nodiscard]] RangeElement *get_neighbor_addr(uint64_t vertex, uint64_t timestamp) const;

        [[nodiscard]] std::pair<uint64_t, uint64_t> get_filling_info(uint64_t timestamp) const;

#if VERTEX_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_vertex_property(uint64_t vertex, uint8_t property_id, uint64_t timestamp) const;
#endif

#if VERTEX_PROPERTY_NUM > 1
        void get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res, uint64_t timestamp) const;
#endif

#if EDGE_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, uint64_t timestamp) const;
#endif

#if EDGE_PROPERTY_NUM > 1
        void get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res, uint64_t timestamp) const;
#endif
        [[nodiscard]] bool insert_vertex(uint64_t vertex, Property_t* property, WriterTraceBlock* trace_block);

        [[nodiscard]] bool insert_vertex_batch(const uint64_t* vertices, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

#if VERTEX_PROPERTY_NUM >= 1
        [[nodiscard]] bool set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property, uint64_t timestamp);

        [[nodiscard]] bool set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property, uint64_t timestamp);
#endif

#if VERTEX_PROPERTY_NUM > 1
        [[nodiscard]] bool set_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, Property_t* properties, uint64_t timestamp);
#endif

        [[nodiscard]] bool insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block);

        [[nodiscard]] bool insert_edge_batch(const std::pair<RangeElement, RangeElement> *edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

        void insert_edge_batch_single_thread(const std::pair<RangeElement, RangeElement> *edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

#if EDGE_PROPERTY_NUM >= 1
        [[nodiscard]] bool set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property, uint64_t timestamp);
#endif

#if EDGE_PROPERTY_NUM > 1
        [[nodiscard]] bool set_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, Property_t* properties, uint64_t timestamp);
#endif

        [[nodiscard]] bool remove_vertex(uint64_t vertex, bool is_directed, WriterTraceBlock* trace_block);

        [[nodiscard]] bool remove_edge(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block);

        void clear();

        NeoTree* commit(uint64_t direction, uint64_t timestamp);

        void gc(uint64_t direction, WriterTraceBlock* trace_block);
        ///@return false if vertex does not exist
        template<typename F>
        bool edges(uint64_t src, F &&callback, uint64_t timestamp) const;
    };
}

//--------------------------------------Implementation--------------------------------------
namespace container {
    ///@return false if vertex does not exist
    template<typename F>
    bool NeoGraphIndex::edges(uint64_t src, F &&callback, uint64_t timestamp) const {
        auto iter = forest->at(gen_tree_direction(src)).get();
        if(iter != nullptr) {
            iter->edges(src, std::forward<F>(callback), timestamp);
            return true;
        }
        return false;
    }
}
