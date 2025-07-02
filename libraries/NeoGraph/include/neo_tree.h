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

#include "../utils/types.h"
#include "../utils/config.h"
#include "neo_tree_version.h"
#include "../utils/c_art/include/art.h"
//#include "../utils/art_new/include/art.h"
#include "utils/thread_pool.h"

namespace container {
    class NeoTree {
    public:
        NeoTreeVersion* version_head{};
        NeoTreeVersion* uncommited_version{};
        uint16_t version_num: 15;
        uint16_t direct_gc_flag: 1;
        SpinLock writer_lock{};

        explicit NeoTree(uint64_t prefix);
        ~NeoTree();

        [[nodiscard]] bool has_vertex(uint64_t vertex, uint64_t timestamp) const;

        [[nodiscard]] bool has_edge(uint64_t src, uint64_t dest, uint64_t timestamp) const;

        [[nodiscard]] uint64_t get_degree(uint64_t vertex, uint64_t timestamp) const;

        [[nodiscard]] RangeElement* get_neighbor_addr(uint64_t vertex, uint64_t timestamp) const;

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

        ///@return false if vertex does not exist
        bool get_neighbor(uint64_t src, std::vector<RangeElement>& neighbor, uint64_t timestamp) const;

        ///@return false if vertex does not exist
        template<typename F>
        void edges(uint64_t src, F&& callback, uint64_t timestamp) const;

        static void intersect(NeoTree* tree1, uint64_t src1, NeoTree* tree2, uint64_t src2, std::vector<uint64_t> &result, uint64_t timestamp);

        static uint64_t intersect(NeoTree* tree1, uint64_t src1, NeoTree* tree2, uint64_t src2, uint64_t timestamp);

        void insert_vertex(uint64_t vertex, Property_t* property, WriterTraceBlock* trace_block);

        void insert_vertex_batch(const uint64_t* vertices, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

#if VERTEX_PROPERTY_NUM >= 1
        void set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property);

        void set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property);
#endif

#if VERTEX_PROPERTY_NUM > 1
        void set_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, Property_t* properties);
#endif

        void insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block);

        void insert_edge_batch(const std::pair<RangeElement, RangeElement> *edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

#if EDGE_PROPERTY_NUM >= 1
        void set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property, WriterTraceBlock* trace_block);
#endif

#if EDGE_PROPERTY_NUM > 1
        void set_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, Property_t* properties);
#endif

        bool remove_vertex(uint64_t vertex, bool is_directed, WriterTraceBlock* trace_block);

        void remove_edge(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block);

        void clean(WriterTraceBlock* trace_block);

        [[nodiscard]] NeoTreeVersion* find_version(uint64_t timestamp) const;

        static void release_version(NeoTreeVersion* version) {
            if(version == nullptr) {
                return;
            }
            version->ref_cnt.fetch_sub(1, std::memory_order_release) == 1;
        }

        bool finish_version(NeoTreeVersion* version);
        bool commit_version(uint64_t timestamp);
        void version_gc(NeoTreeVersion*& version, std::vector<uint64_t>& readers, WriterTraceBlock* trace_block);
        void gc(WriterTraceBlock* trace_block);
    };
}

// ---------------------------------------Implementation---------------------------------------
namespace container {

    template<typename F>
    void NeoTree::edges(uint64_t src, F&& callback, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return;
        }
        version->edges(src, std::forward<F>(callback));
        release_version(version);
    }
}

