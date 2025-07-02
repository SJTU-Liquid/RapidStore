#pragma once

#include <cstdint>
#include <memory>
#include <limits>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <immintrin.h>
#include <cassert>
#include <array>
#include <atomic>
#include "config.h"
#include "include/neo_property.h"

namespace container {
    // -----------------------------NeoStructs-----------------------------
    uint8_t get_key_byte(uint64_t key, uint8_t depth);

    struct ARTKey {
        uint32_t key;

        explicit ARTKey(uint64_t dst);

        ///@brief Construct a ARTKey object with a given key and depth. Bytes whose depth is higher than the given depth will be set to 0.
        ARTKey(uint64_t dst, uint8_t depth, bool is_single_byte);

        ARTKey(ARTKey key, uint8_t depth, bool is_single_byte);

        // access, reload [] operator
        uint8_t operator[](int idx) const;

        uint8_t &operator[](int idx);

        // reload
        bool operator==(const ARTKey &rhs) const;
        bool operator!=(const ARTKey &rhs) const;
        bool operator<(const ARTKey &rhs) const;
        void print() const;

        static bool check_partial_match(ARTKey key1, ARTKey key2, uint8_t depth) {
            assert(depth <= 3);
            for (uint8_t i = 0; i < depth; i++) {
                if (key1[i] != key2[i]) {
                    return false;
                }
            }
            return true;
        }

        static bool check_partial_match(uint64_t key1, uint64_t key2, uint8_t depth) {
            assert(depth <= 3);
            for (uint8_t i = 0; i < depth; i++) {
                if (get_key_byte(key1, i) != get_key_byte(key2, i)) {
                    return false;
                }
            }
            return true;
        }

        static uint8_t longest_common_prefix(ARTKey key1, ARTKey key2) {
            for (uint8_t i = 0; i < 3; i++) {
                if (key1[i] != key2[i]) {
                    return i;
                }
            }
            return 5;
        }
    };

    struct RangeTreeInsertElemBatchRes{
        uint64_t new_inserted;
        void * tree_ptr;
    };

    enum ARTNodeSplitStatus {
        SPLIT = 0,  // an existed leaf was split
        NEW_LEAF = 1,   // no split, in the deepest level
        GO_DEEPER = 2,  // go deeper
    };

    struct ARTNodeSplitRes {
        ARTNodeSplitStatus status;
        void* leaf;
    };

    struct ARTInsertElemBatchRes{
        uint64_t new_inserted;
        void * art_ptr;
    };

    struct ARTInsertElemCopyRes {
        uint64_t is_new: 16;
        uint64_t art_ptr: 48;
    };

    struct ARTRemoveElemCopyRes {
        uint64_t found: 16;
        uint64_t tree_ptr: 48;
    };

    enum ARTNodeRemoveRes {
        NOT_FOUND,
        ELEMENT_REMOVED,
        CHILD_REMOVED,
    };

    // Root range direction
    struct NeoRangeNode {
        uint64_t key: 16;   // key, is selected when the target >= key
        uint64_t arr_ptr: 48;   // pointer to the RangeElement array
        uint64_t size: 48;
#if EDGE_PROPERTY_NUM == 1
        RangePropertyVec_t* property;
#elif EDGE_PROPERTY_NUM > 1
        MultiRangePropertyVec_t* property;
#endif

        NeoRangeNode();

        NeoRangeNode(uint64_t key, uint64_t size, uint64_t arr_ptr, void* property_ptr);

        NeoRangeNode(const NeoRangeNode &rhs) = default;

        [[nodiscard]] bool is_empty() const;
    };

    // In leaf
//    struct RangeElement {
////        uint64_t src: 16;
//        uint64_t dst: 48;
//
//        // functions
//        explicit RangeElement();
//        RangeElement(uint64_t dst);
//        // enable copy Ctor
//        RangeElement(const RangeElement &) = default;
//        RangeElement &operator=(const RangeElement &) = default;
//        // enable move Ctor
//        RangeElement(RangeElement &&) = default;
//        RangeElement &operator=(RangeElement &&) = default;
//
//        // reload
//        bool operator<(const RangeElement &rhs) const;
//        bool operator==(const RangeElement &rhs) const;
//        bool operator!=(const RangeElement &rhs) const;
//    };
    using RangeElement = uint32_t;
//    using InRangeElement = uint32_t;

    // Independent range tree node
    struct InRangeNode {
        uint64_t size: 16;
        uint64_t arr_ptr: 48;
#if EDGE_PROPERTY_NUM == 1
        RangePropertyVec_t* property_map{};
#elif EDGE_PROPERTY_NUM > 1
        MultiRangePropertyVec_t* property_map;
#endif

        InRangeNode();

        InRangeNode(uint64_t size, uint64_t arr_ptr);

#if EDGE_PROPERTY_NUM != 0
        InRangeNode(uint64_t size, uint64_t arr_ptr, RangePropertyVec_t* prop_arr_ptr);
#endif

        InRangeNode(const InRangeNode &rhs) = default;
    };

    // In vertex_map
    struct NeoVertex {
        uint64_t is_independent: 1;
        uint64_t is_art: 1;
        uint64_t exist: 1;
        uint64_t degree: 32;
        uint64_t range_node_idx: 16; // pointer to the range node array
        uint64_t neighbor_offset: 12;
        uint64_t neighborhood_ptr: 48; // pointer to the neighbor: range segment or ART
        explicit NeoVertex(): is_art(0), exist(0), degree(0), range_node_idx(0), neighbor_offset(0) {}
    };

    enum GCResourceType {
        Outer_Segment = 1,
        Inner_Segment = 2,
        Range_Tree_Copied = 3,
        Range_Tree_Upgraded = 4,
        ART_Tree = 5,
#if VERTEX_PROPERTY_NUM != 0
        Vertex_Property_Vec = 6,
        Vertex_Property_Map_All_Modified = 7,
        Multi_Vertex_Property_Vec_Copied = 8,
        Multi_Vertex_Property_Vec_Mounted = 9,
#endif
#if EDGE_PROPERTY_NUM != 0
        Range_Property_Vec = 10,
        Range_Property_Map_All_Modified = 11,
#endif
    };

    struct GCResourceInfo {
        GCResourceType type;
        void* ptr;
    };

    enum ARTResourceType {
        ART_Leaf = 1,
        ART_Node_Copied = 2,
        ART_Node_Mounted = 3,
#if EDGE_PROPERTY_NUM != 0
        ART_Property_Vec = 4,
        ART_Property_Map_All_Modified = 5,
        Multi_ART_Property_Vec_Copied = 6,
//        Multi_ART_Property_Vec_Mounted = 7,
#endif
    };

    struct ARTResourceInfo{
        ARTResourceType type;
        void* ptr;
    };

    using RangeNodeSegment_t = std::vector<NeoRangeNode>;
    using VertexMap_t = std::array<NeoVertex, VERTEX_GROUP_SIZE>;
    struct RangeElementSegment_t {
        std::array<RangeElement, RANGE_LEAF_SIZE> value;
        std::atomic<uint32_t> ref_cnt{1};
    };
//    struct InRangeElementSegment_t {
//        std::array<RangeElement , RANGE_LEAF_SIZE> value;
//        std::atomic<uint32_t> ref_cnt{1};
//    };

    // --------------------Temporarily useless.---------------------
    struct RequiredLock {
        uint64_t idx;
        bool is_exclusive;
    };

}

