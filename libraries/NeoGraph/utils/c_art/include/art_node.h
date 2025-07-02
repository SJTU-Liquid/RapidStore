#pragma once

#include <cstdint>
#include <immintrin.h>
#include <cstring>
#include <queue>
#include <variant>
#include "art_leaf.h"
#include "helper.h"
#include "utils/bitmap/include/bitmap.h"
#include "include/neo_reader_trace.h"

namespace container {

/**
 * This struct is included as part
 * of all the various node sizes
 */
    struct ARTNode {
        ARTKey prefix{0};
        uint8_t type: 4;
        uint8_t depth: 4;   // for path compression
        uint16_t num_children = 0;
        std::atomic<uint16_t> ref_cnt = 1;
    };

/**
 * Small node with only 4 children
 */
    struct ARTNode_4 {
        ARTNode n{};
        unsigned char keys[4]{};
        ARTNode *children[4]{};
    };

/**
 * Node with 16 children
 */
    struct ARTNode_16 {
        ARTNode n{};
        unsigned char keys[16]{};
        ARTNode *children[16]{};
    };

/**
 * Node with 48 children, but
 * a full 256 byte field.
 */
    struct ARTNode_48 {
        ARTNode n{};
        Bitmap<4> unique_bitmap{};
        unsigned char keys[256]{};
        ARTNode *children[48]{};
//        uint8_t unique_ptrs[256]{};
    };

/**
 * Full node with 256 children
 */
    struct ARTNode_256 {
        ARTNode n{};
        Bitmap<4> unique_bitmap{};
        std::array<ARTNode*, ART_LEAF_SIZE> children{};
//        uint8_t unique_ptrs[256]{};
    };

    ARTNode *alloc_node(uint8_t type, ARTKey prefix, uint8_t depth, WriterTraceBlock* trace_block);

    // Recursively destroys the tree
    void recursive_destroy_node(ARTNode *n);

    // Just destroys the node
    void delete_node(ARTNode *n, WriterTraceBlock* trace_block);
}
