#include <cstdint>
#include <immintrin.h>
#include <cstring>
#include <iostream>
#include <set>
#include <map>

#include "../include/art_node.h"
#include "include/art.h"


namespace container {
    ARTNode *alloc_node(uint8_t type, ARTKey prefix, uint8_t depth, WriterTraceBlock* trace_block) {
        ARTNode *n;
        switch (type) {
            case NODE4:
                n = (ARTNode *) new ARTNode_4();
                break;
            case NODE16:
                n = (ARTNode *) new ARTNode_16();
                break;
            case NODE48:
                n = (ARTNode *) trace_block->allocate_art_node48();
                break;
            case NODE256:
                n = (ARTNode *) trace_block->allocate_art_node256();
                break;
            default:
                throw std::runtime_error("alloc_node(): Invalid node type");
        }
        n->type = type;
        n->prefix = prefix;
        n->depth = depth;

        return n;
    }

    // Recursively destroys the tree
    void recursive_destroy_node(ARTNode *n) {
        // Break if null
        if (!LEAF_RAW(n)) return;

        // Special case leafs
        if (IS_LEAF(n)) {
            leaf_destroy(LEAF_RAW(n));
            delete LEAF_RAW(n);
            return;
        }

        auto iter = alloc_iterator(n);
        while(iter_is_valid(iter)) {
            recursive_destroy_node(*iter_get_current(iter));
            iter_next(iter);
        }
        destroy_iterator(iter);
        delete n;
    }

    void delete_node(ARTNode *n, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                delete (ARTNode_4 *) n;
                break;
            case NODE16:
                delete (ARTNode_16 *) n;
                break;
            case NODE48:
                trace_block->deallocate_art_node48((ARTNode_48 *) n);
                break;
            case NODE256:
                trace_block->deallocate_art_node256((ARTNode_256 *) n);
                break;
            default:
                throw std::runtime_error("delete_node(): Invalid node type");
        }
    }
}