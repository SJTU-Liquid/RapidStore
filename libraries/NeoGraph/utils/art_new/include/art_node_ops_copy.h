#pragma once

#include "art_node.h"
#include "art_node_ops.h"

namespace container {
    ARTNode** add_child_copy(ARTNode *n, uint8_t child_idx, ARTNode *child);

    ARTNode_256* copy_node256(ARTNode_256 *n, WriterTraceBlock* trace_block);

    ARTNode_48* copy_node48(ARTNode_48 *n, WriterTraceBlock* trace_block);

    ARTNode_16* copy_node16(ARTNode_16 *n);

    ARTNode_4* copy_node4(ARTNode_4 *n);

    ARTNode* copy_node(ARTNode *n, WriterTraceBlock* trace_block);

    ARTLeaf* copy_leaf(ARTLeaf *l, bool is_single_byte, WriterTraceBlock* trace_block);

    ///@brief similar to `insert`, but the function will copy the leaf node if the leaf node to maintain the consistency.
    bool insert_copy(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);
}