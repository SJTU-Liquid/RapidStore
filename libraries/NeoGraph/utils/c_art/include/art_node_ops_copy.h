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

    std::pair<void*, void*> find_leaf_copy4(ARTNode_4* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block);

    std::pair<void*, void*> find_leaf_copy16(ARTNode_16* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block);

    std::pair<void*, void*> find_leaf_copy48(ARTNode_48* node, ARTKey key, WriterTraceBlock* trace_block);

    std::pair<void*, void*> find_leaf_copy256(ARTNode_256* node, ARTKey key, WriterTraceBlock* trace_block);
    
    ///@brief find the leaf that can be inserted from the child pointer, might allocate a new leaf if no valid leaf is found.
    ///@param n the node that contains the child pointer
    ///@param child the child pointer that to be inserted
    ///@return the leaf that can be inserted
    std::pair<void*, void*> find_leaf_copy(ARTNode* n, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split_copy4(ARTNode_4 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split_copy16(ARTNode_16 *n, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split_copy48(ARTNode_48 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split_copy256(ARTNode_256 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    ///@brief split the leaf range which contains the target pointer, this function will find the begin and the end of the range.
    ARTNodeSplitRes node_split_copy(ARTNode *n, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    ///@brief similar to `insert`, but the function will copy the leaf node if the leaf node to maintain the consistency.
    bool insert_copy(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    ARTNodeRemoveRes remove_copy(ARTNode** n, ARTKey key, uint64_t value, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    uint64_t batch_extend_copy(ARTNode* n, ARTNode** target_n, uint8_t extend_depth, RangeElement *elem_list, Property_t** prop_list, uint64_t list_size, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    uint64_t batch_insert_copy(ARTNode* n, ARTNode** target_n, uint8_t depth, RangeElement* elem_list, Property_t** prop_list, uint64_t list_size, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);

    void set_property_copy(ARTNode** n, ARTKey key, uint64_t value, uint8_t property_id, Property_t property, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block);
}