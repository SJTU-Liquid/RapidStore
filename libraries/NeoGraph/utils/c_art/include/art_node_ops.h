#pragma once

#include "art_node.h"
#include "art_node_iter.h"
#include <map>

namespace container {
    void node_ref(ARTNode* node);

    uint16_t find_mid_count(std::vector<container::ARTNode **>* pointers_in_range);

    ARTNode** find_mid_count(ARTNode** begin_ptr, ARTNode** end_ptr);

    ARTNode **find_child(ARTNode *n, unsigned char c);

    ARTLeaf * node_search(ARTNode* u, ARTKey key);

    ///@return the index of the child, or 256 if is root
    uint16_t find_child_idx(ARTNode *n, unsigned char c);

    ARTNode** add_child256(ARTNode_256 *n, ARTNode **ref, unsigned char c, void *child);

    ARTNode** add_child48(ARTNode_48 *n, ARTNode **ref, unsigned char c, void *child, WriterTraceBlock* trace_block);

    ARTNode** add_child16(ARTNode_16 *n, ARTNode **ref, unsigned char c, void *child, WriterTraceBlock* trace_block);

    ARTNode** add_child4(ARTNode_4 *n, ARTNode **ref, unsigned char c, void *child);

    ///@return the address of the child pointer
    ARTNode** add_child(ARTNode *n, ARTNode **ref, unsigned char c, void *child, WriterTraceBlock* trace_block);

    void node16_upgrade(ARTNode_16* n, ARTNode** ref, WriterTraceBlock* trace_block);

    void remove_child256(ARTNode_256 *n, ARTNode **ref, unsigned char c, ARTNode **child);

    void remove_child48(ARTNode_48 *n, ARTNode **ref, unsigned char c, ARTNode **child);

    void remove_child16(ARTNode_16 *n, ARTNode **ref, unsigned char c, ARTNode **child);

    void remove_child4(ARTNode_4 *n, ARTNode **ref, unsigned char c, ARTNode **child);

    void remove_child(ARTNode *n, ARTNode **ref, unsigned char c, ARTNode **child);

    void node_pointers_update(ARTNode* node, ARTNode** child, ARTKey key, int offset);

    ///@brief if the `node_split` find the size of the leaf range is 1, the split will fail and the leaf pointer will be expanded - a new node will be allocated.
    ///@return old leaf pointer
    ARTLeaf* leaf_pointer_expand(ARTNode** n, uint8_t depth, WriterTraceBlock* trace_block);

    ARTLeaf* find_leaf4(ARTNode_4* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block);

    ARTLeaf* find_leaf16(ARTNode_16* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block);

    ARTLeaf* find_leaf48(ARTNode_48* node, ARTKey key, WriterTraceBlock* trace_block);

    ARTLeaf* find_leaf256(ARTNode_256* node, ARTKey key, WriterTraceBlock* trace_block);

    ///@brief find the leaf that can be inserted from the child pointer, might allocate a new leaf if no valid leaf is found.
    ///@param n the node that contains the child pointer
    ///@param child the child pointer that to be inserted
    ///@return the leaf that can be inserted.
    ARTLeaf* find_leaf(ARTNode* n, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split4(ARTNode_4 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split16(ARTNode_16 *n, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split48(ARTNode_48 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block);

    ARTNodeSplitRes node_split256(ARTNode_256 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block);

    ///@brief split the leaf range which contains the target pointer, this function will find the begin and the end of the range.
    ARTNodeSplitRes node_split(ARTNode *n, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block);

    ///@brief given a node that might bind to a leaf in which the element can be inserted, should ensure that there no deeper matched node. \n
    ///@return true if the element is inserted, false if the element is already present.
    bool insert(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block);

    ARTNodeRemoveRes remove(ARTNode** n, ARTKey key, uint64_t value);

    uint64_t empty_leaf_batch_insert8(ARTNode** n, ARTLeaf8** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block);

    uint64_t empty_leaf_batch_insert16(ARTNode** n, ARTLeaf16** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block);

    uint64_t empty_leaf_batch_insert32(ARTNode** n, ARTLeaf32** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block);

    uint64_t empty_leaf_batch_insert64(ARTNode** n, ARTLeaf64** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block);

    uint64_t empty_leaf_batch_insert(ARTNode** n, ARTLeaf** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block);

    uint64_t leaf_list_merge_batch_insert(ARTNode** n, ARTLeaf* &new_leaf, uint8_t cur_byte, ARTLeaf* leaf, uint16_t leaf_count, RangeElement* insert_list, Property_t** properties, uint64_t list_count, WriterTraceBlock* trace_block);

    uint64_t leaf_list_merge(ARTNode** n, ARTLeaf* leaf, RangeElement *elem_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block);

    void add_list_segment_to_new_leaf (ARTNode** new_node, ARTLeaf* &new_leaf, uint8_t depth, RangeElement* elem_list, Property_t** prop_list, uint64_t count, uint8_t cur_byte, WriterTraceBlock* trace_block);

    void add_leaf_segment_to_new_leaf (ARTNode** new_node, ARTLeaf* &new_leaf, uint8_t depth, ARTLeaf* cur_leaf, uint64_t count, uint8_t cur_byte, WriterTraceBlock* trace_block);

    std::pair<uint64_t , uint64_t> get_node_filling_info(ARTNode* n);

    void gc_node_ref(ARTNode *n, WriterTraceBlock* trace_block);

    template<bool IS_COPY_ON_WRITE = true>
    void batch_subtree_build(ARTNode** new_node, uint8_t depth, RangeElement* elem_list, Property_t** prop_list, uint64_t list_size, WriterTraceBlock* trace_block);

    void node_range_intersect(ARTNode* node, RangeElement* range, uint16_t range_size, std::vector<uint64_t>& result);

    void leaf_intersect(ARTLeaf* leaf1, uint8_t leaf_start1, ARTLeaf* leaf2, uint8_t leaf_start2, std::vector<uint64_t>& result);

    void node_leaf_intersect(ARTNode* node, ARTLeaf* , uint8_t leaf_start, std::vector<uint64_t>& result);

    void node_intersect(ARTNode* node1, ARTNode* node2, std::vector<uint64_t>& result);

    uint64_t node_range_intersect(ARTNode* node, RangeElement* range, uint16_t range_size);

    uint64_t leaf_intersect(ARTLeaf* leaf1, uint8_t leaf_start1, ARTLeaf* leaf2, uint8_t leaf_start2);

    uint64_t node_leaf_intersect(ARTNode* node, ARTLeaf* leaf, uint8_t leaf_start);

    uint64_t node_intersect(ARTNode* node1, ARTNode* node2);

    void check_node(ARTNode* node);

    template<typename F>
    int tree_leaf_iter(ARTNode *n, F &&callback);

    /// the function make sure that the leaves are traversed in order except for Node48 though the `in_order` flag is set to false.
    template<typename F>
    int tree_leaf_iter_unordered(ARTNode *n, F &&callback);

    template<typename F>
    void node_for_each(ARTNode *n, F &&callback);

}


//--------------------------------------IMPLEMENTATION--------------------------------------
namespace container {
    template<bool IS_COPY_ON_WRITE>
    void batch_subtree_build(ARTNode** node, uint8_t depth, RangeElement * elem_list, Property_t** prop_list, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(node != nullptr);
        if(ARTKey::check_partial_match(elem_list[0], elem_list[list_size - 1], KEY_LEN)) {
            assert(list_size <= ART_LEAF_SIZE);
            // directly add a leaf if the key is the same
            auto *new_leaf = alloc_leaf(ARTKey{elem_list[0]}, depth, IS_COPY_ON_WRITE, true, trace_block);
            new_leaf->append_from_list(elem_list, prop_list, list_size);

            if(depth != 0) {
                *node = (ARTNode *) LEAF_POINTER_CTOR(new_leaf, 0);
            } else {
                auto new_node = (ARTNode_4 *) alloc_node(NODE4, ARTKey{elem_list[0]}, 0, trace_block);
                add_child4(new_node, (ARTNode **)&new_node, get_key_byte(elem_list[0], 0), LEAF_POINTER_CTOR(new_leaf, 0));
                *node = (ARTNode *) new_node;
            }
            return;
        }

        // get necessary information
        while (depth <= 4) {
            if (get_key_byte(elem_list[0], depth) != get_key_byte(elem_list[list_size - 1], depth)) {
                break;
            }
            depth += 1;
        }

        // alloc corresponding node
        *node = alloc_node(NODE4, ARTKey{elem_list[0]}, depth, trace_block);

        // insert
        uint64_t cur_st = 0;
        uint64_t cur_ed = 0;
        ARTLeaf* cur_leaf = nullptr;
        uint8_t cur_byte;

        // TODO can be optimized
        while(cur_st < list_size) {
            cur_byte = get_key_byte(elem_list[cur_st], depth);
            while (cur_ed < list_size && get_key_byte(elem_list[cur_ed], depth) == cur_byte) {
                cur_ed++;
            }

            // new node
            if (cur_ed - cur_st > ART_LEAF_SIZE) {
                ARTNode **child = add_child(*node, node, cur_byte, (void*) 0x1, trace_block);    //
                batch_subtree_build(child, depth + 1, elem_list + cur_st, prop_list + cur_st, cur_ed - cur_st, trace_block);
                cur_st = cur_ed;
                // invalidate the leaf
                cur_leaf = nullptr;
                continue;
            }

            // new segment
            if(cur_leaf == nullptr || cur_leaf->size + (cur_ed - cur_st) > ART_LEAF_SIZE) {
                // allocate a new leaf
                cur_leaf = alloc_leaf(ARTKey{elem_list[cur_st]}, depth, false, true, trace_block);
            }

            assert(cur_leaf != nullptr);
            add_child(*node, node, cur_byte, LEAF_POINTER_CTOR(cur_leaf, cur_leaf->size), trace_block);
            cur_leaf->append_from_list(elem_list + cur_st, prop_list + cur_st, cur_ed - cur_st);

            cur_st = cur_ed;
        }
#ifndef NDEBUG
        check_node(*node);
#endif
    }


    template<typename F>
    int tree_leaf_iter(ARTNode *n, F &&callback) {
        switch(n->type) {
            case NODE4:
            case NODE16: {
                auto iter = alloc_iterator(n);
                while (iter_is_valid(iter)) {
                    auto child = iter_get_current_ro(iter);
                    if (IS_LEAF(child)) {
                        LEAF_RAW(child)->for_each(callback);
                    } else {
                        tree_leaf_iter(child, callback);
                    }
                    iter_next(iter);
                }
                destroy_iterator(iter);
                break;
            }
            case NODE48: {
                auto node = (ARTNode_48*) n;
                auto for_each = [&](uint8_t byte) {
                    auto child = node->children[node->keys[byte] - 1];
                    if (IS_LEAF(child)) {
                        LEAF_RAW(child)->for_each(callback);
                    } else {
                        tree_leaf_iter(child, callback);
                    }
                };
                node->unique_bitmap.for_each(for_each);
                break;
            }
            case NODE256: {
                auto node = (ARTNode_256*) n;
                auto for_each = [&](uint8_t byte) {
                    auto child = node->children[byte];
                    if (IS_LEAF(child)) {
                        LEAF_RAW(child)->for_each(callback);
                    } else {
                        tree_leaf_iter(child, callback);
                    }
                };
                node->unique_bitmap.for_each(for_each);
                break;
            }
        }
        return 0;
    }

    template<typename F>
    int tree_leaf_iter_unordered(ARTNode *n, F &&callback) {
        int idx = 0;
        ARTLeaf* leaf = nullptr;
        switch (n->type) {
            case NODE4: {
                for (idx = 0; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_4 *) n)->children[idx];
                    if (IS_LEAF(ptr)) {
                        leaf = LEAF_RAW(ptr);
                        break;
                    } else {
                        tree_leaf_iter(ptr, callback);
                    }
                }
                for (; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_4 *) n)->children[idx];
                    if (IS_LEAF(ptr)) {
                        if(LEAF_RAW(ptr) != leaf) {
                            callback(leaf);
                            leaf = LEAF_RAW(ptr);
                        }
                    } else {
                        tree_leaf_iter(ptr, callback);
                    }
                }
                break;
            }

            case NODE16: {
                for (idx = 0; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_16 *) n)->children[idx];
                    if (IS_LEAF(ptr)) {
                        leaf = LEAF_RAW(ptr);
                        break;
                    } else {
                        tree_leaf_iter(ptr, callback);
                    }
                }
                for (; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_16 *) n)->children[idx];
                    if (IS_LEAF(ptr)) {
                        if(LEAF_RAW(ptr) != leaf) {
                            callback(leaf);
                            leaf = LEAF_RAW(ptr);
                        }
                    } else {
                        tree_leaf_iter(ptr, callback);
                    }
                }
                break;
            }

            case NODE48:{
                for (idx = 0; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_48 *) n)->children[idx];
                    if (IS_LEAF(ptr)) {
                        leaf = LEAF_RAW(ptr);
                        break;
                    } else {
                        tree_leaf_iter(ptr, callback);
                    }
                }
                for (; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_48 *) n)->children[idx];
                    if (IS_LEAF(ptr)) {
                        if(LEAF_RAW(ptr) != leaf) {
                            callback(leaf);
                            leaf = LEAF_RAW(ptr);
                        }
                    } else {
                        tree_leaf_iter(ptr, callback);
                    }
                }
                break;
            }

            case NODE256: {
                for (idx = 0; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_256 *) n)->children[idx];
                    if(ptr) {
                        if (IS_LEAF(ptr)) {
                            leaf = LEAF_RAW(ptr);
                            break;
                        } else {
                            tree_leaf_iter(ptr, callback);
                        }
                    }
                }
                for (; idx < n->num_children; idx++) {
                    auto ptr = ((ARTNode_4 *) n)->children[idx];
                    if(ptr) {
                        if (IS_LEAF(ptr)) {
                            if (LEAF_RAW(ptr) != leaf) {
                                callback(leaf);
                                leaf = LEAF_RAW(ptr);
                            }
                        } else {
                            tree_leaf_iter(ptr, callback);
                        }
                    }
                }
                break;
            }
            default: {
                abort();
            }
        }
        if(leaf) {
            callback(leaf);
        }
        return 0;
    }

    template<typename F>
    void node_for_each(ARTNode *n, F &&callback) {
        switch (n->type) {
            case NODE4: {
                ARTNode* child = nullptr;
                for(int i = 0; i < n->num_children; i++) {
                    auto cur_child = ((ARTNode_4 *) n)->children[i];
                    if(cur_child != child) {
                        callback(cur_child);
                        child = cur_child;
                    }
                }
                break;
            }

            case NODE16: {
                ARTNode* child = nullptr;
                for(int i = 0; i < n->num_children; i++) {
                    auto cur_child = ((ARTNode_16 *) n)->children[i];
                    if(cur_child != child) {
                        callback(cur_child);
                        child = cur_child;
                    }
                }
                break;
            }

            case NODE48:{
                auto node48 = (ARTNode_48*) n;
                auto for_each = [&](uint8_t idx) {
                    callback(node48->children[node48->keys[idx] - 1]);
                };
                node48->unique_bitmap.for_each(for_each);
                break;
            }

            case NODE256: {
                auto node256 = (ARTNode_256*) n;
                auto for_each = [&](uint8_t idx) {
                    callback(node256->children[idx]);
                };
                node256->unique_bitmap.for_each(for_each);
                break;
            }
            default: {
                abort();
            }
        }
    }

}