#include <thread>
#include "include/art_node_ops_copy.h"

namespace container {
    ARTNode_256 *copy_node256(ARTNode_256 *n, WriterTraceBlock* trace_block) {
        auto new_node = (ARTNode_256 *)alloc_node(NODE256, n->n.prefix, n->n.depth, trace_block);
        new_node->n.num_children = n->n.num_children;
        std::copy(n->children.begin(), n->children.end(), new_node->children.begin());
        new_node->unique_bitmap = n->unique_bitmap;
        return new_node;
    }

    ARTNode_48 *copy_node48(ARTNode_48 *n, WriterTraceBlock* trace_block) {
        auto new_node = (ARTNode_48 *)alloc_node(NODE48, n->n.prefix, n->n.depth, trace_block);
        new_node->n.num_children = n->n.num_children;
        std::copy(n->keys, n->keys + 256, new_node->keys);
        std::copy(n->children, n->children + 48, new_node->children);
        new_node->unique_bitmap = n->unique_bitmap;
        return new_node;
    }

    ARTNode_16 *copy_node16(ARTNode_16 *n) {
        auto new_node = (ARTNode_16 *)alloc_node(NODE16, n->n.prefix, n->n.depth, nullptr);
        new_node->n.num_children = n->n.num_children;
        std::copy(n->keys, n->keys + 16, new_node->keys);
        std::copy(n->children, n->children + 16, new_node->children);
        return new_node;
    }

    ARTNode_4 *copy_node4(ARTNode_4 *n) {
        auto new_node = (ARTNode_4 *)alloc_node(NODE4, n->n.prefix, n->n.depth, nullptr);
        new_node->n.num_children = n->n.num_children;
        std::copy(n->keys, n->keys + 4, new_node->keys);
        std::copy(n->children, n->children + 4, new_node->children);
        return new_node;
    }

    ARTNode *copy_node(ARTNode *n, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                return (ARTNode *) copy_node4((ARTNode_4 *) n);
            case NODE16:
                return (ARTNode *) copy_node16((ARTNode_16 *) n);
            case NODE48:
                return (ARTNode *) copy_node48((ARTNode_48 *) n, trace_block);
            case NODE256:
                return (ARTNode *) copy_node256((ARTNode_256 *) n, trace_block);
            default:
                throw std::runtime_error("copy_node(): Invalid node type");
        }
    }

    ARTLeaf *copy_leaf(ARTLeaf *l, bool is_single_byte, WriterTraceBlock* trace_block) {
        auto new_leaf = alloc_leaf(l->key, l->depth, is_single_byte, true, trace_block);

        new_leaf->size = l->size;
        l->copy_to_leaf(0, l->size, new_leaf, 0);

        return new_leaf;
    }

    std::pair<void*, void*> find_leaf_copy4(ARTNode_4* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block) {
        ARTLeaf* leaf_to_find = nullptr;
        if(*child == nullptr) {
            if(child - 1 >= node->children && IS_LEAF(*(child - 1))) {
                leaf_to_find = LEAF_RAW(*(child - 1));
            } else if (child + 1 < node->children + node->n.num_children && IS_LEAF(*(child + 1))) {
                leaf_to_find = LEAF_RAW(*(child + 1));
            } else {
                auto res = alloc_leaf(ARTKey{key, node->n.depth, true}, node->n.depth, true, true, trace_block);
                return {res, res};
            }
        } else {
            leaf_to_find = LEAF_RAW(*child);
        }

        auto begin_ptr = child;
        while(begin_ptr >= node->children) {
            if((*begin_ptr) && (!IS_LEAF(*begin_ptr) || LEAF_RAW(*begin_ptr) != leaf_to_find)) {
                break;
            }
            begin_ptr -= 1;
        }
        begin_ptr += 1;
        assert(LEAF_RAW(*begin_ptr) == leaf_to_find || *begin_ptr == nullptr);
        auto end_ptr = child + 1;
        while(end_ptr < node->children + node->n.num_children) {
            if(LEAF_RAW(*end_ptr) != leaf_to_find) {
                break;
            }
            end_ptr += 1;
        }
        return {begin_ptr, end_ptr};
    }

    std::pair<void*, void*> find_leaf_copy16(ARTNode_16* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block) {
        ARTLeaf* leaf_to_find = nullptr;
        if(*child == nullptr) {
            if(child - 1 >= node->children && IS_LEAF(*(child - 1))) {
                leaf_to_find = LEAF_RAW(*(child - 1));
            } else if (child + 1 < node->children + node->n.num_children && IS_LEAF(*(child + 1))) {
                leaf_to_find = LEAF_RAW(*(child + 1));
            } else {
                auto res = alloc_leaf(ARTKey{key, node->n.depth, true}, node->n.depth, true, true, trace_block);
                return {res, res};
            }
        } else {
            leaf_to_find = LEAF_RAW(*child);
        }

        auto begin_ptr = child;
        while(begin_ptr >= node->children) {
            if((*begin_ptr) && (!IS_LEAF(*begin_ptr) || LEAF_RAW(*begin_ptr) != leaf_to_find)) {
                break;
            }
            begin_ptr -= 1;
        }
        begin_ptr += 1;
        assert(LEAF_RAW(*begin_ptr) == leaf_to_find || *begin_ptr == nullptr);
        auto end_ptr = child + 1;
        while(end_ptr < node->children + node->n.num_children) {
            if(LEAF_RAW(*end_ptr) != leaf_to_find) {
                break;
            }
            end_ptr += 1;
        }
        return {begin_ptr, end_ptr};
    }

    std::pair<void*, void*> find_leaf_copy48(ARTNode_48* node, ARTKey key, WriterTraceBlock* trace_block) {
        ARTLeaf* leaf_to_find = nullptr;
        int prev_byte = key[node->n.depth];
        int rear_byte = key[node->n.depth] + 1;
        // <-
        while(prev_byte >= 0) {
            if(node->keys[prev_byte]) {
                auto child_ptr = node->children[node->keys[prev_byte] - 1];
                if(IS_LEAF(child_ptr)) {
                    leaf_to_find = LEAF_RAW(child_ptr);
                }
                break;
            }
            prev_byte -= 1;
        }
        // ->
        if(leaf_to_find == nullptr) {
            while(rear_byte <= 255) {
                if(node->keys[rear_byte]) {
                    auto child_ptr = node->children[node->keys[rear_byte] - 1];
                    if(IS_LEAF(child_ptr)) {
                        leaf_to_find = LEAF_RAW(child_ptr);
                    }
                    break;
                }
                rear_byte += 1;
            }
        }
        // not found
        if(leaf_to_find == nullptr) {
            auto res = alloc_leaf(ARTKey{key, node->n.depth, true}, node->n.depth, true, true, trace_block);
            return {res, res};
        }
        auto pointer_vec = new std::vector<ARTNode**>{};

        // <-
        while(prev_byte >= 0) {
            if(node->keys[prev_byte]) {
                auto child_ptr = node->children[node->keys[prev_byte] - 1];
                if(LEAF_RAW(child_ptr) == leaf_to_find) {
                    pointer_vec->push_back(&node->children[node->keys[prev_byte] - 1]);
                } else {
                    break;
                }
            }
            prev_byte -= 1;
        }
        std::reverse(pointer_vec->begin(), pointer_vec->end());
        // ->
        while(rear_byte <= 255) {
            if(node->keys[rear_byte]) {
                auto child_ptr = node->children[node->keys[rear_byte] - 1];
                if(LEAF_RAW(child_ptr) == leaf_to_find) {
                    pointer_vec->push_back(&node->children[node->keys[rear_byte] - 1]);
                } else {
                    break;
                }
            }
            rear_byte += 1;
        }
        assert(!pointer_vec->empty());
        return {pointer_vec, nullptr};
    }

    std::pair<void*, void*> find_leaf_copy256(ARTNode_256* node, ARTKey key, WriterTraceBlock* trace_block) {
        ARTLeaf* leaf_to_find = nullptr;
        ARTNode** prev_ptr = &node->children[key[node->n.depth]];
        ARTNode** rear_ptr = &node->children[key[node->n.depth] + 1];

        // <-
        while(prev_ptr >= node->children.begin()) {
            if(*prev_ptr) {
                if(IS_LEAF(*prev_ptr)) {
                    leaf_to_find = LEAF_RAW(*prev_ptr);
                }
                break;
            }
            prev_ptr -= 1;
        }
        // ->
        if(leaf_to_find == nullptr) {
            while(rear_ptr < node->children.begin() + 256) {
                if(*rear_ptr) {
                    if(IS_LEAF(*rear_ptr)) {
                        leaf_to_find = LEAF_RAW(*rear_ptr);
                    }
                    break;
                }
                rear_ptr += 1;
            }
        }
        // not found
        if(leaf_to_find == nullptr) {
            auto res = alloc_leaf(ARTKey{key, node->n.depth, true}, node->n.depth, true, true, trace_block);
            return {res, res};
        }
        auto pointer_vec = new std::vector<ARTNode**>{};

        // <-
        while(prev_ptr >= node->children.begin()) {
            if(*prev_ptr) {
                if(LEAF_RAW(*prev_ptr) == leaf_to_find) {
                    pointer_vec->push_back(prev_ptr);
                } else {
                    break;
                }
            }
            prev_ptr -= 1;
        }
        std::reverse(pointer_vec->begin(), pointer_vec->end());
        // ->
        while(rear_ptr < node->children.begin() + 256) {
            if(*rear_ptr) {
                if(LEAF_RAW(*rear_ptr) == leaf_to_find) {
                    pointer_vec->push_back(rear_ptr);
                } else {
                    break;
                }
            }
            rear_ptr += 1;
        }
        assert(!pointer_vec->empty());
        return {pointer_vec, nullptr};
    }

    std::pair<void*, void*> find_leaf_copy(ARTNode* n, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                return find_leaf_copy4((ARTNode_4 *) n, child, key, trace_block);
            case NODE16:
                return find_leaf_copy16((ARTNode_16 *) n, child, key, trace_block);
            case NODE48:
                return find_leaf_copy48((ARTNode_48 *) n, key, trace_block);
            case NODE256:
                return find_leaf_copy256((ARTNode_256 *) n, key, trace_block);
            default:
                throw std::runtime_error("find_leaf(): Invalid node type");
        }
    }

    ARTNodeSplitRes node_split_copy4(ARTNode_4 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        assert(IS_LEAF(*child) || *child == nullptr);
        auto begin_ptr = (ARTNode**) find_res.first;
        auto end_ptr = (ARTNode**) find_res.second;
        bool empty_child = (*child == nullptr);

#ifndef NDEBUG
        leaf->leaf_check();
#endif

        // single byte check
        if(empty_child) {
            if(end_ptr == begin_ptr + 1 + ((child == begin_ptr + 1) || child == begin_ptr)) {
                // cannot go deeper
                auto new_leaf = alloc_leaf(key, leaf->depth, true, true, trace_block);  // no need to set the key's depth
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            }
        } else if (end_ptr == begin_ptr + 1) {
            // go deeper
            assert(node->n.depth != 4);
            auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
            resources.push_back({ART_Leaf, old_leaf});
            return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
        }

        ARTNode** mid_ptr = find_mid_count(begin_ptr, end_ptr);
        assert(mid_ptr != begin_ptr);
        bool child_in_right_leaf = (child >= mid_ptr);

        uint16_t segment_mid_idx = GET_OFFSET(*mid_ptr);
        resources.push_back({ART_Leaf, leaf});
        // create the new leaf
        auto new_leaf_left = alloc_leaf(ARTKey{leaf->at(0)}, leaf->depth, (mid_ptr == begin_ptr + 1 && child_in_right_leaf), true, trace_block);
        new_leaf_left->size = segment_mid_idx;
        auto new_leaf_right = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, (mid_ptr == end_ptr - 1 && !child_in_right_leaf), true, trace_block);
        new_leaf_right->size = leaf->size - segment_mid_idx;

        leaf->copy_to_leaf(0, segment_mid_idx, new_leaf_left, 0);
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf_right, 0);

        // modify the node
        if(empty_child) {
            *child = (ARTNode*) LEAF_POINTER_CTOR(nullptr, segment_mid_idx);    // avoid overflow
        }
        for(auto cur_ptr = begin_ptr; cur_ptr < mid_ptr; cur_ptr++) {
            *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_left, GET_OFFSET(*cur_ptr));
        }
        for(auto cur_ptr = mid_ptr; cur_ptr < end_ptr; cur_ptr++) {
            *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_right, (GET_OFFSET(*cur_ptr) - segment_mid_idx));
        }
        if(empty_child) {
            *child = nullptr;
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
                return {ARTNodeSplitStatus::SPLIT, new_leaf_right};
            } else {
                return {ARTNodeSplitStatus::SPLIT, new_leaf_left};
            }
        } else {
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
            }
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split_copy16(ARTNode_16 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        assert(IS_LEAF(*child) || *child == nullptr);
        auto begin_ptr = (ARTNode**) find_res.first;
        auto end_ptr = (ARTNode**) find_res.second;
        bool empty_child = (*child == nullptr);

#ifndef NDEBUG
        leaf->leaf_check();
#endif

        // single byte check
        if(empty_child) {
            if(end_ptr == begin_ptr + 1 + ((child == begin_ptr + 1) || child == begin_ptr)) {   // the leaf is only bonded with one child
                auto new_leaf = alloc_leaf(key, leaf->depth, true, true, trace_block);  // no need to set the key's depth
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            }
        } else if (end_ptr == begin_ptr + 1) {
            // go deeper
            assert(node->n.depth != 4);
            auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
            resources.push_back({ART_Leaf, old_leaf});
            return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
        }

        ARTNode** mid_ptr = find_mid_count(begin_ptr, end_ptr);
        assert(mid_ptr != begin_ptr);
        bool child_in_right_leaf = (child >= mid_ptr);

        uint16_t segment_mid_idx = GET_OFFSET(*mid_ptr);
        assert(segment_mid_idx != 0);
        resources.push_back({ART_Leaf, leaf});

        // create the new leaf
        auto new_leaf_left = alloc_leaf(ARTKey{leaf->at(0)}, leaf->depth, (mid_ptr == begin_ptr + 1 && child_in_right_leaf), true, trace_block);
        new_leaf_left->size = segment_mid_idx;
        auto new_leaf_right = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, (mid_ptr == end_ptr - 1 && !child_in_right_leaf), true, trace_block);
        new_leaf_right->size = leaf->size - segment_mid_idx;

        leaf->copy_to_leaf(0, segment_mid_idx, new_leaf_left, 0);
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf_right, 0);

        // modify the node
        if(empty_child) {
            *child = (ARTNode*) LEAF_POINTER_CTOR(nullptr, segment_mid_idx);    // avoid overflow
        }
        for(auto cur_ptr = begin_ptr; cur_ptr < mid_ptr; cur_ptr++) {
            *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_left, GET_OFFSET(*cur_ptr));
        }
        for(auto cur_ptr = mid_ptr; cur_ptr < end_ptr; cur_ptr++) {
            *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_right, (GET_OFFSET(*cur_ptr) - segment_mid_idx));
        }

        if(empty_child) {
            *child = nullptr;
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
                return {ARTNodeSplitStatus::SPLIT, new_leaf_right};
            } else {
                return {ARTNodeSplitStatus::SPLIT, new_leaf_left};
            }
        } else {
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
            }
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split_copy48(ARTNode_48 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        bool empty_child = (child == nullptr);
        if(empty_child) {
            child = &node->children[key[node->n.depth] - 1];
        }
        auto& pointers_in_range = *((std::vector<ARTNode**>*)find_res.first);

#ifndef NDEBUG
        leaf->leaf_check();
#endif

        if(pointers_in_range.size() <= 1) {
            if(empty_child || (node->n.num_children == 17 && *child == nullptr)) {  // second condition is for the case that Node16 is upgraded to Node48
                auto new_leaf = alloc_leaf(key, leaf->depth, false, true, trace_block);
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            } else {
                auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
                resources.push_back({ART_Leaf, old_leaf});
                return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
            }
        }

        uint16_t mid_idx = find_mid_count(&pointers_in_range);
        assert(mid_idx != 0);
        uint16_t segment_mid_idx = GET_OFFSET(*pointers_in_range[mid_idx]);

        bool child_in_right_leaf = false;
        if(pos > segment_mid_idx || (pos == segment_mid_idx && (empty_child || GET_OFFSET(*child) >= segment_mid_idx))) {
            child_in_right_leaf = true;
        }

        resources.push_back({ART_Leaf, leaf});
        // create the new leaf
        auto new_leaf_left = alloc_leaf(ARTKey{leaf->at(0)}, leaf->depth, (mid_idx == 1 && child_in_right_leaf), true, trace_block);
        new_leaf_left->size = segment_mid_idx;
        auto new_leaf_right = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, (mid_idx == pointers_in_range.size() - 1 && !child_in_right_leaf), true, trace_block);
        new_leaf_right->size = leaf->size - segment_mid_idx;

        leaf->copy_to_leaf(0, segment_mid_idx, new_leaf_left, 0);
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf_right, 0);

        // modify the node
        for(int cur_idx = 0; cur_idx < mid_idx; cur_idx++) {
            ARTNode** ptr = pointers_in_range[cur_idx];
            *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_left, GET_OFFSET(*ptr));
        }
        for(int cur_idx = mid_idx; cur_idx < pointers_in_range.size(); cur_idx++) {
            ARTNode** ptr = pointers_in_range[cur_idx];
            *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_right, (GET_OFFSET(*ptr) - segment_mid_idx));
        }

        node->unique_bitmap.set(get_key_byte(new_leaf_right->at(0), leaf->depth));

        // correct the pos
        if(empty_child) {
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
                return {ARTNodeSplitStatus::SPLIT, new_leaf_right};
            } else {
                return {ARTNodeSplitStatus::SPLIT, new_leaf_left};
            }
        } else {
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
            }
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split_copy256(ARTNode_256 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        bool empty_child = (child == nullptr);
        if(empty_child) {
            child = &node->children[key[node->n.depth] - 1];
        }
        auto& pointers_in_range = *((std::vector<ARTNode**>*)find_res.first);

#ifndef NDEBUG
        leaf->leaf_check();
#endif

        if(pointers_in_range.size() <= 1) {
            if(empty_child) {
                auto new_leaf = alloc_leaf(key, leaf->depth, false, true, trace_block);
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            } else {
                auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
                resources.push_back({ART_Leaf, old_leaf});
                return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
            }
        }

        uint16_t mid_idx = find_mid_count(&pointers_in_range);
        assert(mid_idx != 0);
        bool child_in_right_leaf = (child >= pointers_in_range[mid_idx]);

        uint16_t segment_mid_idx = GET_OFFSET(*pointers_in_range[mid_idx]);
        resources.push_back({ART_Leaf, leaf});

        // create the new leaf
        auto new_leaf_left = alloc_leaf(ARTKey{leaf->at(0)}, leaf->depth, (mid_idx == 1 && child_in_right_leaf), true, trace_block);
        new_leaf_left->size = segment_mid_idx;
        auto new_leaf_right = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, (mid_idx == pointers_in_range.size() - 1 && !child_in_right_leaf), true, trace_block);
        new_leaf_right->size = leaf->size - segment_mid_idx;

        leaf->copy_to_leaf(0, segment_mid_idx, new_leaf_left, 0);
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf_right, 0);

        // modify the node
        for(int cur_idx = 0; cur_idx < mid_idx; cur_idx++) {
            ARTNode** ptr = pointers_in_range[cur_idx];
            *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_left, GET_OFFSET(*ptr));
        }
        for(int cur_idx = mid_idx; cur_idx < pointers_in_range.size(); cur_idx++) {
            ARTNode** ptr = pointers_in_range[cur_idx];
            *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf_right, (GET_OFFSET(*ptr) - segment_mid_idx));
        }

        node->unique_bitmap.set(get_key_byte(new_leaf_right->at(0), leaf->depth));

        if(empty_child) {
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
                return {ARTNodeSplitStatus::SPLIT, new_leaf_right};
            } else {
                return {ARTNodeSplitStatus::SPLIT, new_leaf_left};
            }
        } else {
            if(child_in_right_leaf) {
                pos = pos - segment_mid_idx;
            }
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split_copy(ARTNode *n, ARTNode** child, ARTLeaf* leaf, ARTKey key, std::pair<void*, void*> find_res, uint16_t& pos, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                return node_split_copy4((ARTNode_4*)n, child, leaf, key, find_res, pos, resources, trace_block);
            case NODE16:
                return node_split_copy16((ARTNode_16*)n,  child, leaf, key, find_res, pos, resources, trace_block);
            case NODE48:
                return node_split_copy48((ARTNode_48*)n,  child, leaf, key, find_res, pos, resources, trace_block);
            case NODE256:
                return node_split_copy256((ARTNode_256*)n,  child, leaf, key, find_res, pos, resources, trace_block);
            default:
                throw std::runtime_error("node_split(): Invalid node type");
        }
    }

    bool insert_copy(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        auto child = find_child(*n, key[(*n)->depth]);
        bool empty_child = (child == nullptr);
#ifndef NDEBUG
        if((*n)->type == NODE4) {
            auto node4 = (ARTNode_4*)*n;
            assert(GET_OFFSET(node4->children[0]) == 0); // DEBUG
        }
        if((*n)->type == NODE16) {
            auto node16 = (ARTNode_16*)*n;
            assert(GET_OFFSET(node16->children[0]) == 0);
        }
        {
            check_node(*n);
        }
//        if(value == 7479994) {
//            std::cout << "insert_copy: " << value << std::endl;
//        }
#endif

        if(!empty_child && !IS_LEAF(*child)) {
            // EXTEND - not match
            auto common_prefix_len = ARTKey::longest_common_prefix(key, (*child)->prefix);
            assert(ARTKey::check_partial_match(key, (*child)->prefix, common_prefix_len));
            auto new_node = (ARTNode_4 *) alloc_node(NODE4, key, common_prefix_len, trace_block);

            auto new_leaf = alloc_leaf(ARTKey{value}, common_prefix_len, true, true, trace_block);
            new_leaf->insert(value, property, 0);

            // remount
            assert(LEAF_RAW(*child)->key[common_prefix_len] != key[common_prefix_len]);
            add_child4(new_node, (ARTNode **) &new_node, key[common_prefix_len], LEAF_POINTER_CTOR(new_leaf, 0));
            add_child4(new_node, (ARTNode **) &new_node, (*child)->prefix[common_prefix_len], *child);
            resources.push_back({ART_Node_Mounted, *child});

            // apply
            *child = (ARTNode *) new_node;
            return true;
        }

        // find leaf
        ARTLeaf* leaf;
        uint16_t pos = 0;

        if(empty_child && (*n)->type <= NODE16) {  // add a child to enable find_leaf()
            if((*n)->type == NODE16 && (*n)->num_children == 16) {
                node16_upgrade((ARTNode_16*)*n, n, trace_block);
            } else {
                child = add_child(*n, n, key[(*n)->depth], nullptr, trace_block);
            }
        }
        auto find_res = find_leaf_copy(*n, child, key, trace_block);
        if(find_res.first == find_res.second) { // new leaf
            assert(empty_child || *child == nullptr);
            auto new_leaf = (ARTLeaf*)find_res.first;
            new_leaf->insert(value, property, 0);
            if(empty_child && (*n)->type > NODE16) {
                add_child(*n, n, key[(*n)->depth], LEAF_POINTER_CTOR(new_leaf, 0), trace_block);
            } else {
                *child = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, 0);
            }
            return true;
        } else if(find_res.second == nullptr) { // vec
            leaf = LEAF_RAW(*(((std::vector<ARTNode**>*)find_res.first)->at(0)));
        } else { // begin -> end
            if (*((ARTNode**)find_res.first) != nullptr) {
                leaf = LEAF_RAW(*((ARTNode**)find_res.first));
            } else {
                leaf = LEAF_RAW(*((ARTNode**)find_res.second - 1));
            }
        }
        pos = leaf->find(value, 0);
        assert(pos == leaf->size || leaf->at(pos) >= value);
//        if(!empty_child && pos != leaf->size && leaf->at(pos) == value) {
//            assert(false);
//        }
        assert(leaf != nullptr);

#ifndef NDEBUG
        for(int i = 0; i < leaf->size - 1; i ++) {
            assert(leaf->at(i) < leaf->at(i + 1));
        }
        if(leaf->type == LEAF8) {
            auto leaf8 = (ARTLeaf8*)leaf;
            int count = 0;
            leaf8->value.for_each([&](uint8_t idx) {
                count += 1;
            });
            assert(count == leaf8->size);
        }
#endif
        // SPLIT check
        assert(leaf->size <= ART_LEAF_SIZE);
        if(leaf->size == ART_LEAF_SIZE) {
            auto split_res = node_split_copy(*n, child, leaf, key, find_res, pos, resources, trace_block);
            if(find_res.second == nullptr) {
                delete (std::vector<ARTNode**>*)find_res.first;
            }
            switch (split_res.status) {
                case ARTNodeSplitStatus::NEW_LEAF: {
                    assert(empty_child || *child == nullptr);
                    auto new_leaf = (ARTLeaf*) split_res.leaf;
                    new_leaf->insert(value, property, 0);
                    if(empty_child && (*n)->type > NODE16) {
                        add_child(*n, n, key[(*n)->depth], LEAF_POINTER_CTOR(new_leaf, 0), trace_block);
                    } else {
                        *child = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, 0);
                    }
                    return true;
                }
                case ARTNodeSplitStatus::GO_DEEPER: {
                    // restart
                    auto next_depth_node = find_child(*n, key[(*n)->depth]);
                    return insert(next_depth_node, key, value, property, trace_block);
                }
                case ARTNodeSplitStatus::SPLIT: {
                    leaf = (ARTLeaf*) split_res.leaf;
                    break;
                }
                default: {
                    throw std::runtime_error("insert(): Invalid split status");
                }
            }
        } else {
            // copy the leaf
            resources.push_back({ART_Leaf, leaf});
            auto new_leaf = copy_leaf(leaf, (leaf->is_single_byte && !empty_child), trace_block);
#if EDGE_PROPERTY_NUM != 0
            art_property_map_copy(leaf->property_map, new_leaf->property_map);
#endif
            // update the leaf in range
            if(find_res.second == nullptr) {    // vec
                auto vec = (std::vector<ARTNode**>*)find_res.first;
                for(auto ptr : *vec) {
                    *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr)));
                }
                delete vec;
            } else {    // begin -> end
                auto begin_ptr = (ARTNode**)find_res.first;
                auto end_ptr = (ARTNode**)find_res.second;
                for(auto ptr = begin_ptr; ptr < end_ptr; ptr++) {
                    *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr)));
                }
            }
            leaf = new_leaf;
        }

        if(empty_child) {  // not found
            if(child == nullptr) {  //  NODE48 or NODE256
                if(pos == 0) {  // old byte is not the first byte now
                    ((ARTNode_256*)*n)->unique_bitmap.reset(get_key_byte(leaf->at(0), leaf->depth));
                }
                child = add_child(*n, n, key[(*n)->depth], LEAF_POINTER_CTOR(leaf, pos), trace_block);
            } else {    // NODE4 or NODE16
                *child = (ARTNode*) LEAF_POINTER_CTOR(leaf, pos);
            }
        }

        leaf->insert(value, property, pos);
        node_pointers_update(*n, child, key, 1);
#ifndef NDEBUG
        if((*n)->type == NODE4) {
            auto node4 = (ARTNode_4*)*n;
            assert(GET_OFFSET(node4->children[0]) == 0); // DEBUG
        }
        if((*n)->type == NODE16) {
            auto node16 = (ARTNode_16*)*n;
            assert(GET_OFFSET(node16->children[0]) == 0);
        }
        for(int i = 0; i < leaf->size - 1; i ++) {
            assert(leaf->at(i) < leaf->at(i + 1));
        }
        if(leaf->type == LEAF8) {
            auto leaf8 = (ARTLeaf8*)leaf;
            int count = 0;
            leaf8->value.for_each([&](uint8_t idx) {
                count += 1;
            });
            assert(count == leaf8->size);
        }
        if((*n)->type == NODE256) {
            auto node256 = (ARTNode_256*)(*n);
            ARTLeaf* leaf_ptr = nullptr;
            for(int i = 0; i < 256; i++) {
                if(node256->children[i] && leaf_ptr != LEAF_RAW(node256->children[i])) {
                    assert(GET_OFFSET(node256->children[i]) == 0);
                    leaf_ptr = LEAF_RAW(node256->children[i]);
                }
            }
        }
        {
            check_node(*n);
        }
#endif
        return true;
    }


    ARTNodeRemoveRes remove_copy(ARTNode** n, ARTKey key, uint64_t value, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        auto target_byte = key[(*n)->depth];
        auto last_byte = value & 0xFF;
        auto child = find_child(*n, target_byte);
        assert(child != nullptr && IS_LEAF(*child));
        assert((*n)->num_children > 0);
#ifndef NDEBUG
        {
//            if(value == 126598) {
//                std::cout << "debug" << std::endl;
//            }
            check_node(*n);
        }
#endif
        auto leaf = LEAF_RAW(*child);
        auto pos = leaf->find(value, GET_OFFSET(*child));
        if(pos == leaf->size || leaf->at(pos) != value) {
            return NOT_FOUND;
        }

        resources.push_back({ART_Leaf, leaf});

        if(leaf->size == 1) {   // last element
            remove_child(*n, n, target_byte, child);
            return CHILD_REMOVED;
        } else {
            auto new_leaf = copy_leaf(leaf, leaf->is_single_byte, trace_block);
            auto find_res = find_leaf_copy(*n, child, key, trace_block);

            // update the leaf in range
            if(find_res.second == nullptr) {    // vec
                auto vec = (std::vector<ARTNode**>*)find_res.first;
                for(auto ptr : *vec) {
                    *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr)));
                }
                // free the vec
                delete vec;
            } else {    // begin -> end
                auto begin_ptr = (ARTNode**)find_res.first;
                auto end_ptr = (ARTNode**)find_res.second;
                for(auto ptr = begin_ptr; ptr < end_ptr; ptr++) {
                    *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr)));
                }
            }

            // remove the element
            new_leaf->remove(pos, last_byte);
            *child = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, GET_OFFSET(*child));
            node_pointers_update(*n, child, key, -1);
            auto res = CHILD_REMOVED;
            // check if the byte segment is empty
            if(get_key_byte(new_leaf->at(GET_OFFSET(*child)), new_leaf->depth) != key[new_leaf->depth] || GET_OFFSET(*child) >= new_leaf->size) {
                if(GET_OFFSET(*child) == 0 && (*n)->type > NODE16) {
                    if((*n)->type == NODE48) {
                        ((ARTNode_48*)*n)->unique_bitmap.set(get_key_byte(new_leaf->at(0), new_leaf->depth));
                    } else {
                        ((ARTNode_256*)*n)->unique_bitmap.set(get_key_byte(new_leaf->at(0), new_leaf->depth));
                    }
                }
                remove_child(*n, n, target_byte, child);
            } else {
                res = ELEMENT_REMOVED;
            }
#ifndef NDEBUG
            if((*n)->type == NODE256) {
                auto node256 = (ARTNode_256*)(*n);
                ARTLeaf* leaf_ptr = nullptr;
                for(int i = 0; i < 256; i++) {
                    if(node256->children[i] && leaf_ptr != LEAF_RAW(node256->children[i])) {
                        assert(GET_OFFSET(node256->children[i]) == 0);
                        leaf_ptr = LEAF_RAW(node256->children[i]);
                    }
                }
            }
            {
                check_node(*n);
            }
#endif
            return res;
        }
    }

    void set_property_copy(ARTNode** n, ARTKey key, uint64_t value, uint8_t property_id, Property_t property, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        auto target_byte = key[(*n)->depth];
        auto last_byte = value & 0xFF;
        auto child = find_child(*n, target_byte);
        assert(child != nullptr && IS_LEAF(*child));
        assert((*n)->num_children > 0);

        auto leaf = LEAF_RAW(*child);
        auto pos = leaf->find(value, GET_OFFSET(*child));
        if(pos == leaf->size || leaf->at(pos) != value) {
            assert(false);
            abort();
            return;
        }

        resources.push_back({ART_Leaf, leaf});

        auto new_leaf = copy_leaf(leaf, leaf->is_single_byte, trace_block);
        auto find_res = find_leaf_copy(*n, child, key, trace_block);

        // update the leaf in range
        if(find_res.second == nullptr) {    // vec
            auto vec = (std::vector<ARTNode**>*)find_res.first;
            for(auto ptr : *vec) {
                *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr)));
            }
            // free the vec
            delete vec;
        } else {    // begin -> end
            auto begin_ptr = (ARTNode**)find_res.first;
            auto end_ptr = (ARTNode**)find_res.second;
            for(auto ptr = begin_ptr; ptr < end_ptr; ptr++) {
                *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr)));
            }
        }

        // remove the element
#if EDGE_PROPERTY_NUM > 0
        new_leaf->set_property(pos, property_id, property); // TODO bug in ARTLeaf8?
#endif
        *child = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, GET_OFFSET(*child));
    }

    uint64_t batch_extend_copy(ARTNode* n, ARTNode** target_n, uint8_t extend_depth, RangeElement *elem_list, Property_t** prop_list, uint64_t list_size, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        auto list_st_key = ARTKey{elem_list[0]};

        ARTNode* new_node = nullptr;
        new_node = alloc_node(NODE4, list_st_key, extend_depth, trace_block);

        uint64_t cur_list_st = 0;
        uint64_t cur_list_ed = 0;
        uint16_t cur_list_byte = get_key_byte(elem_list[cur_list_st], extend_depth);
        uint16_t cur_element_count = 0;
        uint64_t cur_element_st = 0;

        ARTLeaf* new_leaf = nullptr;
        uint16_t node_byte = n->prefix[extend_depth];
        auto inserted = list_size;

        while(cur_list_ed < list_size) {
            cur_list_byte = get_key_byte(elem_list[cur_list_st], extend_depth);
            if (cur_list_byte > node_byte) {
                // Add the origin node
                add_child(new_node, &new_node, node_byte, n, trace_block);
                resources.push_back({ART_Node_Mounted, n});
                node_byte = std::numeric_limits<uint16_t>::max();   // control the branch
            } else if(cur_list_byte == node_byte) {
                // Conflict!
                while (cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], extend_depth) == cur_list_byte) {
                    cur_list_ed++;
                }
                auto child = add_child(new_node, &new_node, node_byte, (void*) 0x1, trace_block);

                auto new_elements_num = batch_insert_copy(n, child, n->depth, elem_list + cur_list_st, prop_list + cur_list_st, cur_list_ed - cur_list_st, resources, trace_block);
                inserted -= (cur_list_ed - cur_list_st - new_elements_num);

                cur_list_st = cur_list_ed;
                cur_element_st = cur_list_st;
                cur_element_count = 0;
                node_byte = std::numeric_limits<uint16_t>::max();   // control the branch
            } else {
                while (cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], extend_depth) == cur_list_byte) {
                    cur_list_ed++;
                }

                if(cur_element_count + (cur_list_ed - cur_list_st) > ART_LEAF_SIZE) {
                    // insert a new leaf for the current recorded elements anyway
                    if(cur_element_count > 0) {
                        new_leaf = alloc_leaf(ARTKey{elem_list[cur_element_st]}, extend_depth, ARTKey::check_partial_match(elem_list[cur_element_st], elem_list[cur_list_ed - 1], extend_depth), true, trace_block);
                        empty_leaf_batch_insert(&new_node, &new_leaf, elem_list + cur_element_st, prop_list + cur_element_st, cur_element_count, trace_block);
                    }

                    if(cur_list_ed - cur_list_st > ART_LEAF_SIZE) { // new node
                        auto new_child = add_child(new_node, &new_node, cur_list_byte, (void*) 0x1, trace_block);
                        batch_subtree_build(new_child, extend_depth + 1, elem_list + cur_list_st, prop_list + cur_list_st, cur_list_ed - cur_list_st, trace_block);
                        cur_element_st = cur_list_ed;
                        cur_element_count = 0;
                    } else {    // new leaf
                        cur_element_st = cur_list_st;
                        cur_element_count = cur_list_ed - cur_list_st;
                    }
                } else {    // record
                    cur_element_count += cur_list_ed - cur_list_st;
                }

                cur_list_st = cur_list_ed;
            }
        };

        if(cur_element_count > 0) {
            new_leaf = alloc_leaf(ARTKey{elem_list[cur_element_st]}, extend_depth, ARTKey::check_partial_match(elem_list[cur_element_st], elem_list[list_size - 1], extend_depth), true, trace_block);
            empty_leaf_batch_insert(&new_node, &new_leaf, elem_list + cur_element_st, prop_list + cur_element_st, cur_element_count, trace_block);
        } else if(node_byte != std::numeric_limits<uint16_t>::max()) {
            add_child(new_node, &new_node, node_byte, n, trace_block);
            resources.push_back({ART_Node_Mounted, n});
        }

        *target_n = new_node;

        return inserted;
    }

    uint64_t batch_insert_copy(ARTNode* n, ARTNode** target_n, uint8_t depth, RangeElement* elem_list, Property_t** prop_list, uint64_t list_size, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        assert(list_size > 0);
        assert(n);
        assert(!IS_LEAF(n));
//        static bool after = false;
//        if(elem_list[1] == 10158304) {
//            std::cout << "cout" << std::endl;
//        }
#ifndef NDEBUG
        check_node(n);
//        if(n->prefix.key == 262144 &&depth == 2) {
//            std::cout << "debug" << std::endl;
//        }
#endif
        uint64_t inserted = 0;
        uint64_t cur_list_st = 0;
        uint64_t cur_list_ed = 0;
        uint16_t cur_list_byte;

        if(!ARTKey::check_partial_match(n->prefix, ARTKey{elem_list[list_size - 1]}, depth)) {
            auto common_prefix_len = ARTKey::longest_common_prefix(n->prefix, ARTKey{elem_list[list_size - 1]});
            return batch_extend_copy(n, target_n, common_prefix_len, elem_list, prop_list, list_size, resources, trace_block);
        }
        resources.push_back({ART_Node_Copied, n});

        auto new_node = alloc_node(NODE4, n->prefix, n->depth, trace_block);
        auto node_iter = alloc_iterator(n);

        std::pair<uint8_t, ARTNode*> cur_node_info = iter_get(node_iter);

        // TODO need a move_to_next ?
        while (cur_list_ed < list_size && iter_is_valid(node_iter)) {
            cur_list_byte = get_key_byte(elem_list[cur_list_st], depth);
#ifndef NDEBUG
//            if(elem_list[0] == 268805 && list_size == 1 && depth == 2) {
//                std::cout << "debug" << std::endl;
//            }
#endif
            if(!IS_LEAF(cur_node_info.second)) {
                if (cur_list_byte < cur_node_info.first) {
                    ARTLeaf* new_leaf = nullptr;
                    while(cur_list_ed < list_size && cur_list_byte < cur_node_info.first) {
                        while(cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], depth) == cur_list_byte) {
                            cur_list_ed++;
                        }
                        add_list_segment_to_new_leaf(&new_node, new_leaf, depth, elem_list + cur_list_st, prop_list + cur_list_st, cur_list_ed - cur_list_st, cur_list_byte, trace_block);
                        inserted += cur_list_ed - cur_list_st;
                        cur_list_st = cur_list_ed;
                        if(cur_list_ed < list_size) {
                            cur_list_byte = get_key_byte(elem_list[cur_list_st], depth);
                        }
                    }
                }
                if(cur_list_byte == cur_node_info.first) {
                    while(cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], depth) == cur_list_byte) {
                        cur_list_ed++;
                    }
                    auto new_child = add_child(new_node, &new_node, cur_list_byte, (void*) 0x1, trace_block);
                    inserted += batch_insert_copy(cur_node_info.second, new_child, cur_node_info.second->depth, elem_list + cur_list_st, prop_list + cur_list_st, cur_list_ed - cur_list_st, resources, trace_block);
                    cur_list_st = cur_list_ed;
                } else {    // cur_list_byte > cur_node_info.first
                    add_child(new_node, &new_node, cur_node_info.first, cur_node_info.second, trace_block);
                }
            }
            else {
                auto target_leaf = LEAF_RAW(cur_node_info.second);
                uint8_t last_leaf_byte = get_key_byte(LEAF_RAW(cur_node_info.second)->at(LEAF_RAW(cur_node_info.second)->size - 1), depth);

                // direct mount the old leaf
                if(cur_list_byte > last_leaf_byte) {    // TODO bug here.
                    while(iter_is_valid(node_iter) && cur_node_info.first <= last_leaf_byte && LEAF_RAW(cur_node_info.second) == target_leaf) {
                        add_child(new_node, &new_node, cur_node_info.first, cur_node_info.second, trace_block);
                        iter_next_without_skip(node_iter);
                        cur_node_info = iter_get(node_iter);
                    }
                    continue;
                }

                resources.push_back({ART_Leaf, LEAF_RAW(cur_node_info.second)});
                while (cur_list_ed < list_size && cur_list_byte <= last_leaf_byte) {
                    while(cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], depth) == cur_list_byte) {
                        cur_list_ed++;
                    }
                    if(cur_list_ed < list_size) {
                        cur_list_byte = get_key_byte(elem_list[cur_list_ed], depth);
                    }
                }

                inserted += leaf_list_merge(&new_node, LEAF_RAW(cur_node_info.second), elem_list + cur_list_st, prop_list + cur_list_st, cur_list_ed - cur_list_st, trace_block);
                cur_list_st = cur_list_ed;
            }
            iter_next(node_iter);
            cur_node_info = iter_get(node_iter);
        }

        if(cur_list_ed < list_size) {
            inserted += list_size - cur_list_st;
            ARTLeaf* new_leaf = nullptr;
            while(cur_list_ed < list_size) {
                cur_list_byte = get_key_byte(elem_list[cur_list_st], depth);
                while(cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], depth) == cur_list_byte) {
                    cur_list_ed++;
                }
                add_list_segment_to_new_leaf(&new_node, new_leaf, depth, elem_list + cur_list_st, prop_list + cur_list_st, cur_list_ed - cur_list_st, cur_list_byte, trace_block);

                inserted += cur_list_ed - cur_list_st;
                cur_list_st = cur_list_ed;
            }
        }
        else if (iter_is_valid(node_iter)) {
            while(iter_is_valid(node_iter)) {
                cur_node_info = iter_get(node_iter);
                add_child(new_node, &new_node, cur_node_info.first, cur_node_info.second, trace_block);
                iter_next_without_skip(node_iter);
            }
        }

        // remount the node
        destroy_iterator(node_iter);
        *target_n = new_node;
#ifndef NDEBUG
        check_node(new_node);
#endif
        return inserted;
    }

}
