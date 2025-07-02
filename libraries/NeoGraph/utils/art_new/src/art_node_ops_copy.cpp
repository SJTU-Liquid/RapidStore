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

    bool insert_copy(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, std::vector<ARTResourceInfo>& resources, WriterTraceBlock* trace_block) {
        auto child = find_child(*n, key[(*n)->depth]);
        bool empty_child = (child == nullptr);

#ifndef NDEBUG
        if((*n)->type == NODE256) {
            auto node256 = (ARTNode_256*)(*n);
            for(int i = 0; i < 256; i++) {
                assert(node256->children[i] != node256->children[i + 1] || node256->children[i] == nullptr);
                if(IS_LEAF(node256->children[i])) {
                    auto check_leaf = LEAF_RAW(node256->children[i]);
                    assert(check_leaf->size < 256 && check_leaf->type < 5);
                }
            }
        }
#endif
        if(!empty_child && !IS_LEAF(*child)) {
            std::cout << "Extend" <<std::endl;
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

        if(child != nullptr) {
            leaf = LEAF_RAW(*child);
            pos = leaf->find(value, 0);
            if(pos != leaf->size && leaf->at(pos) == value) {
                return false;
            }
        } else {
            leaf = alloc_leaf(ARTKey{key, (*n)->depth, true}, (*n)->depth, true, true, trace_block);
            add_child(*n, n, key[(*n)->depth], LEAF_POINTER_CTOR(leaf, 0), trace_block);
            leaf->insert(value, property, 0);
            return true;
        }

        // SPLIT check
        assert(leaf->size <= ART_LEAF_SIZE);
        if(leaf->size == ART_LEAF_SIZE) {
            auto old_leaf = leaf_pointer_expand(child, (*n)->depth, trace_block);
            resources.push_back({ART_Leaf, old_leaf});
//            leaf_clean(old_leaf, trace_block);
//            delete old_leaf;
            auto next_depth_node = find_child(*n, key[(*n)->depth]);
            return insert(next_depth_node, key, value, property, trace_block);
        } else {
            // copy the leaf
            resources.push_back({ART_Leaf, leaf});
            auto new_leaf = copy_leaf(leaf, true, trace_block);
#if EDGE_PROPERTY_NUM != 0
            art_property_map_copy(leaf->property_map, new_leaf->property_map);
#endif
//            leaf_clean(leaf, trace_block);
//            delete leaf;
            leaf = new_leaf;
        }

        leaf->insert(value, property, pos);
        *child = (ARTNode*) LEAF_POINTER_CTOR(leaf, 0);
#ifndef NDEBUG
        if((*n)->type == NODE256) {
            auto node256 = (ARTNode_256*)(*n);
            for(int i = 0; i < 256; i++) {
                assert(node256->children[i] != node256->children[i + 1] || node256->children[i] == nullptr);
                if(IS_LEAF(node256->children[i])) {
                    auto check_leaf = LEAF_RAW(node256->children[i]);
                    assert(check_leaf->size <= 256 && check_leaf->type < 5);
                }
            }
        }
#endif
        return true;
    }

}
