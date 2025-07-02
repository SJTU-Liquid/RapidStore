#include <set>
#include "include/art_node_ops.h"
#include "include/art_iter.h"

namespace container {
    void node_ref(ARTNode* node) {
        auto cb = [](ARTNode* child) {
            if(IS_LEAF(child)) {
                LEAF_RAW(child)->ref_cnt += 1;
            } else {
                child->ref_cnt += 1;
            }
        };
        node_for_each(node, cb);
    }

    uint16_t find_mid_count(std::vector<container::ARTNode **>* pointers_in_range) {
        uint16_t cur_idx = 0;
        while(cur_idx < pointers_in_range->size() - 1) {
            cur_idx += 1;
            if(GET_OFFSET(*(pointers_in_range->at(cur_idx))) >= ART_LEAF_SIZE / 2) {
                break;
            }
        }
        return cur_idx;
    }

    ARTNode** find_mid_count(ARTNode** begin_ptr, ARTNode** end_ptr) {
        auto cur_ptr = begin_ptr;
        if(*(end_ptr - 1) == nullptr) {
            end_ptr -= 1;
        }
        while(cur_ptr < end_ptr - 1) {
            cur_ptr += 1;
            if(GET_OFFSET(*cur_ptr) >= ART_LEAF_SIZE / 2) { // GET_OFFSET(nullptr) == 0
                break;
            }
        }
        return cur_ptr;
    }

    ARTNode **find_child(ARTNode *n, unsigned char c) {
        int i, mask, bitfield;
        union {
            ARTNode_4 *p1;
            ARTNode_16 *p2;
            ARTNode_48 *p3;
            ARTNode_256 *p4;
        } p{};

        switch (n->type) {
            case NODE4:
                p.p1 = (ARTNode_4 *) n;
                for (i = 0; i < n->num_children; i++) {
                    if (((unsigned char *) p.p1->keys)[i] == c) {
                        return &p.p1->children[i];
                    }
                }
                break;

            case NODE16:
                p.p2 = (ARTNode_16 *) n;

                __m128i cmp;
                cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *) p.p2->keys));

                mask = (1 << n->num_children) - 1;
                bitfield = _mm_movemask_epi8(cmp) & mask;

                if (bitfield) {
                    return &p.p2->children[__builtin_ctz(bitfield)];
                }
                break;

            case NODE48:
                p.p3 = (ARTNode_48 *) n;
                i = p.p3->keys[c];
                if (i) {
                    return &p.p3->children[i - 1];
                }
                break;

            case NODE256:
                p.p4 = (ARTNode_256 *) n;
                if (p.p4->children[c]) {
                    return &p.p4->children[c];
                }
                break;

            default:
                throw std::runtime_error("find_child(): Invalid node type");
        }
        return nullptr;
    }

    uint16_t find_child_idx(ARTNode *n, unsigned char c) {
        int i;
        union {
            ARTNode_4 *p1;
            ARTNode_16 *p2;
            ARTNode_48 *p3;
            ARTNode_256 *p4;
        } p{};

        switch (n->type) {
            case NODE4: {
                p.p1 = (ARTNode_4 *) n;
                for (i = 0; i < n->num_children; i++) {
                    if (((unsigned char *) p.p1->keys)[i] == c) {
                        return i;
                    }
                }
                break;
            }

            case NODE16: {
                int mask, bitfield;
                p.p2 = (ARTNode_16 *) n;

                __m128i cmp;
                cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i *) p.p2->keys));

                mask = (1 << n->num_children) - 1;
                bitfield = _mm_movemask_epi8(cmp) & mask;

                if (bitfield) {
                    return __builtin_ctz(bitfield);
                }
                break;
            }

            case NODE48: {
                p.p3 = (ARTNode_48 *) n;
                i = p.p3->keys[c];
                if (i) {
                    return i - 1;
                }
                break;
            }

            case NODE256: {
                p.p4 = (ARTNode_256 *) n;
                if (p.p4->children[c]) {
                    return c;
                }
                break;
            }

            default:
                throw std::runtime_error("find_child_idx(): Invalid node type");
        }
        return 256; // not found
    }

    ARTLeaf * node_search(ARTNode* u, ARTKey key) {
        ARTNode **child;
        ARTNode *n = u;
        int depth = 0;

        union {
            ARTNode_4 *p1;
            ARTNode_16 *p2;
            ARTNode_48 *p3;
            ARTNode_256 *p4;
        } p{};
        while (n) {
            // Might be a leaf
            if (IS_LEAF(n)) {
                auto l = LEAF_RAW(n);
                auto offset = GET_OFFSET(n);
                if(l->depth == 4) {
                    assert(ARTKey::check_partial_match(ARTKey{l->at(offset)}, key, l->depth));
                    return (ARTLeaf*)n;
                } else {
                    // path compression
                    if(ARTKey::check_partial_match(ARTKey{l->at(offset)}, key, l->depth)) {
                        return (ARTLeaf*)n;
                    }
                }
                return nullptr;
            }
            p.p1 = (ARTNode_4*)n;
            // Recursively search
            if(n->depth == depth) {
                child = find_child(n, key[depth]);
                n = (child) ? *child : nullptr;
                depth++;
            } else {
                // path compression
                assert(n->depth > depth);
                if(ARTKey::check_partial_match(n->prefix, key, n->depth - 1)) {
                    depth = n->depth;
                    child = find_child(n, key[depth]);
                    n = (child) ? *child : nullptr;
                } else {
                    return nullptr;
                }
            }
        }
        return nullptr;
    }

    ARTNode** add_child256(ARTNode_256 *n, ARTNode **ref, unsigned char c, void *child) {
        assert(child);
        if(GET_OFFSET(child) == 0) {
            n->unique_bitmap.set(c);
        }
        n->children[c] = (ARTNode *) child;
        n->n.num_children++;

#ifndef NDEBUG
        uint64_t unique_child_num = 0;
        ARTLeaf* cur_child = nullptr;

        auto iter = alloc_iterator((ARTNode*)n);
        for (int i = 0; i < 256; i++) {
            if (LEAF_RAW(((ARTNode_256 *) n)->children[i]) &&
                LEAF_RAW(((ARTNode_256 *) n)->children[i]) != cur_child) {
                unique_child_num++;
                cur_child = LEAF_RAW(((ARTNode_256 *) n)->children[i]);
                assert(LEAF_RAW(*iter_get_current(iter)) == cur_child);
                iter_next(iter);
            }
        }
        destroy_iterator(iter);

        iter = alloc_iterator((ARTNode*)n);
        cur_child = nullptr;
        while(iter_is_valid(iter)) {
            assert(LEAF_RAW(*iter_get_current(iter)) != cur_child);
            cur_child = LEAF_RAW(*iter_get_current(iter));
            iter_next(iter);
        }
        destroy_iterator(iter);
#endif

        return &n->children[c];
    }

    ARTNode** add_child48(ARTNode_48 *n, ARTNode **ref, unsigned char c, void *child, WriterTraceBlock* trace_block) {
        assert(child);
        if (n->n.num_children < 48) {
            if(GET_OFFSET(child) == 0) {
                n->unique_bitmap.set(c);
            }
            int pos = 0;
            while (n->children[pos]) pos++;     // fill the first empty slot
            n->children[pos] = (ARTNode *) child;
            assert(n->keys[c] == 0);
            n->keys[c] = pos + 1;
            n->n.num_children++;

#ifndef NDEBUG
            uint64_t unique_child_num = 0;
            ARTLeaf* cur_child = nullptr;

            auto iter = alloc_iterator((ARTNode*)n);
            auto node48 = (ARTNode_48*)n;
            for (int i = 0; i < 256; i++) {
                if (node48->keys[i] && LEAF_RAW(node48->children[node48->keys[i] - 1]) != cur_child) {
                    unique_child_num++;
                    cur_child = LEAF_RAW(node48->children[node48->keys[i] - 1]);
                    assert(LEAF_RAW(*iter_get_current(iter)) == cur_child);
                    iter_next(iter);
                }
            }
            destroy_iterator(iter);
            uint64_t count = 0;
            iter = alloc_iterator((ARTNode*)node48);
            cur_child = nullptr;
            while(iter_is_valid(iter)) {
                assert(LEAF_RAW(*iter_get_current(iter)) != cur_child);
                cur_child = LEAF_RAW(*iter_get_current(iter));
                if(IS_LEAF(*iter_get_current(iter))) {
                    count += cur_child->size;
                }
                iter_next(iter);
            }
            destroy_iterator(iter);
#endif

            return &n->children[pos];
        } else {
            auto *new_node = (ARTNode_256 *) alloc_node(NODE256, n->n.prefix, n->n.depth, trace_block);
            new_node->n.num_children = n->n.num_children;
            for(int i = 0; i < 256; i++) {
                if(n->keys[i]) {
                    new_node->children[i] = n->children[n->keys[i] - 1];
                }
            }
            new_node->unique_bitmap = n->unique_bitmap;
            *ref = (ARTNode *) new_node;
            auto res = add_child256((ARTNode_256*) *ref, ref, c, child);

            trace_block->deallocate_art_node48(n);

#ifndef NDEBUG
            uint64_t unique_child_num = 0;
            ARTLeaf* cur_child = nullptr;

            auto iter = alloc_iterator((ARTNode*)*ref);
            for (int i = 0; i < 256; i++) {
                if (LEAF_RAW(((ARTNode_256 *) *ref)->children[i]) &&
                    LEAF_RAW(((ARTNode_256 *) *ref)->children[i]) != cur_child) {
                    unique_child_num++;
                    cur_child = LEAF_RAW(((ARTNode_256 *) *ref)->children[i]);
                    assert(LEAF_RAW(*iter_get_current(iter)) == cur_child);
                    iter_next(iter);
                }
            }
            destroy_iterator(iter);

            iter = alloc_iterator((ARTNode*)*ref);
            cur_child = nullptr;
            while(iter_is_valid(iter)) {
                assert(LEAF_RAW(*iter_get_current(iter)) != cur_child);
                cur_child = LEAF_RAW(*iter_get_current(iter));
                iter_next(iter);
            }
            destroy_iterator(iter);
#endif
            return res;
        }
    }

    ARTNode** add_child16(ARTNode_16 *n, ARTNode **ref, unsigned char c, void *child, WriterTraceBlock* trace_block) {
        if (n->n.num_children < 16) {
            int idx;
            for (idx = 0; idx < n->n.num_children; idx++) {
                if (c < n->keys[idx]) break;
            }

            // Shift to make room
            memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
            memmove(n->children + idx + 1, n->children + idx,
                    (n->n.num_children - idx) * sizeof(void *));

            // Set the child
            n->keys[idx] = c;
            n->children[idx] = (ARTNode *) child;
            n->n.num_children++;

            return &n->children[idx];
        } else {
            auto *new_node = (ARTNode_48 *) alloc_node(NODE48, n->n.prefix, n->n.depth, trace_block);
            new_node->n.num_children = n->n.num_children;

            // Copy the child pointers and populate the key map
            memcpy(new_node->children, n->children,
                   sizeof(void *) * n->n.num_children);
            for (int i = 0; i < n->n.num_children; i++) {
                new_node->keys[n->keys[i]] = i + 1;
                if(GET_OFFSET(n->children[i]) == 0) {
                    new_node->unique_bitmap.set(n->keys[i]);
                }
            }
            *ref = (ARTNode *) new_node;
            auto res = add_child48(new_node, ref, c, child, trace_block);
            delete n;

#ifndef NDEBUG
            uint64_t unique_child_num = 0;
            ARTLeaf* cur_child = nullptr;

            auto iter = alloc_iterator((ARTNode*)*ref);
            auto node48 = (ARTNode_48*)*ref;
            for (int i = 0; i < 256; i++) {
                if (node48->keys[i] && LEAF_RAW(node48->children[node48->keys[i] - 1]) != cur_child) {
                    unique_child_num++;
                    cur_child = LEAF_RAW(node48->children[node48->keys[i] - 1]);
                    assert(LEAF_RAW(*iter_get_current(iter)) == cur_child);
                    iter_next(iter);
                }
            }
            destroy_iterator(iter);

            iter = alloc_iterator((ARTNode*)*ref);
            cur_child = nullptr;
            while(iter_is_valid(iter)) {
                assert(LEAF_RAW(*iter_get_current(iter)) != cur_child);
                cur_child = LEAF_RAW(*iter_get_current(iter));
                iter_next(iter);
            }
            destroy_iterator(iter);
#endif

            return res;
        }
    }

    ARTNode** add_child4(ARTNode_4 *n, ARTNode **ref, unsigned char c, void *child) {
        if (n->n.num_children < 4) {
            int idx;
            for (idx = 0; idx < n->n.num_children; idx++) {
                if (c < n->keys[idx]) break;
            }

            // Shift to make room
            memmove(n->keys + idx + 1, n->keys + idx, n->n.num_children - idx);
            memmove(n->children + idx + 1, n->children + idx,
                    (n->n.num_children - idx) * sizeof(void *));

            // Insert element
            n->keys[idx] = c;
            n->children[idx] = (ARTNode *) child;
            n->n.num_children++;

            return &n->children[idx];
        } else {
            auto *new_node = (ARTNode_16 *) alloc_node(NODE16, n->n.prefix, n->n.depth, nullptr);
            new_node->n.num_children = n->n.num_children;

            // Copy the child pointers and the key map
            memcpy(new_node->children, n->children,
                   sizeof(void *) * n->n.num_children);
            memcpy(new_node->keys, n->keys,
                   sizeof(unsigned char) * n->n.num_children);
            *ref = (ARTNode *) new_node;
            auto res = add_child16(new_node, ref, c, child, nullptr);

            delete n;
            return res;
        }
    }

    ARTNode** add_child(ARTNode *n, ARTNode **ref, unsigned char c, void *child, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                return add_child4((ARTNode_4 *) n, ref, c, child);
            case NODE16:
                return add_child16((ARTNode_16 *) n, ref, c, child, trace_block);
            case NODE48:
                return add_child48((ARTNode_48 *) n, ref, c, child, trace_block);
            case NODE256:
                return add_child256((ARTNode_256 *) n, ref, c, child);
            default:
                throw std::runtime_error("add_child(): Invalid node type");
        }
    }

    void node16_upgrade(ARTNode_16* n, ARTNode** ref, WriterTraceBlock* trace_block) {
        auto *new_node = (ARTNode_48 *) alloc_node(NODE48, n->n.prefix, n->n.depth, trace_block);
        new_node->n.num_children = n->n.num_children;

        // Copy the child pointers and populate the key map
        memcpy(new_node->children, n->children,
               sizeof(void *) * n->n.num_children);
        for (int i = 0; i < n->n.num_children; i++) {
            new_node->keys[n->keys[i]] = i + 1;
            if(GET_OFFSET(n->children[i]) == 0) {
                new_node->unique_bitmap.set(n->keys[i]);
            }
        }
        delete n;

        *ref = (ARTNode *) new_node;

#ifndef NDEBUG
        uint64_t unique_child_num = 0;
        ARTLeaf* cur_child = nullptr;

        auto iter = alloc_iterator((ARTNode*)*ref);
        auto node48 = (ARTNode_48*)*ref;
        for (int i = 0; i < 256; i++) {
            if (node48->keys[i] && LEAF_RAW(node48->children[node48->keys[i] - 1]) != cur_child) {
                unique_child_num++;
                cur_child = LEAF_RAW(node48->children[node48->keys[i] - 1]);
                assert(LEAF_RAW(*iter_get_current(iter)) == cur_child);
                iter_next(iter);
            }
        }
        destroy_iterator(iter);

        iter = alloc_iterator((ARTNode*)*ref);
        cur_child = nullptr;
        while(iter_is_valid(iter)) {
            assert(LEAF_RAW(*iter_get_current(iter)) != cur_child);
            cur_child = LEAF_RAW(*iter_get_current(iter));
            iter_next(iter);
        }
        destroy_iterator(iter);
#endif
    }

    ARTNode** add_child_copy(ARTNode *n, uint8_t child_idx, ARTNode *child) {
        switch (n->type) {
            case NODE4:
                ((ARTNode_4 *) n)->children[child_idx] = child;
                return &((ARTNode_4 *) n)->children[child_idx];
            case NODE16:
                ((ARTNode_16 *) n)->children[child_idx] = child;
                return &((ARTNode_16 *) n)->children[child_idx];
            case NODE48:
                ((ARTNode_48 *) n)->children[child_idx] = child;
                return &((ARTNode_48 *) n)->children[child_idx];
            case NODE256:
                ((ARTNode_256 *) n)->children[child_idx] = child;
                return &((ARTNode_256 *) n)->children[child_idx];
            default:
                throw std::runtime_error("add_child_copy(): Invalid node type");
        }
    }

    ARTLeaf* leaf_pointer_expand(ARTNode** n, uint8_t depth, WriterTraceBlock* trace_block) {
        depth = depth + 1;
        assert(GET_OFFSET(*n) == 0);
        auto leaf = LEAF_RAW(*n);

        if(ARTKey::check_partial_match(leaf->at(0), leaf->at(leaf->size - 1), KEY_LEN)) {
            depth = KEY_LEN - 1;
            auto cur_byte = get_key_byte(leaf->at(0), depth);
            auto new_leaf = alloc_leaf(ARTKey{leaf->at(0)}, depth, true, true, trace_block);
            auto new_node = (ARTNode_4*) alloc_node(NODE4, new_leaf->key, depth, trace_block);

            // copy the leaf
            new_leaf->size = leaf->size;
            leaf->copy_to_leaf(0, leaf->size, new_leaf, 0);
            assert(ARTKey(leaf->at(0), depth, new_leaf->is_single_byte) == new_leaf->key);
            add_child4(new_node, (ARTNode**)&new_node, cur_byte, LEAF_POINTER_CTOR(new_leaf, 0));

            // apply the new node
            *n = (ARTNode*) new_node;
            return leaf;
        }

        // find the minimum depth to differentiate the keys
        while(depth < KEY_LEN) {
            if(get_key_byte(leaf->at(0), depth) != get_key_byte(leaf->at(leaf->size - 1), depth)) {
                break;
            }
            depth++;
        }

        assert(depth != KEY_LEN);

        // expand
        uint16_t st = 0;
        uint16_t ed = 0;
        uint8_t cur_byte = get_key_byte(leaf->at(0), depth);

        ARTNode* new_node = nullptr;
        new_node = (ARTNode*) alloc_node(NODE4, ARTKey{leaf->at(0)}, depth, trace_block);

        while (ed < leaf->size) {
            assert(cur_byte <= get_key_byte(leaf->at(ed), depth));   // the array should be sorted
            cur_byte = get_key_byte(leaf->at(ed), depth);
            while(ed < leaf->size && get_key_byte(leaf->at(ed), depth) == cur_byte) {
                ed++;
            }

            // create leaf and copy
            auto new_leaf = alloc_leaf(ARTKey{leaf->at(st)}, depth, true, true, trace_block);
            new_leaf->size = ed - st;
            leaf->copy_to_leaf(st, ed, new_leaf, 0);
            add_child(new_node, (ARTNode **) &new_node, cur_byte,  LEAF_POINTER_CTOR(new_leaf, 0), trace_block);

            st = ed;
        }

        *n = (ARTNode*) new_node;

#ifndef NDEBUG
        if(new_node->type == NODE48) {
            auto node48 = (ARTNode_48*)new_node;
            for(int i = 0; i < 48; i++) {
                assert(node48->children[i] != node48->children[i + 1] || node48->children[i] == nullptr);
                if(IS_LEAF(node48->children[i])) {
                    auto check_leaf = LEAF_RAW(node48->children[i]);
                    assert(check_leaf->size <= 256 && check_leaf->type < 5);
                }
            }
        }
        if(new_node->type == NODE256) {
            auto node256 = (ARTNode_256*)new_node;
            for(int i = 0; i < 256; i++) {
                assert(node256->children[i] != node256->children[i + 1] || node256->children[i] == nullptr);
                if(IS_LEAF(node256->children[i])) {
                    auto check_leaf = LEAF_RAW(node256->children[i]);
                    assert(check_leaf->size <= 256 && check_leaf->type < 5);
                }
            }
        }
#endif
        return leaf;
    }


    bool insert(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block) {
        auto child = find_child(*n, key[(*n)->depth]);
//        if(value == 167781) {
//            std::cout << "debug" << std::endl;
//        }

#ifndef NDEBUG
        if((*n)->type == NODE48) {
            auto node48 = (ARTNode_48*)(*n);
            for(int i = 0; i < 48; i++) {
                assert(node48->children[i] != node48->children[i + 1] || node48->children[i] == nullptr);
                if(IS_LEAF(node48->children[i])) {
                    auto check_leaf = LEAF_RAW(node48->children[i]);
                    assert(check_leaf->size <= 256 && check_leaf->type < 5);
                }
            }
        }
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
        if(child && !IS_LEAF(*child)) {
            // EXTEND - not match
            auto common_prefix_len = ARTKey::longest_common_prefix(key, (*child)->prefix);
            assert(ARTKey::check_partial_match(key, (*child)->prefix, common_prefix_len));
            auto new_node = (ARTNode_4 *) alloc_node(NODE4, key, common_prefix_len, trace_block);

            auto new_leaf = alloc_leaf(ARTKey{key}, common_prefix_len, true, true, trace_block);
            new_leaf->insert(value, property, 0);

            // remount
            assert((*child)->prefix[common_prefix_len] != key[common_prefix_len]);
            add_child4(new_node, (ARTNode **) &new_node, key[common_prefix_len], LEAF_POINTER_CTOR(new_leaf, 0));
            add_child4(new_node, (ARTNode **) &new_node, (*child)->prefix[common_prefix_len], *child);

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
//            std::cout << "splited" << std::endl;
            auto old_leaf = leaf_pointer_expand(child, (*n)->depth, trace_block);
//            resources.push_back({ART_Leaf, old_leaf});
            leaf_clean(old_leaf, trace_block);
            delete old_leaf;
            auto next_depth_node = find_child(*n, key[(*n)->depth]);
            return insert(next_depth_node, key, value, property, trace_block);
        } else {
            auto new_leaf = alloc_leaf(leaf->key, leaf->depth, true, true, trace_block);
            new_leaf->size = leaf->size;
            leaf->copy_to_leaf(0, leaf->size, new_leaf, 0);
#if EDGE_PROPERTY_NUM != 0
            art_property_map_copy(leaf->property_map, new_leaf->property_map);
#endif
            leaf_clean(leaf, trace_block);
            delete leaf;
            leaf = new_leaf;
            *child = (ARTNode*) LEAF_POINTER_CTOR(leaf, 0);
        }

        leaf->insert(value, property, pos);
#ifndef NDEBUG
        if((*n)->type == NODE48) {
            auto node48 = (ARTNode_48*)(*n);
            for(int i = 0; i < 48; i++) {
                assert(node48->children[i] != node48->children[i + 1] || node48->children[i] == nullptr);
                if(IS_LEAF(node48->children[i])) {
                    auto check_leaf = LEAF_RAW(node48->children[i]);
                    assert(check_leaf->size <= 256 && check_leaf->type < 5);
                }
            }
        }
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

    std::pair<uint64_t, uint64_t> get_node_filling_info(ARTNode* n) {
        std::pair<uint64_t, uint64_t> res = std::make_pair(0, 0);
        auto iter = alloc_iterator(n);
        while(iter_is_valid(iter)) {
            auto child = *iter_get_current(iter);
            if(!IS_LEAF(child)) {
                auto info = get_node_filling_info(child);
                res.first += info.first;
                res.second += info.second;
            }
            iter_next(iter);
        }
        destroy_iterator(iter);

        res.second += n->num_children;
        switch (n->type) {
            case NODE4: {
                auto node4 = (ARTNode_4*) n;
                for(int i = 0; i < n->num_children; i++) {
                    auto child = node4->children[i];
                    if(IS_LEAF(child) && GET_OFFSET(child) == 0) {
                        res.first += 1;
                    }
                }
                break;
            }
            case NODE16: {
                auto node16 = (ARTNode_16*) n;
                for(int i = 0; i < n->num_children; i++) {
                    auto child = node16->children[i];
                    if(IS_LEAF(child) && GET_OFFSET(child) == 0) {
                        res.first += 1;
                    }
                }
                break;
            }
            case NODE48:
            case NODE256: {
                throw std::runtime_error("get_node_filling_info(): NODE48 and NODE256 are not supported");
                break;
            }
        }

        return res;
    }


    void gc_node_ref(ARTNode *n, WriterTraceBlock* trace_block) {
        if(LEAF_RAW(n) == nullptr) {
            return;
        }

        if(IS_LEAF(n)) {
//            auto l = LEAF_RAW(n);
//            if(l->ref_cnt.fetch_sub(1) == 1) {
//                leaf_clean(l, trace_block);
//                delete l;
//            }
            return;
        }

        if(n->ref_cnt.fetch_sub(1) != 1) {
            return;
        }

        assert(n->ref_cnt == 0);

        auto for_each = [&](ARTNode* child) {
            gc_node_ref(child, trace_block);
        };
        node_for_each(n, for_each);
        delete_node(n, trace_block);
    }

    void node_range_intersect(ARTNode* node, RangeElement* range, uint16_t range_size, std::vector<uint64_t>& result) {
        uint16_t cur_idx1 = 0;
        uint64_t range_last_element = range[range_size - 1];
//        ARTIterator iter{node};
        ARTLeaf* node_leaf = nullptr;

        while(cur_idx1 < range_size) {
            node_leaf = node_search(node, ARTKey(range[cur_idx1]));
            if(!node_leaf) {
                cur_idx1 += 1;
                continue;
            }
            uint16_t size2 = node_leaf->size;
            if(node_leaf->at(size2 - 1) >= range[cur_idx1]) {
                uint16_t cur_idx2 = 0;
                while(cur_idx1 < range_size && cur_idx2 < size2) {
                    if(range[cur_idx1] == node_leaf->at(cur_idx2)) {
                        result.push_back(range[cur_idx1]);
                        cur_idx1 += 1;
                        cur_idx2 += 1;
                    } else if(range[cur_idx1] < node_leaf->at(cur_idx2)) {
                        cur_idx1 += 1;
                    } else {
                        cur_idx2 += 1;
                    }
                }
            }
        }
    }

    void leaf_intersect(ARTLeaf* leaf1, uint8_t leaf_start1, ARTLeaf* leaf2, uint8_t leaf_start2, std::vector<uint64_t>& result) {
        auto size1 = leaf1->size;
        auto size2 = leaf2->size;
        uint16_t cur_idx1 = leaf_start1;
        uint16_t cur_idx2 = leaf_start2;
        uint8_t byte1 = get_key_byte(leaf1->at(cur_idx1), leaf1->depth);
        uint8_t byte2 = get_key_byte(leaf2->at(cur_idx2), leaf2->depth);

        while(cur_idx1 < size1 && cur_idx2 < size2) {
            if(byte1 != get_key_byte(leaf1->at(cur_idx1), leaf1->depth)) {
                break;
            }
            if(byte2 != get_key_byte(leaf2->at(cur_idx2), leaf2->depth)) {
                break;
            }
            if(leaf1->at(cur_idx1) == leaf2->at(cur_idx2)) {
                result.push_back(leaf1->at(cur_idx1));
                cur_idx1 += 1;
                cur_idx2 += 1;
            } else if(leaf1->at(cur_idx1) < leaf2->at(cur_idx2)) {
                cur_idx1 += 1;
            } else {
                cur_idx2 += 1;
            }
        }
    }

    void node_leaf_intersect(ARTNode* node, ARTLeaf* leaf, uint8_t leaf_start, std::vector<uint64_t>& result) {
        uint16_t size1 = leaf->size;
        uint8_t byte1 = get_key_byte(leaf->at(leaf_start), leaf->depth);
        uint64_t leaf_last_element = leaf->at(size1 - 1);
        uint16_t cur_idx1 = leaf_start;
        ARTLeaf* node_leaf = nullptr;

//        ARTIterator iter{node};
        while(cur_idx1 < size1) {
            if(byte1 != get_key_byte(leaf->at(cur_idx1), leaf->depth)) {
                break;
            }
            auto raw_leaf = node_search(node, ARTKey(leaf->at(cur_idx1)));
            if(!raw_leaf) {
                cur_idx1 += 1;
                continue;
            }
            node_leaf = LEAF_RAW(raw_leaf);
            uint16_t size2 = node_leaf->size;
            if(node_leaf->at(size2 - 1) >= leaf->at(cur_idx1)) {
                uint16_t cur_idx2 = GET_OFFSET(raw_leaf);
                while(cur_idx1 < size1 && cur_idx2 < size2) {
                    if(leaf->at(cur_idx1) == node_leaf->at(cur_idx2)) {
                        result.push_back(leaf->at(cur_idx1));
                        cur_idx1 += 1;
                        cur_idx2 += 1;
                    } else if(leaf->at(cur_idx1) < node_leaf->at(cur_idx2)) {
                        cur_idx1 += 1;
                    } else {
                        cur_idx2 += 1;
                    }
                }
            } else {
                cur_idx1 += 1;
            }
        }
    }

    void node_intersect(ARTNode* node1, ARTNode* node2, std::vector<uint64_t>& result) {
        assert(node1 && node2);
        assert(!IS_LEAF(node1) && !IS_LEAF(node2));
        if(!ARTKey::check_partial_match(node1->prefix, node2->prefix, std::min(node1->depth, node2->depth))) {
            return;
        }
        if (node1->depth != node2->depth) {
            // swap to make node1's depth <= node2's depth
            if (node1->depth > node2->depth) {
                std::swap(node1, node2);
            }

            auto child = find_child(node1, node2->prefix[node1->depth]);
            if(child) { // must be valid
                assert(*child);
                if(IS_LEAF(*child)) {
                    node_leaf_intersect(node2, LEAF_RAW(*child), GET_OFFSET(*child), result);
                } else {
                    node_intersect(*child, node2, result);
                }
            }
        } else {
            auto iter1 = alloc_iterator(node1);
            auto iter2 = alloc_iterator(node2);

            while (iter1->is_valid() && iter2->is_valid()) {
                std::pair<uint8_t, ARTNode *> byte_pair1 = iter_get(iter1);
                std::pair<uint8_t, ARTNode *> byte_pair2 = iter_get(iter2);
                if (byte_pair1.first < byte_pair2.first) {
                    iter_next_without_skip(iter1);
                    continue;
                } else if (byte_pair1.first > byte_pair2.first) {
                    iter_next_without_skip(iter2);
                    continue;
                }
                auto child1 = byte_pair1.second;
                auto child2 = byte_pair2.second;
                if(IS_LEAF(child1)) {
                    if(IS_LEAF(child2)) {
                        leaf_intersect(LEAF_RAW(child1), GET_OFFSET(child1), LEAF_RAW(child2), GET_OFFSET(child2), result);
                        iter_next_without_skip(iter1);
                        iter_next_without_skip(iter2);
                    } else {
                        node_leaf_intersect(child2, LEAF_RAW(child1), GET_OFFSET(child1), result);
                        iter_next_without_skip(iter1);
                        iter_next(iter2);
                    }
                } else {
                    if(IS_LEAF(child2)) {
                        node_leaf_intersect(child1, LEAF_RAW(child2), GET_OFFSET(child2), result);
                        iter_next_without_skip(iter2);
                        iter_next(iter1);
                    } else {
                        node_intersect(child1, child2, result);
                        iter_next(iter1);
                        iter_next(iter2);
                    }
                }
            }
            destroy_iterator(iter1);
            destroy_iterator(iter2);
        }
    }

    uint64_t node_range_intersect(ARTNode* node, RangeElement* range, uint16_t range_size) {
        uint16_t cur_idx1 = 0;
        uint64_t range_last_element = range[range_size - 1];
//        ARTIterator iter{node};
        ARTLeaf* node_leaf = nullptr;
        uint64_t res = 0;

        while(cur_idx1 < range_size) {
            auto raw_leaf = node_search(node, ARTKey(range[cur_idx1]));
            if(!raw_leaf) {
                cur_idx1 += 1;
                continue;
            }
            node_leaf = LEAF_RAW(raw_leaf);
            uint16_t size2 = node_leaf->size;
            if(node_leaf->at(size2 - 1) >= range[cur_idx1]) {
                uint16_t cur_idx2 = GET_OFFSET(raw_leaf);
                while(cur_idx1 < range_size && cur_idx2 < size2) {
                    if(range[cur_idx1] == node_leaf->at(cur_idx2)) {
                        res += 1;
                        cur_idx1 += 1;
                        cur_idx2 += 1;
                    } else if(range[cur_idx1] < node_leaf->at(cur_idx2)) {
                        cur_idx1 += 1;
                    } else {
                        cur_idx2 += 1;
                    }
                }
            } else {
                cur_idx1 += 1;
            }
        }
        return res;
    }

    uint64_t leaf_intersect(ARTLeaf* leaf1, uint8_t leaf_start1, ARTLeaf* leaf2, uint8_t leaf_start2) {
        auto size1 = leaf1->size;
        auto size2 = leaf2->size;
        uint16_t cur_idx1 = leaf_start1;
        uint16_t cur_idx2 = leaf_start2;
        uint8_t byte1 = get_key_byte(leaf1->at(cur_idx1), leaf1->depth);
        uint8_t byte2 = get_key_byte(leaf2->at(cur_idx2), leaf2->depth);
        uint64_t res = 0;

        while(cur_idx1 < size1 && cur_idx2 < size2) {
            if(byte1 != get_key_byte(leaf1->at(cur_idx1), leaf1->depth)) {
                break;
            }
            if(byte2 != get_key_byte(leaf2->at(cur_idx2), leaf2->depth)) {
                break;
            }
            if(leaf1->at(cur_idx1) == leaf2->at(cur_idx2)) {
                res += 1;
                cur_idx1 += 1;
                cur_idx2 += 1;
            } else if(leaf1->at(cur_idx1) < leaf2->at(cur_idx2)) {
                cur_idx1 += 1;
            } else {
                cur_idx2 += 1;
            }
        }
        return res;
    }

    uint64_t node_leaf_intersect(ARTNode* node, ARTLeaf* leaf, uint8_t leaf_start) {
        uint16_t size1 = leaf->size;
        uint8_t byte1 = get_key_byte(leaf->at(leaf_start), leaf->depth);
        uint64_t leaf_last_element = leaf->at(size1 - 1);
        uint16_t cur_idx1 = leaf_start;
        uint64_t res = 0;
        ARTLeaf* node_leaf = nullptr;

//        ARTIterator iter{node};
        while(cur_idx1 < size1) {
            if(byte1 != get_key_byte(leaf->at(cur_idx1), leaf->depth)) {
                break;
            }
            auto raw_leaf = node_search(node, ARTKey(leaf->at(cur_idx1)));
            if(!raw_leaf) {
                cur_idx1 += 1;
                continue;
            }
            node_leaf = LEAF_RAW(raw_leaf);
            uint16_t size2 = node_leaf->size;
            if(node_leaf->at(size2 - 1) >= leaf->at(cur_idx1)) {
                uint16_t cur_idx2 = GET_OFFSET(raw_leaf);
                while(cur_idx1 < size1 && cur_idx2 < size2) {
                    if(leaf->at(cur_idx1) == node_leaf->at(cur_idx2)) {
                        res += 1;
                        cur_idx1 += 1;
                        cur_idx2 += 1;
                    } else if(leaf->at(cur_idx1) < node_leaf->at(cur_idx2)) {
                        cur_idx1 += 1;
                    } else {
                        cur_idx2 += 1;
                    }
                }
            } else {
                cur_idx1 += 1;
            }
        }
        return res;
    }

    uint64_t node_intersect(ARTNode* node1, ARTNode* node2) {
        assert(node1 && node2);
        assert(!IS_LEAF(node1) && !IS_LEAF(node2));
        uint64_t res = 0;
        if(!ARTKey::check_partial_match(node1->prefix, node2->prefix, std::min(node1->depth, node2->depth))) {
            return res;
        }
        if (node1->depth != node2->depth) {
            // swap to make node1's depth <= node2's depth
            if (node1->depth > node2->depth) {
                std::swap(node1, node2);
            }

            auto child = find_child(node1, node2->prefix[node1->depth]);
            if(child) { // must be valid
                assert(*child);
                if(IS_LEAF(*child)) {
                    res += node_leaf_intersect(node2, LEAF_RAW(*child), GET_OFFSET(*child));
                } else {
                    res += node_intersect(*child, node2);
                }
            }
        } else {
            auto iter1 = alloc_iterator(node1);
            auto iter2 = alloc_iterator(node2);

            while (iter1->is_valid() && iter2->is_valid()) {
                std::pair<uint8_t, ARTNode *> byte_pair1 = iter_get(iter1);
                std::pair<uint8_t, ARTNode *> byte_pair2 = iter_get(iter2);
                if (byte_pair1.first < byte_pair2.first) {
                    iter_next_without_skip(iter1);
                    continue;
                } else if (byte_pair1.first > byte_pair2.first) {
                    iter_next_without_skip(iter2);
                    continue;
                }
                auto child1 = byte_pair1.second;
                auto child2 = byte_pair2.second;
                if(IS_LEAF(child1)) {
                    if(IS_LEAF(child2)) {
                        res += leaf_intersect(LEAF_RAW(child1), GET_OFFSET(child1), LEAF_RAW(child2), GET_OFFSET(child2));
                        iter_next_without_skip(iter1);
                        iter_next_without_skip(iter2);
                    } else {
                        res += node_leaf_intersect(child2, LEAF_RAW(child1), GET_OFFSET(child1));
                        iter_next_without_skip(iter1);
                        iter_next(iter2);
                    }
                } else {
                    if(IS_LEAF(child2)) {
                        res += node_leaf_intersect(child1, LEAF_RAW(child2), GET_OFFSET(child2));
                        iter_next_without_skip(iter2);
                        iter_next(iter1);
                    } else {
                        res += node_intersect(child1, child2);
                        iter_next(iter1);
                        iter_next(iter2);
                    }
                }
            }
            destroy_iterator(iter1);
            destroy_iterator(iter2);
        }
        return res;
    }

    void check_node(ARTNode* node) {
        switch (node->type) {
            case NODE4: {
                auto node4 = (ARTNode_4*)node;
                for(int i = 0; i < node4->n.num_children; i++) {
                    auto cur_child = node4->children[i];
                    if(IS_LEAF(cur_child)) {
                        assert(LEAF_RAW(cur_child)->depth == node->depth);
                    }
                }
                break;
            }
            case NODE16: {
                auto node16 = (ARTNode_16*)node;
                for(int i = 0; i < node16->n.num_children; i++) {
                    auto cur_child = node16->children[i];
                    if(IS_LEAF(cur_child)) {
                        assert(LEAF_RAW(cur_child)->depth == node->depth);
                    }
                }
                break;
            }
            case NODE48: {
                auto node48 = (ARTNode_48*)node;
                for(int i = 0; i < 256; i++) {
                    if(node48->keys[i]) {
                        auto cur_child = node48->children[node48->keys[i] - 1];
                        if(IS_LEAF(cur_child)) {
                            assert(LEAF_RAW(cur_child)->depth == node->depth);
                        }
                    }
                }
                break;
            }
            case NODE256: {
                auto node256 = (ARTNode_256*)node;
                for(int i = 0; i < 256; i++) {
                    if(node256->children[i] && IS_LEAF(node256->children[i])) {
                        assert(LEAF_RAW(node256->children[i])->depth == node->depth);
                    }
                }
                break;
            }
            default:
                throw std::runtime_error("check_node(): Invalid node type");
        }
    }

}