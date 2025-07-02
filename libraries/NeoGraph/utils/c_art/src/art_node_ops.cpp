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

#ifndef NDEBUG
        assert(!n->children[c]);
//        if(n->n.prefix.key == 131328 && c == 3 && n->n.depth == 2) {
//            std::cout << "debug" << std::endl;
//        }
#endif
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
#ifndef NDEBUG
        if(IS_LEAF(child)) {
            assert(GET_OFFSET(child) < ART_LEAF_SIZE);
        }
//        if(c == 112 && GET_OFFSET(child) == 0x8f) {
//            std::cout << "debug" << std::endl;
//        }
#endif
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

    void remove_child256(ARTNode_256 *n, ARTNode **ref, unsigned char c, ARTNode **child) {
        n->children[c] = nullptr;
        n->unique_bitmap.reset(c);
        n->n.num_children--;
    }

    void remove_child48(ARTNode_48 *n, ARTNode **ref, unsigned char c, ARTNode **child) {
        n->children[n->keys[c] - 1] = nullptr;
        n->keys[c] = 0;
        n->unique_bitmap.reset(c);
        n->n.num_children--;
    }

    void remove_child16(ARTNode_16 *n, ARTNode **ref, unsigned char c, ARTNode **child) {
        uint8_t offset = child - n->children;
        std::copy(n->keys + offset + 1, n->keys + n->n.num_children, n->keys + offset);
        std::copy(n->children + offset + 1, n->children + n->n.num_children, n->children + offset);
        n->n.num_children--;
    }

    void remove_child4(ARTNode_4 *n, ARTNode **ref, unsigned char c, ARTNode **child) {
        uint8_t offset = child - n->children;
        std::copy(n->keys + offset + 1, n->keys + n->n.num_children, n->keys + offset);
        std::copy(n->children + offset + 1, n->children + n->n.num_children, n->children + offset);
        n->n.num_children--;
    }

    void remove_child(ARTNode *n, ARTNode **ref, unsigned char c, ARTNode **child) {
        switch (n->type) {
            case NODE4:
                remove_child4((ARTNode_4 *) n, ref, c, child);
                break;
            case NODE16:
                remove_child16((ARTNode_16 *) n, ref, c, child);
                break;
            case NODE48:
                remove_child48((ARTNode_48 *) n, ref, c, child);
                break;
            case NODE256:
                remove_child256((ARTNode_256 *) n, ref, c, child);
                break;
            default:
                throw std::runtime_error("add_child(): Invalid node type");
        }
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

    void node_pointers_update(ARTNode* node, ARTNode** child, ARTKey key, int offset) {
        assert(offset < 256);
        assert(offset > -256);
        // normal case
        auto leaf = LEAF_RAW(*child);
        switch(node->type) {
            case NODE4: {
                auto node4 = (ARTNode_4*) node;
                auto cur_ptr = child + 1;
                while(cur_ptr < node4->children + node4->n.num_children) {
                    if(IS_LEAF(*cur_ptr)) {
                        if(LEAF_RAW(*cur_ptr) == leaf) {
                            *cur_ptr = (ARTNode* )LEAF_POINTER_CTOR(*cur_ptr, GET_OFFSET(*cur_ptr) + offset);
                        } else {
                            return;
                        }
                    }
                    cur_ptr += 1;
                }
                break;
            }
            case NODE16: {
                auto node16 = (ARTNode_16*) node;
                auto cur_ptr = child + 1;
                while(cur_ptr < node16->children + node16->n.num_children) {
                    if(IS_LEAF(*cur_ptr)) {
                        if(LEAF_RAW(*cur_ptr) == leaf) {
                            *cur_ptr = (ARTNode* )LEAF_POINTER_CTOR(*cur_ptr, GET_OFFSET(*cur_ptr) + offset);
                        } else {
                            return;
                        }
                    }
                    cur_ptr += 1;
                }
                break;
            }
            case NODE48: {
                auto node48 = (ARTNode_48*) node;
                auto cur_byte = key[node->depth] + 1;
                while(cur_byte <= 255) {
                    if(node48->keys[cur_byte]) {
                        auto cur_ptr = &node48->children[node48->keys[cur_byte] - 1];
                        if(IS_LEAF(*cur_ptr)) {
                            if(LEAF_RAW(*cur_ptr) == leaf) {
                                *cur_ptr = (ARTNode* )LEAF_POINTER_CTOR(*cur_ptr, GET_OFFSET(*cur_ptr) + offset);
                            } else {
                                return;
                            }
                        }
                    }
                    cur_byte += 1;
                }
                break;
            }
            case NODE256: {
                auto node256 = (ARTNode_256*) node;
                auto cur_ptr = child + 1;
                while(cur_ptr < node256->children.begin() + 256) {
                    if(*cur_ptr) {
                        if(IS_LEAF(*cur_ptr) && LEAF_RAW(*cur_ptr) == leaf) {
                            *cur_ptr = (ARTNode* )LEAF_POINTER_CTOR(*cur_ptr, GET_OFFSET(*cur_ptr) + offset);
                        } else {
                            // node pointer or leaf pointer with different leaf
                            return;
                        }
                    }
                    cur_ptr += 1;
                }
                break;
            }
            default:
                throw std::runtime_error("node_pointers_update(): Invalid node type");
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
        uint16_t left_byte_count = 0;
        uint16_t right_byte_count = 0;

        auto new_leaf_left = alloc_leaf(ARTKey{leaf->at(0)}, depth, false, true, trace_block);
        auto new_leaf_right = alloc_leaf(leaf->key, depth, false, true, trace_block);

        ARTNode* new_node = nullptr;
        new_node = (ARTNode*) alloc_node(NODE4, new_leaf_left->key, depth, trace_block);

        while (ed < leaf->size) {
            assert(cur_byte <= get_key_byte(leaf->at(ed), depth));   // the array should be sorted
            cur_byte = get_key_byte(leaf->at(ed), depth);
            while(ed < leaf->size && get_key_byte(leaf->at(ed), depth) == cur_byte) {
                ed++;
            }

            if((ed < leaf->size / 2 || st == 0) && ed != leaf->size) {
                add_child(new_node, (ARTNode **) &new_node, cur_byte,  LEAF_POINTER_CTOR(new_leaf_left, st), trace_block);
                left_byte_count += 1;
            } else {
                break;
            }
            st = ed;
        }

        new_leaf_left->size = st;
        new_leaf_right->size = leaf->size - st;
        leaf->copy_to_leaf(0, st, new_leaf_left, 0);
        leaf->copy_to_leaf(st, leaf->size, new_leaf_right, 0);

        add_child(new_node, (ARTNode **) &new_node, cur_byte,  LEAF_POINTER_CTOR(new_leaf_right, 0), trace_block);
        right_byte_count += 1;
        st = ed;

        while (ed < leaf->size) {
            cur_byte = get_key_byte(leaf->at(ed), depth);
            while(ed < leaf->size && get_key_byte(leaf->at(ed), depth) == cur_byte) {
                ed++;
            }

            add_child(new_node, (ARTNode **) &new_node, cur_byte, LEAF_POINTER_CTOR(new_leaf_right, st - new_leaf_left->size), trace_block);
            right_byte_count += 1;

            st = ed;
        }

        // remount the leaf
        if(left_byte_count == 1) {
            new_leaf_left->is_single_byte = true;
        }
        if(right_byte_count == 1) {
            new_leaf_right->is_single_byte = true;
        }
        new_leaf_left->key = ARTKey(leaf->at(0), depth, new_leaf_left->is_single_byte);
        new_leaf_right->key = ARTKey(leaf->at(new_leaf_left->size), depth, new_leaf_right->is_single_byte);
        *n = (ARTNode*) new_node;
        return leaf;
    }

    ARTLeaf* find_leaf4(ARTNode_4* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block) {
        auto cur_child = child - 1;
        if (cur_child >= node->children && IS_LEAF(*cur_child)) {
            return LEAF_RAW(*cur_child);
        }
        cur_child = child + 1;
        if (cur_child < node->children + node->n.num_children && IS_LEAF(*cur_child)) {
            return LEAF_RAW(*cur_child);
        }
        return alloc_leaf(key, node->n.depth, true, true, trace_block);
    }

    ARTLeaf* find_leaf16(ARTNode_16* node, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block) {
        auto cur_child = child - 1;
        if (cur_child >= node->children && IS_LEAF(*cur_child)) {
            return LEAF_RAW(*cur_child);
        }
        cur_child = child + 1;
        if (cur_child < node->children + node->n.num_children && IS_LEAF(*cur_child)) {
            return LEAF_RAW(*cur_child);
        }
        return alloc_leaf(key, node->n.depth, true, true, trace_block);
    }

    ARTLeaf* find_leaf48(ARTNode_48* node, ARTKey key, WriterTraceBlock* trace_block) {
        int cur_byte = key[node->n.depth] - 1;
        // <-
        while(cur_byte >= 0) {
            if(node->keys[cur_byte]) {
                auto child_ptr = node->children[node->keys[cur_byte] - 1];
                if(IS_LEAF(child_ptr)) {
                    return LEAF_RAW(child_ptr);
                } else {
                    break;
                }
            }
            cur_byte -= 1;
        }
        cur_byte = key[node->n.depth] + 1;
        while(cur_byte <= 255) {
            if(node->keys[cur_byte]) {
                auto child_ptr = node->children[node->keys[cur_byte] - 1];
                if(IS_LEAF(child_ptr)) {
                    return LEAF_RAW(child_ptr);
                } else {
                    break;
                }
            }
            cur_byte += 1;
        }
        return alloc_leaf(key, node->n.depth, true, true, trace_block);
    }

    ARTLeaf* find_leaf256(ARTNode_256* node, ARTKey key, WriterTraceBlock* trace_block) {
        ARTNode** cur_ptr = &node->children[key[node->n.depth] - 1];
        // <-
        while(cur_ptr >= &node->children[0]) {
            if(*cur_ptr) {
                if(IS_LEAF(*cur_ptr)) {
                    return LEAF_RAW(*cur_ptr);
                } else {
                    break;
                }
            }
            cur_ptr -= 1;
        }
        // ->
        cur_ptr = &node->children[key[node->n.depth] + 1];
        while(cur_ptr <= &node->children[255]) {
            if(*cur_ptr) {
                if(IS_LEAF(*cur_ptr)) {
                    return LEAF_RAW(*cur_ptr);
                } else {
                    break;
                }
            }
            cur_ptr += 1;
        }
        return alloc_leaf(key, node->n.depth, true, true, trace_block);
    }

    ///@brief find the leaf that can be inserted from the child pointer.
    ///@param n the node that contains the child pointer
    ///@param child the child pointer that to be inserted
    ///@return the leaf that can be inserted, nullptr if need to allocate new leaf
    ARTLeaf* find_leaf(ARTNode* n, ARTNode** child, ARTKey key, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                return find_leaf4((ARTNode_4*) n, child, key, trace_block);
            case NODE16:
                return find_leaf16((ARTNode_16*) n, child, key, trace_block);
            case NODE48:
                return find_leaf48((ARTNode_48*) n, key, trace_block);
            case NODE256:
                return find_leaf256((ARTNode_256*) n, key, trace_block);
            default:
                throw std::runtime_error("find_leaf(): Invalid node type");
        }
    }

    ARTNodeSplitRes node_split4(ARTNode_4 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block) {
        assert(IS_LEAF(*child) || *child == nullptr);
        // reach the beginning
        ARTNode** begin_ptr = node->children;
        while(begin_ptr < node->children + node->n.num_children) {
            if(IS_LEAF(*begin_ptr) && LEAF_RAW(*begin_ptr) == leaf) {
                break;
            }
            begin_ptr += 1;
        }
        // ensure we find the target
        assert(LEAF_RAW(*begin_ptr) == leaf);
        // find the end
        auto end_ptr = child + 1;
        while(end_ptr < node->children + node->n.num_children) {
            if(IS_LEAF(*end_ptr)) { // filter out the nullptr child automatically
                if(LEAF_RAW(*end_ptr) != leaf) {
                    break;
                }
                end_ptr += 1;
            } else {
                break;
            }
        }

        // single byte check
        if(*child == nullptr) {
            if(end_ptr == child + 1 || begin_ptr == child + 1) {
                // cannot go deeper
                auto new_leaf = alloc_leaf(key, leaf->depth, true, true, trace_block);
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            }
        } else if (end_ptr == begin_ptr + 1) {
            // go deeper
            assert(node->n.depth != 4);
            auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
            delete old_leaf;
            return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
        }

        ARTNode** mid_ptr = find_mid_count(begin_ptr, end_ptr);
        assert(mid_ptr != begin_ptr);

        uint16_t segment_mid_idx = GET_OFFSET(*mid_ptr);
        auto new_leaf = alloc_leaf(ARTKey{leaf->at(leaf->size - 1)}, leaf->depth, false, true, trace_block);   // TODO still can be optimized, same to the other node_split()

        new_leaf->size = leaf->size - segment_mid_idx;
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf, 0);
        // erase [mid_ptr, end_ptr) from the old segment
        leaf->size = segment_mid_idx;

        assert(leaf->depth == node->n.depth);

        // modify the node
        if(*child == nullptr) {
            for(auto cur_ptr = mid_ptr; cur_ptr < end_ptr; cur_ptr++) {
                *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*cur_ptr) - segment_mid_idx));
            }
            *child = nullptr;
            return {ARTNodeSplitStatus::SPLIT, (child >= mid_ptr) ? new_leaf : leaf};
        } else {
            for(auto cur_ptr = mid_ptr; cur_ptr < end_ptr; cur_ptr++) {
                *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*cur_ptr) - segment_mid_idx));
            }
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split16(ARTNode_16 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block) {
        assert(IS_LEAF(*child) || *child == nullptr);
        // reach the beginning
        ARTNode** begin_ptr = node->children;
        while(begin_ptr < node->children + node->n.num_children) {
            if(IS_LEAF(*begin_ptr) && LEAF_RAW(*begin_ptr) == leaf) {
                break;
            }
            begin_ptr += 1;
        }
        // ensure we find the target
        assert(LEAF_RAW(*begin_ptr) == leaf);
        // find the end
        auto end_ptr = child + 1;
        while(end_ptr < node->children + node->n.num_children) {
            if(IS_LEAF(*end_ptr)) { // filter out the nullptr child automatically
                if(LEAF_RAW(*end_ptr) != leaf) {
                    break;
                }
                end_ptr += 1;
            } else {
                break;
            }
        }

        // single byte check
        if(*child == nullptr) {
            if(end_ptr == child + 1 || begin_ptr == child + 1) {
                // cannot go deeper
                auto new_leaf = alloc_leaf(key, leaf->depth, true, true, trace_block);
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            }
        } else if (end_ptr == begin_ptr + 1) {
            // go deeper
            assert(node->n.depth != 4);
            auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
            delete old_leaf;
            return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
        }

        ARTNode** mid_ptr = find_mid_count(begin_ptr, end_ptr);
        assert(mid_ptr != begin_ptr);

        uint16_t segment_mid_idx = GET_OFFSET(*mid_ptr);

        auto new_leaf = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, false, true, trace_block);
        new_leaf->size = leaf->size - segment_mid_idx;
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf, 0);
        // erase [mid_ptr, end_ptr) from the old segment
        leaf->size = segment_mid_idx;

        assert(leaf->depth == node->n.depth);

        // modify the node
        if(*child == nullptr) {
            for(auto cur_ptr = mid_ptr; cur_ptr < end_ptr; cur_ptr++) {
                *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*cur_ptr) - segment_mid_idx));
            }
            *child = nullptr;
            return {ARTNodeSplitStatus::SPLIT, (child >= mid_ptr) ? new_leaf : leaf};
        } else {
            for(auto cur_ptr = mid_ptr; cur_ptr < end_ptr; cur_ptr++) {
                *cur_ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*cur_ptr) - segment_mid_idx));
            }
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split48(ARTNode_48 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block) {
        std::vector<ARTNode **> pointers_in_range{};
        bool empty_child = (child == nullptr);
        if(empty_child) {
            child = &node->children[key[node->n.depth] - 1];
        }

        int cur_byte = key[node->n.depth];
        // <-
        while(cur_byte >= 0) {
            if(node->keys[cur_byte]) {
                auto cur_ptr = &node->children[node->keys[cur_byte] - 1];
                if(IS_LEAF(*cur_ptr)) {
                    if(LEAF_RAW(*cur_ptr) == leaf) {
                        pointers_in_range.push_back(cur_ptr);
                    } else {
                        break;
                    }
                }
            }
            cur_byte -= 1;
        }
        std::reverse(pointers_in_range.begin(), pointers_in_range.end());
        cur_byte = key[node->n.depth] + 1;
        while(cur_byte <= 255) {
            auto cur_ptr = &node->children[node->keys[cur_byte] - 1];
            if(IS_LEAF(*cur_ptr)) {
                if(LEAF_RAW(*cur_ptr) == leaf) {
                    pointers_in_range.push_back(cur_ptr);
                } else {
                    break;
                }
            }
            cur_byte += 1;
        }

        if(pointers_in_range.size() <= 1) {
            if(empty_child || (node->n.num_children == 17 && *child == nullptr)) {  // second condition is for the case that Node16 is upgraded to Node48
                assert(node->n.depth == 4);
                auto new_leaf = alloc_leaf(key, leaf->depth, true, true, trace_block);
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            } else {
                assert(node->n.depth != 4);
                auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
                delete old_leaf;
                return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
            }
        }

        uint16_t mid_idx = find_mid_count(&pointers_in_range);
        assert(mid_idx != 0);

        uint16_t segment_mid_idx = GET_OFFSET(*pointers_in_range[mid_idx]);
        auto new_leaf = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, false, true, trace_block);
        new_leaf->size = leaf->size - segment_mid_idx;
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf, 0);
        // erase [mid_ptr, end_ptr) from the old segment
        leaf->size = segment_mid_idx;

        assert(leaf->depth == node->n.depth);

        // modify the node
        for(int cur_idx = mid_idx; cur_idx < pointers_in_range.size(); cur_idx++) {
            ARTNode** ptr = pointers_in_range[cur_idx];
            *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr) - segment_mid_idx));
        }

        if(empty_child) {
            return {ARTNodeSplitStatus::SPLIT, (child >= pointers_in_range[mid_idx]) ? new_leaf : leaf};
        } else {
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split256(ARTNode_256 *node, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block) {
        std::vector<ARTNode **> pointers_in_range{};
        bool empty_child = (child == nullptr);
        if(empty_child) {
            child = &node->children[key[node->n.depth] - 1];
        }

        ARTNode** cur_ptr = &node->children[key[node->n.depth]];
        // <-
        while(cur_ptr >= &node->children[0]) {
            if(*cur_ptr) {
                if(LEAF_RAW(*cur_ptr) == leaf) {
                    pointers_in_range.push_back(cur_ptr);
                } else {
                    break;
                }
            }
            cur_ptr -= 1;
        }
        std::reverse(pointers_in_range.begin(), pointers_in_range.end());
        // ->
        cur_ptr = &node->children[key[node->n.depth] + 1];
        while(cur_ptr <= &node->children[255]) {
            if(*cur_ptr) {
                if(LEAF_RAW(*cur_ptr) == leaf) {
                    pointers_in_range.push_back(cur_ptr);
                } else {
                    break;
                }
            }
            cur_ptr += 1;
        }

        if(pointers_in_range.size() <= 1) {
            if(empty_child) {
                assert(node->n.depth == 4);
                auto new_leaf = alloc_leaf(key, leaf->depth, true, true, trace_block);
                return {ARTNodeSplitStatus::NEW_LEAF, new_leaf};
            } else {
                assert(node->n.depth != 4);
                auto old_leaf = leaf_pointer_expand(child, node->n.depth, trace_block);
                delete old_leaf;
                return {ARTNodeSplitStatus::GO_DEEPER, nullptr};
            }
        }

        uint16_t mid_idx = find_mid_count(&pointers_in_range);
        assert(mid_idx != 0);

        uint16_t segment_mid_idx = GET_OFFSET(*pointers_in_range[mid_idx]);
        auto new_leaf = alloc_leaf(ARTKey{leaf->at(segment_mid_idx)}, leaf->depth, false, true, trace_block);
        new_leaf->size = leaf->size - segment_mid_idx;
        leaf->copy_to_leaf(segment_mid_idx, leaf->size, new_leaf, 0);

        // erase [mid_ptr, end_ptr) from the old segment
        leaf->size = segment_mid_idx;

        assert(leaf->depth == node->n.depth);

        // modify the node
        for(int cur_idx = mid_idx; cur_idx < pointers_in_range.size(); cur_idx++) {
            ARTNode** ptr = pointers_in_range[cur_idx];
            *ptr = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, (GET_OFFSET(*ptr) - segment_mid_idx));
        }

        if(empty_child) {
            return {ARTNodeSplitStatus::SPLIT, (child >= pointers_in_range[mid_idx]) ? new_leaf : leaf};
        } else {
            return {ARTNodeSplitStatus::SPLIT, LEAF_RAW(*child)};
        }
    }

    ARTNodeSplitRes node_split(ARTNode *n, ARTNode** child, ARTLeaf* leaf, ARTKey key, WriterTraceBlock* trace_block) {
        switch (n->type) {
            case NODE4:
                return node_split4((ARTNode_4*)n, child, leaf, key, trace_block);
            case NODE16:
                return node_split16((ARTNode_16*)n,  child, leaf, key, trace_block);
            case NODE48:
                return node_split48((ARTNode_48*)n,  child, leaf, key, trace_block);
            case NODE256:
                return node_split256((ARTNode_256*)n,  child, leaf, key, trace_block);
            default:
                throw std::runtime_error("node_split(): Invalid node type");
        }
    }

    bool insert(ARTNode** n, ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block) {
        auto child = find_child(*n, key[(*n)->depth]);
        bool empty_child = (child == nullptr);
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
            pos = leaf->find(value, GET_OFFSET(*child));
            if(pos != leaf->size && leaf->at(pos) == value) {
                return false;
            }
        } else if ((*n)->num_children == 0) {
            // c-art initial status
            child = add_child(*n, n, key[(*n)->depth], nullptr, trace_block);
            leaf = alloc_leaf(key, (*n)->depth, true, true, trace_block);
            pos = 0;
        } else {
            if((*n)->type <= NODE16) {  // add a child to enable find_leaf()
                if((*n)->num_children == 16) {
                    node16_upgrade((ARTNode_16*)*n, n, trace_block);
                } else {
                    child = add_child(*n, n, key[(*n)->depth], nullptr, trace_block);
                }
            }
            leaf = find_leaf(*n, child, key, trace_block);
            if(leaf->is_single_byte && empty_child) {
                // the leaf will be not single byte anymore
                auto new_leaf = alloc_leaf(ARTKey{leaf->key, leaf->depth, false}, leaf->depth, false, true, trace_block);
                new_leaf->size = leaf->size;
                leaf->copy_to_leaf(0, leaf->size, new_leaf, 0);
                auto iter = alloc_iterator(*n);
                while(iter_is_valid(iter)) {
                    auto cur_child = iter_get_current(iter);
                    if(IS_LEAF(*cur_child) && LEAF_RAW(*cur_child) == leaf) {
                        assert(GET_OFFSET(*cur_child) == 0);
                        *cur_child = (ARTNode*) LEAF_POINTER_CTOR(new_leaf, 0);
                        break;
                    }
                    iter_next(iter);
                }
                destroy_iterator(iter);
                delete leaf;
                leaf = new_leaf;
            }
            pos = leaf->find(value, 0);
        }
        assert(leaf != nullptr);

        // SPLIT check
        assert(leaf->size <= ART_LEAF_SIZE);
        if(leaf->size == ART_LEAF_SIZE) {
            auto split_res = node_split(*n, child, leaf, key, trace_block);
            switch (split_res.status) {
                case ARTNodeSplitStatus::SPLIT: {
                    auto old_leaf = leaf;
                    leaf = (ARTLeaf*) split_res.leaf;
                    // correct the pos
                    if (pos >= old_leaf->size) {
                        pos = pos - old_leaf->size;
                    }
                    break;
                }
                case ARTNodeSplitStatus::NEW_LEAF: {
                    leaf = (ARTLeaf*) split_res.leaf;
                    pos = 0;
                    break;
                }
                case ARTNodeSplitStatus::GO_DEEPER: {
                    // restart
                    auto next_depth_node = find_child(*n, key[(*n)->depth]);
                    return insert(next_depth_node, key, value, property, trace_block);
                }
                default: {
                    throw std::runtime_error("insert(): Invalid split status");
                }
            }
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
        return true;
    }

    ARTNodeRemoveRes remove(ARTNode** n, ARTKey key, uint64_t value) {
        auto child = find_child(*n, key[(*n)->depth]);
        if(child == nullptr && !IS_LEAF(*child)) {
            return NOT_FOUND;
        }

        auto leaf = LEAF_RAW(*child);
        auto pos = leaf->find(value, GET_OFFSET(*child));
        if(pos == leaf->size || leaf->at(pos) != value) {
            return NOT_FOUND;
        }

        // remove the element
        leaf->remove(pos, key[(*n)->depth]);

        if(leaf->size == 0) {
            delete leaf;
            remove_child(*n, n, key[(*n)->depth], child);
            return CHILD_REMOVED;
        } else {
            // check if the byte segment is empty
            if(get_key_byte(leaf->at(pos), leaf->depth) != key[leaf->depth]) {
                // remove the child
                remove_child(*n, n, key[(*n)->depth], child);
                node_pointers_update(*n, child, key, -1);
                return CHILD_REMOVED;
            } else {
                node_pointers_update(*n, child, key, -1);
                return ELEMENT_REMOVED;
            }
        }
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
            auto l = LEAF_RAW(n);
            if(l->ref_cnt.fetch_sub(1) == 1) {
                leaf_clean(l, trace_block);
                delete l;
            }
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
                ARTNode* last_leaf_ptr = nullptr;
                for(int i = 0; i < 256; i++) {
                    if(node256->children[i] && IS_LEAF(node256->children[i])) {
                        if(LEAF_RAW(last_leaf_ptr) != LEAF_RAW(node256->children[i])) {
                            assert(GET_OFFSET(node256->children[i]) == 0);
                        } else {
                            assert(GET_OFFSET(last_leaf_ptr) < GET_OFFSET(node256->children[i]));
                        }
                        last_leaf_ptr = node256->children[i];
                    }
                }
                break;
            }
            default:
                throw std::runtime_error("check_node(): Invalid node type");
        }
    }


    uint64_t empty_leaf_batch_insert8(ARTNode** n, ARTLeaf8** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(list_size <= ART_LEAF_SIZE);
        uint8_t depth = (*n)->depth;
        uint8_t cur_byte;
        uint64_t cur_byte_st = 0;
        uint64_t cur_byte_ed = 0;

        do {
            cur_byte = get_key_byte(insert_list[cur_byte_st], depth);
            while(cur_byte_ed < list_size && get_key_byte(insert_list[cur_byte_ed], depth) == cur_byte) {
                cur_byte_ed++;
            }

            for(uint64_t i = 0; i < cur_byte_ed - cur_byte_st; i++) {
                (*leaf)->value.set(insert_list[cur_byte_st + i] & 0xFF);
#if EDGE_PROPERTY_NUM != 0
                map_set_sa_art_property((*leaf)->property_map, (*leaf)->size + i, properties[cur_byte_st + i]);
#endif
            }
            add_child(*n, n, cur_byte, LEAF_POINTER_CTOR(*leaf, (*leaf)->size), trace_block);
            (*leaf)->size += cur_byte_ed - cur_byte_st;

            cur_byte_st = cur_byte_ed;
        } while(cur_byte_ed < list_size);

        return list_size;
    }

    uint64_t empty_leaf_batch_insert16(ARTNode** n, ARTLeaf16** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(list_size <= ART_LEAF_SIZE);
        uint8_t depth = (*n)->depth;
        uint8_t cur_byte;
        uint64_t cur_byte_st = 0;
        uint64_t cur_byte_ed = 0;

        do {
            cur_byte = get_key_byte(insert_list[cur_byte_st], depth);
            while(cur_byte_ed < list_size && get_key_byte(insert_list[cur_byte_ed], depth) == cur_byte) {
                cur_byte_ed++;
            }

            for(uint64_t i = 0; i < cur_byte_ed - cur_byte_st; i++) {
                (*leaf)->value->at((*leaf)->size + i) = insert_list[cur_byte_st + i] & 0xFFFF;
#if EDGE_PROPERTY_NUM != 0
                map_set_sa_art_property((*leaf)->property_map, (*leaf)->size + i, properties[cur_byte_st + i]);
#endif
            }
            add_child(*n, n, cur_byte, LEAF_POINTER_CTOR(*leaf, (*leaf)->size), trace_block);
            (*leaf)->size += cur_byte_ed - cur_byte_st;

            cur_byte_st = cur_byte_ed;
        } while(cur_byte_ed < list_size);

        return list_size;
    }

    uint64_t empty_leaf_batch_insert32(ARTNode** n, ARTLeaf32** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(list_size <= ART_LEAF_SIZE);
        uint8_t depth = (*n)->depth;
        uint8_t cur_byte;
        uint64_t cur_byte_st = 0;
        uint64_t cur_byte_ed = 0;

        do {
            cur_byte = get_key_byte(insert_list[cur_byte_st], depth);
            while(cur_byte_ed < list_size && get_key_byte(insert_list[cur_byte_ed], depth) == cur_byte) {
                cur_byte_ed++;
            }

            for(uint64_t i = 0; i < cur_byte_ed - cur_byte_st; i++) {
                (*leaf)->value->at((*leaf)->size + i) = insert_list[cur_byte_st + i] & 0xFFFFFFFF;
#if EDGE_PROPERTY_NUM != 0
                map_set_sa_art_property((*leaf)->property_map, (*leaf)->size + i, properties[cur_byte_st + i]);
#endif
            }
            add_child(*n, n, cur_byte, LEAF_POINTER_CTOR(*leaf, (*leaf)->size), trace_block);
            (*leaf)->size += cur_byte_ed - cur_byte_st;

            cur_byte_st = cur_byte_ed;
        } while(cur_byte_ed < list_size);

        return list_size;
    }

    uint64_t empty_leaf_batch_insert64(ARTNode** n, ARTLeaf64** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(list_size <= ART_LEAF_SIZE);
        uint8_t depth = (*n)->depth;
        uint8_t cur_byte;
        uint64_t cur_byte_st = 0;
        uint64_t cur_byte_ed = 0;

        do {
            cur_byte = get_key_byte(insert_list[cur_byte_st], depth);
            while(cur_byte_ed < list_size && get_key_byte(insert_list[cur_byte_ed], depth) == cur_byte) {
                cur_byte_ed++;
            }

            for(uint64_t i = 0; i < cur_byte_ed - cur_byte_st; i++) {
                (*leaf)->value->at((*leaf)->size + i) = insert_list[cur_byte_st + i];
#if EDGE_PROPERTY_NUM != 0
                map_set_sa_art_property((*leaf)->property_map, (*leaf)->size + i, properties[cur_byte_st + i]);
#endif
            }
            add_child(*n, n, cur_byte, LEAF_POINTER_CTOR(*leaf, (*leaf)->size), trace_block);
            (*leaf)->size += cur_byte_ed - cur_byte_st;

            cur_byte_st = cur_byte_ed;
        } while(cur_byte_ed < list_size);

        return list_size;
    }

    uint64_t empty_leaf_batch_insert(ARTNode** n, ARTLeaf** leaf, RangeElement* insert_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(leaf && *leaf);
        switch((*leaf)->depth + (*leaf)->is_single_byte) {
            case 0:
            case 1: {
                return empty_leaf_batch_insert32(n, (ARTLeaf32 **) leaf, insert_list, properties, list_size, trace_block);
            }
            case 2: {
                return empty_leaf_batch_insert16(n, (ARTLeaf16 **) leaf, insert_list, properties, list_size, trace_block);
            }
            case 3: {
                return empty_leaf_batch_insert8(n, (ARTLeaf8 **) leaf, insert_list, properties, list_size, trace_block);
            }
            default: {
                throw std::runtime_error("empty_leaf_batch_insert(): Invalid depth");
            }
        }
    }

    uint64_t leaf_list_merge_batch_insert(ARTNode** n, ARTLeaf* &new_leaf, uint8_t cur_byte, ARTLeaf* leaf, uint16_t leaf_count, RangeElement* insert_list, Property_t** properties, uint64_t list_count, WriterTraceBlock* trace_block) {
        uint64_t leaf_idx = GET_OFFSET(leaf);
        leaf = LEAF_RAW(leaf);
        uint64_t inserted = list_count;

        uint64_t list_idx = 0;
        uint64_t leaf_ed = leaf_idx + leaf_count;
        uint64_t total_count = leaf_count + list_count;

        if(new_leaf == nullptr || new_leaf->size + total_count > ART_LEAF_SIZE) {
            if(total_count > ART_LEAF_SIZE) { // new node
                auto batch_build_list = new std::vector<RangeElement>();
                batch_build_list->reserve(total_count);
                auto batch_build_prop_list = new std::vector<Property_t *>();
                while(leaf_idx < leaf_ed && list_idx < list_count) {
                    if(leaf->at(leaf_idx) < insert_list[list_idx]) {
                        batch_build_list->push_back(leaf->at(leaf_idx));
#if EDGE_PROPERTY_NUM > 0
                        batch_build_prop_list->push_back(map_get_all_art_property(leaf->property_map, leaf_idx));
#endif
                        leaf_idx += 1;
                    } else if (leaf->at(leaf_idx) > insert_list[list_idx]) {
                        batch_build_list->push_back(insert_list[list_idx]);
                        batch_build_prop_list->push_back(properties[list_idx]);
                        list_idx++;
                    } else {
                        batch_build_list->push_back(insert_list[list_idx]);
                        batch_build_prop_list->push_back(properties[list_idx]);
                        list_idx++;
                        leaf_idx += 1;
                        inserted -= 1;
                    }
                }
                while (leaf_idx < leaf_ed) {
#if EDGE_PROPERTY_NUM > 0
                    batch_build_prop_list->push_back(map_get_all_art_property(leaf->property_map, leaf_idx));
#endif
                    batch_build_list->push_back(leaf->at(leaf_idx++));
                }
                while(list_idx < list_count) {
                    batch_build_list->push_back(insert_list[list_idx]);
                    batch_build_prop_list->push_back(properties[list_idx]);
                    list_idx++;
                }
                // batch build
                auto new_child = add_child(*n, n, cur_byte, (void*) 0x1, trace_block);
                batch_subtree_build(new_child, leaf->depth + 1, batch_build_list->data(), batch_build_prop_list->data(), batch_build_list->size(), trace_block);
                delete batch_build_list;
                delete batch_build_prop_list;

                // batch insert
//                inserted += batch_insert(new_child, leaf->depth, (*new_child)->depth, insert_list, properties, list_count, trace_block);
                new_leaf = nullptr; // reset
                return inserted;
            } else {
                new_leaf = alloc_leaf(ARTKey{insert_list[0]}, leaf->depth, false, true, trace_block);
            }
        }

        add_child(*n, n, cur_byte, LEAF_POINTER_CTOR(new_leaf, new_leaf->size), trace_block);

        while(leaf_idx < leaf_ed && list_idx < list_count) {
            if(leaf->at(leaf_idx) < insert_list[list_idx]) {
#if EDGE_PROPERTY_NUM > 0
                auto prop = map_get_all_art_property(leaf->property_map, leaf_idx);
                new_leaf->insert(leaf->at(leaf_idx), prop, new_leaf->size);
#else
                new_leaf->insert(leaf->at(leaf_idx), nullptr, new_leaf->size);
#endif
                leaf_idx += 1;
            } else if (leaf->at(leaf_idx) > insert_list[list_idx]) {
                new_leaf->insert(insert_list[list_idx], properties[list_idx], new_leaf->size);
                list_idx++;
            } else {
                new_leaf->insert(insert_list[list_idx], properties[list_idx], new_leaf->size);
                list_idx++;
                leaf_idx += 1;
                inserted -= 1;
            }
        }
        while (leaf_idx < leaf_ed) {
#if EDGE_PROPERTY_NUM > 0
            auto prop = map_get_all_art_property(leaf->property_map, leaf_idx);
            new_leaf->insert(leaf->at(leaf_idx), prop, new_leaf->size);
#else
            new_leaf->insert(leaf->at(leaf_idx), nullptr, new_leaf->size);
#endif
            leaf_idx += 1;
        }
        while(list_idx < list_count) {
            new_leaf->insert(insert_list[list_idx], properties[list_idx], new_leaf->size);
            list_idx += 1;
        }

        return inserted;
    }

    uint64_t leaf_list_merge(ARTNode** n, ARTLeaf* leaf, RangeElement *elem_list, Property_t** properties, uint64_t list_size, WriterTraceBlock* trace_block) {
        assert(list_size != 0);

        ARTLeaf* new_leaf = nullptr;
        uint64_t inserted = 0;

        uint64_t cur_list_st = 0;
        uint64_t cur_list_ed = 0;
        uint16_t cur_list_byte = get_key_byte(elem_list[0], leaf->depth);
        uint64_t cur_leaf_st = 0;
        uint64_t cur_leaf_ed = 0;
        uint16_t cur_leaf_byte = get_key_byte(leaf->at(0), leaf->depth);

        while(cur_list_ed < list_size && cur_leaf_ed < leaf->size) {
            cur_list_byte = get_key_byte(elem_list[cur_list_st], leaf->depth);
            cur_leaf_byte = get_key_byte(leaf->at(cur_leaf_st), leaf->depth);
            if(cur_list_byte < cur_leaf_byte) {
                while(cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], leaf->depth) == cur_list_byte) {
                    cur_list_ed++;
                }
                inserted += cur_list_ed - cur_list_st;
                add_list_segment_to_new_leaf(n, new_leaf, leaf->depth, elem_list + cur_list_st, properties + cur_list_st, cur_list_ed - cur_list_st, cur_list_byte, trace_block);
                cur_list_st = cur_list_ed;
            }
            else if(cur_list_byte > cur_leaf_byte) {
                while(cur_leaf_ed < leaf->size && get_key_byte(leaf->at(cur_leaf_ed), leaf->depth) == cur_leaf_byte) {
                    cur_leaf_ed++;
                }
                add_leaf_segment_to_new_leaf(n, new_leaf, leaf->depth, (ARTLeaf*)LEAF_POINTER_CTOR(leaf, cur_leaf_st), cur_leaf_ed - cur_leaf_st, cur_leaf_byte, trace_block);
                cur_leaf_st = cur_leaf_ed;
            }
            else {
                while(cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], leaf->depth) == cur_list_byte) {
                    cur_list_ed++;
                }
                while(cur_leaf_ed < leaf->size && get_key_byte(leaf->at(cur_leaf_ed), leaf->depth) == cur_leaf_byte) {
                    cur_leaf_ed++;
                }
                inserted += leaf_list_merge_batch_insert(n, new_leaf, cur_list_byte, (ARTLeaf*)LEAF_POINTER_CTOR(leaf, cur_leaf_st), cur_leaf_ed - cur_leaf_st, elem_list + cur_list_st, properties + cur_list_st, cur_list_ed - cur_list_st, trace_block);
                cur_list_st = cur_list_ed;
                cur_leaf_st = cur_leaf_ed;
            }
        }

        if(cur_list_ed < list_size) {
            inserted += list_size - cur_list_ed;
            while (cur_list_ed < list_size) {
                cur_list_byte = get_key_byte(elem_list[cur_list_st], leaf->depth);
                while (cur_list_ed < list_size && get_key_byte(elem_list[cur_list_ed], leaf->depth) == cur_list_byte) {
                    cur_list_ed++;
                }
                add_list_segment_to_new_leaf(n, new_leaf, leaf->depth, elem_list + cur_list_st, properties + cur_list_st, cur_list_ed - cur_list_st, cur_list_byte, trace_block);
                cur_list_st = cur_list_ed;
            }
        }
        while(cur_leaf_ed < leaf->size) {
            cur_leaf_byte = get_key_byte(leaf->at(cur_leaf_st), leaf->depth);
            while(cur_leaf_ed < leaf->size && get_key_byte(leaf->at(cur_leaf_ed), leaf->depth) == cur_leaf_byte) {
                cur_leaf_ed++;
            }
            add_leaf_segment_to_new_leaf(n, new_leaf, leaf->depth, (ARTLeaf*)LEAF_POINTER_CTOR(leaf, cur_leaf_st), cur_leaf_ed - cur_leaf_st, cur_leaf_byte, trace_block);
            cur_leaf_st = cur_leaf_ed;
        }

        return 0;
    }

    void add_list_segment_to_new_leaf (ARTNode** new_node, ARTLeaf* &new_leaf, uint8_t depth, RangeElement* elem_list, Property_t** prop_list, uint64_t count, uint8_t cur_byte, WriterTraceBlock* trace_block) {
        assert(count != 0);
        if(new_leaf == nullptr || new_leaf->size + count > ART_LEAF_SIZE) {
            if(count > ART_LEAF_SIZE) { // new node
                auto new_child = add_child(*new_node, new_node, cur_byte, (void*) 0x1, trace_block);
#if EDGE_PROPERTY_NUM != 0
                batch_subtree_build(new_child, depth + 1, elem_list, prop_list, count, trace_block);
#else
                batch_subtree_build(new_child, depth + 1, elem_list, nullptr, count, trace_block);
#endif
                new_leaf = nullptr; // reset
                return;
            } else {
                new_leaf = alloc_leaf(ARTKey{elem_list[0]}, depth, false, true, trace_block);
            }
        }
        add_child(*new_node, new_node, cur_byte, LEAF_POINTER_CTOR(new_leaf, new_leaf->size), trace_block);
        for(uint64_t i = 0; i < count; i++) {
#if EDGE_PROPERTY_NUM != 0
            new_leaf->insert(elem_list[i], prop_list[i], new_leaf->size);
#else
            new_leaf->insert(elem_list[i], nullptr, new_leaf->size);
#endif
        }
    }

    void add_leaf_segment_to_new_leaf (ARTNode** new_node, ARTLeaf* &new_leaf, uint8_t depth, ARTLeaf* cur_leaf, uint64_t count, uint8_t cur_byte, WriterTraceBlock* trace_block) {
        assert(count != 0);
#ifndef NDEBUG
//        if((*new_node)->prefix.key == 262144 && depth == 2 && count == 3 && cur_byte == 112) {
//            std::cout << "add_leaf_segment_to_new_leaf(): debug" << std::endl;
//        }
#endif
        auto cur_raw_leaf = LEAF_RAW(cur_leaf);
        auto leaf_st = GET_OFFSET(cur_leaf);
        if(new_leaf == nullptr || new_leaf->size + count > ART_LEAF_SIZE) {
            new_leaf = alloc_leaf(ARTKey{cur_raw_leaf->at(leaf_st)}, depth, false, true, trace_block);
            assert(new_leaf->size == 0);
        }
        cur_raw_leaf->copy_to_leaf(leaf_st, leaf_st + count, new_leaf, new_leaf->size);
        add_child(*new_node, new_node, cur_byte, LEAF_POINTER_CTOR(new_leaf, new_leaf->size), trace_block);
        new_leaf->size += count;
    };

}