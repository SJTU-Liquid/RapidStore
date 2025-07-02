#include "include/art_iter.h"
#include "include/art_node_ops.h"

namespace container {
    ARTIterator::ARTIterator(ARTNode* root): leaf(nullptr), path_len(0) {
        // find first leaf
        ARTNode* cur_node = root;
        while(leaf == nullptr) {
            alloc_iterator_ref(cur_node, path[path_len]);
            cur_node = std::visit([](auto&& iter) {
                return iter.get().second;
            }, path[path_len]);

            path_len += 1;

            if(IS_LEAF(cur_node)) {
                leaf = LEAF_RAW(cur_node);
            }
        }
        assert(leaf != nullptr && path_len > 0);
    }

    bool ARTIterator::depth_step(uint8_t depth) {
        assert(depth < 5);
        assert(path_len > 0);
        uint8_t path_idx = std::min(depth, (uint8_t)(path_len - 1));
        // find the depth
        while(path_idx >= 0) {
            uint8_t path_depth = std::visit([](auto &&iter) -> uint8_t {
                ARTNode* node = iter.get_node();
                return node->depth;
            }, path[path_idx]);

            if(path_depth == depth) {   // no path compression
                break;
            } else if (path_depth < depth || path_idx == 0) {
                return false;
            } else {    // oops, go back
                path_idx--;
            }
        }

        bool is_valid = std::visit([](auto&& iter) {
            ++iter;
            return iter.is_valid();
        }, path[path_idx]);

        // update following path
        if(is_valid) {
            leaf = nullptr;
            ARTNode* cur_node = nullptr;
            while (leaf == nullptr) {
                cur_node = std::visit([](auto &&iter) {
                    return *iter;
                }, path[path_idx]);

                if(!IS_LEAF(cur_node)) {
                    path_idx += 1;
                    alloc_iterator_ref(cur_node, path[path_idx]);
                } else {
                    leaf = LEAF_RAW(cur_node);
                }
            }
            path_len = path_idx + 1;
        }

        return is_valid;
    }

    ARTLeaf* ARTIterator::target_step(uint64_t target) {
        assert(path_len > 0);
        ARTKey key{target};

        uint8_t target_depth = std::visit([key](auto &&iter) -> uint8_t {
            return ARTKey::longest_common_prefix(iter.node->n.prefix, key);
        }, path[path_len - 1]);
        uint8_t path_idx = path_len - 1;

        if(target_depth < KEY_LEN - 1) {
            if(path_len - 1 <= target_depth) {
                return nullptr;
            }
            // find the depth
            uint8_t path_depth = std::visit([](auto &&iter) -> uint8_t {
                ARTNode* node = iter.get_node();
                return node->depth;
            }, path[target_depth]);

            if(path_depth < target_depth) {
                return nullptr;
            }

            path_idx = target_depth;
            if(path_idx != 0) {
                path_idx -= 1;  // go back to the previous node
            }
        }

        ARTNode* cur_node = nullptr;
        ARTLeaf* leaf = nullptr;
        while (true) {
            cur_node = std::visit([](auto &&iter) {
                return *iter;
            }, path[path_idx]);

            auto child = find_child(cur_node, key[cur_node->depth]);
            if(child == nullptr) {
                break;
            }
            
            if(IS_LEAF(*child)) {
                leaf = LEAF_RAW(*child);
                break;
            } else {
                if(ARTKey::longest_common_prefix((*child)->prefix, key) >= (*child)->depth) {
                    path_idx += 1;
                    cur_node = *child;
                    alloc_iterator_ref(cur_node, path[path_idx]);
                } else {
                    break;
                }
            }
        }

        path_len = path_idx + 1;
        return leaf;
    }

    bool ARTIterator::path_step(uint8_t path_idx) {
        assert(path_idx < path_len);
        bool is_valid = std::visit([](auto&& iter) {
            ++iter;
            return iter.is_valid();
        }, path[path_idx]);

        if(is_valid) {
            leaf = nullptr;
            ARTNode* cur_node = nullptr;
            while (leaf == nullptr) {
                cur_node = std::visit([](auto &&iter) {
                    return *iter;
                }, path[path_idx]);

                if(!IS_LEAF(cur_node)) {
                    path_idx += 1;
                    alloc_iterator_ref(cur_node, path[path_idx]);
                } else {
                    leaf = LEAF_RAW(cur_node);
                }
            }
            path_len = path_idx + 1;
        }
        return is_valid;
    }

    void ARTIterator::operator++() {
        int path_idx = path_len - 1;
        while(path_idx >= 0 && !path_step(path_idx)) {
            path_idx -= 1;
        }

        if(path_idx >= 0) {
            auto leaf_raw = std::visit([](auto &&iter) -> ARTNode * {
                return *iter;
            }, path[path_len - 1]);
            assert(IS_LEAF(leaf_raw));
            leaf = LEAF_RAW(leaf_raw);
        }
    }

    bool ARTIterator::is_valid() const {
        return std::visit([](auto&& iter) -> bool {
            return iter.is_valid();
        }, path[0]);
    }

    ARTLeaf* ARTIterator::get() const {
        return leaf;
    }
}