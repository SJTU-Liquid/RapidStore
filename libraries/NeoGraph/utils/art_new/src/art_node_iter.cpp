#include "include/art_node_iter.h"

namespace container {
    ARTNodeIterator_4::ARTNodeIterator_4(ARTNode_4 *node) : node(node), current(node->children) {}

    bool ARTNodeIterator_4::is_valid() const {
        return current != node->children + node->n.num_children;
    }

    ARTNode *ARTNodeIterator_4::operator*() {
        if (current == node->children + node->n.num_children) {
            return nullptr;
        }
        return *current;
    }

    std::pair<uint8_t, ARTNode *> ARTNodeIterator_4::get() {
        if (current == node->children + node->n.num_children) {
            return {0, nullptr};
        }
        return {node->keys[current - node->children], *current};
    }

    ARTNode* ARTNodeIterator_4::get_node() {
        return (ARTNode*)node;
    }

    void ARTNodeIterator_4::operator++() {
        if (current == node->children + node->n.num_children) {
            return;
        }
        if(!IS_LEAF(*current)) {
            current++;
        } else {
            auto leaf = LEAF_RAW(*current);
            do {
                current++;
                if (current == node->children + node->n.num_children) {
                    return;
                }
            } while (LEAF_RAW(*current) == leaf);
        }
    }

    void ARTNodeIterator_4::operator++(int step) {
        for (int i = 0; i < step; i++) {
            (*this)++;
        }
    }

    void ARTNodeIterator_4::next_without_skip() {
        if (current == node->children + node->n.num_children) {
            return;
        }
        current++;
    }

    ARTNodeIterator_16::ARTNodeIterator_16(ARTNode_16 *node) : node(node), current(node->children) {}

    bool ARTNodeIterator_16::is_valid() const {
        return current != node->children + node->n.num_children;
    }

    ARTNode *ARTNodeIterator_16::operator*() {
        if (current == node->children + node->n.num_children) {
            return nullptr;
        }
        return *current;
    }

    std::pair<uint8_t, ARTNode *> ARTNodeIterator_16::get() {
        if (current == node->children + node->n.num_children) {
            return {0, nullptr};
        }
        return {node->keys[current - node->children], *current};
    }

    ARTNode* ARTNodeIterator_16::get_node() {
        return (ARTNode*)node;
    }

    void ARTNodeIterator_16::operator++() {
        if (current == node->children + node->n.num_children) {
            return;
        }
        if(!IS_LEAF(*current)) {
            current++;
        } else {
            auto leaf = LEAF_RAW(*current);
            do {
                current++;
                if (current == node->children + node->n.num_children) {
                    return;
                }
            } while (LEAF_RAW(*current) == leaf);
        }
    }

    void ARTNodeIterator_16::operator++(int step) {
        for (int i = 0; i < step; i++) {
            (*this)++;
        }
    }

    void ARTNodeIterator_16::next_without_skip() {
        if (current == node->children + node->n.num_children) {
            return;
        }
        current++;
    }

    ARTNodeIterator_48::ARTNodeIterator_48(ARTNode_48 *node) : node(node), bitmap(node->unique_bitmap) {
        cur_index = bitmap.consume();
    }

    bool ARTNodeIterator_48::is_valid() const {
        return cur_index != std::numeric_limits<uint64_t>::max();
    }

    ARTNode *ARTNodeIterator_48::operator*() {
        if (!is_valid()) {
            return nullptr;
        }
        return node->children[node->keys[cur_index] - 1];
    }

    std::pair<uint8_t, ARTNode *> ARTNodeIterator_48::get() {
        if (!is_valid()) {
            return {0, nullptr};
        }
        return {cur_index, node->children[node->keys[cur_index] - 1]};
    }

    ARTNode* ARTNodeIterator_48::get_node() {
        return (ARTNode*)node;
    }

    void ARTNodeIterator_48::operator++() {
        if (!is_valid()) {
            return;
        }
        cur_index = bitmap.consume(cur_index);
    }

    void ARTNodeIterator_48::operator++(int step) {
        for (int i = 0; i < step; i++) {
            (*this)++;
        }
    }

    void ARTNodeIterator_48::next_without_skip() {
        if (!is_valid()) {
            return;
        }

        cur_index++;
        while (cur_index < 256 && !node->keys[cur_index]) {
            cur_index++;
        }
        uint64_t next_child = bitmap.find_first();
        if(cur_index >= next_child) {
            bitmap.reset(next_child);
        }
        if(cur_index == 256) {
            cur_index = std::numeric_limits<uint64_t>::max();
        }
    }

    ARTNodeIterator_256::ARTNodeIterator_256(ARTNode_256 *node) : node(node), bitmap(node->unique_bitmap) {
        cur_index = bitmap.consume();
    }

    bool ARTNodeIterator_256::is_valid() const {
        return cur_index != std::numeric_limits<uint64_t>::max();
    }

    ARTNode *ARTNodeIterator_256::operator*() {
        if (!is_valid()) {
            return nullptr;
        }
        return node->children[cur_index];
    }

    std::pair<uint8_t, ARTNode *> ARTNodeIterator_256::get() {
        if (!is_valid()) {
            return {0, nullptr};
        }
        return {cur_index, node->children[cur_index]};
    }

    ARTNode* ARTNodeIterator_256::get_node() {
        return (ARTNode*)node;
    }

    void ARTNodeIterator_256::operator++() {
        if (!is_valid()) {
            return;
        }
        cur_index = bitmap.consume(cur_index);
    }

    void ARTNodeIterator_256::operator++(int step) {
        for (int i = 0; i < step; i++) {
            (*this)++;
        }
    }

    void ARTNodeIterator_256::next_without_skip() {
        if (!is_valid()) {
            return;
        }

        cur_index++;
        while (cur_index < 256 && !node->children[cur_index]) {
            cur_index++;
        }
        uint64_t next_child = bitmap.find_first();
        if(cur_index >= next_child) {
            bitmap.reset(next_child);
        }
        if(cur_index == 256) {
            cur_index = std::numeric_limits<uint64_t>::max();
        }
    }

    ARTNodeIterator* alloc_iterator(const ARTNode* node) {
        switch (node->type) {
            case NODE4: {
                return new ARTNodeIterator_4((ARTNode_4*) node);
            }
            case NODE16: {
                return new ARTNodeIterator_16((ARTNode_16*) node);
            }
            case NODE48: {
                return new ARTNodeIterator_48((ARTNode_48*) node);
            }
            case NODE256: {
                return new ARTNodeIterator_256((ARTNode_256*) node);
            }
            default: {
                throw std::runtime_error("alloc_iterator()::Unknown node type");
            }
        }
    }

    void alloc_iterator_ref(const ARTNode* node, std::variant<ARTNodeIterator_4, ARTNodeIterator_16, ARTNodeIterator_48, ARTNodeIterator_256>& iter) {
        switch (node->type) {
            case NODE4: {
                iter = ARTNodeIterator_4((ARTNode_4*) node);
                break;
            }
            case NODE16: {
                iter = ARTNodeIterator_16((ARTNode_16*) node);
                break;
            }
            case NODE48: {
                iter = ARTNodeIterator_48((ARTNode_48*) node);
                break;
            }
            case NODE256: {
                iter = ARTNodeIterator_256((ARTNode_256*) node);
                break;
            }
            default: {
                throw std::runtime_error("alloc_iterator_ref()::Unknown node type");
            }
        }
    }

    void destroy_iterator(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                delete (ARTNodeIterator_4*) iter;
                break;
            }
            case NODE16: {
                delete (ARTNodeIterator_16*) iter;
                break;
            }
            case NODE48: {
                delete (ARTNodeIterator_48*) iter;
                break;
            }
            case NODE256: {
                delete (ARTNodeIterator_256*) iter;
                break;
            }
            default: {
                throw std::runtime_error("destroy_iterator()::Unknown node type");
            }
        }
    }


    bool iter_is_valid(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                return ((ARTNodeIterator_4*) iter)->is_valid();
            }
            case NODE16: {
                return ((ARTNodeIterator_16*) iter)->is_valid();
            }
            case NODE48: {
                return ((ARTNodeIterator_48*) iter)->is_valid();
            }
            case NODE256: {
                return ((ARTNodeIterator_256*) iter)->is_valid();
            }
            default: {
                throw std::runtime_error("iter_is_valid()::Unknown node type");
            }
        }
    }

    void iter_next(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                ++(*(ARTNodeIterator_4*) iter);
                break;
            }
            case NODE16: {
                ++(*(ARTNodeIterator_16*) iter);
                break;
            }
            case NODE48: {
                ++(*(ARTNodeIterator_48*) iter);
                break;
            }
            case NODE256: {
                ++(*(ARTNodeIterator_256*) iter);
                break;
            }
            default: {
                throw std::runtime_error("iter_next()::Unknown node type");
            }
        }
    }

    // TODO need to extract2art from the iterator to a new type of iterator
    void iter_next_without_skip(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                ((ARTNodeIterator_4*) iter)->next_without_skip();
                break;
            }
            case NODE16: {
                ((ARTNodeIterator_16*) iter)->next_without_skip();
                break;
            }
            case NODE48: {
                ((ARTNodeIterator_48*) iter)->next_without_skip();
                break;
            }
            case NODE256: {
                ((ARTNodeIterator_256*) iter)->next_without_skip();
                break;
            }
            default: {
                throw std::runtime_error("iter_next_without_skip()::Unknown node type");
            }
        }
    }

    std::pair<uint8_t, ARTNode*> iter_get(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                return ((ARTNodeIterator_4*) iter)->get();
            }
            case NODE16: {
                return ((ARTNodeIterator_16*) iter)->get();
            }
            case NODE48: {
                return ((ARTNodeIterator_48*) iter)->get();
            }
            case NODE256: {
                return ((ARTNodeIterator_256*) iter)->get();
            }
            default: {
                throw std::runtime_error("iter_get()::Unknown node type");
            }
        }
    }

    ARTNode** iter_get_node(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                return (ARTNode**)(&((ARTNodeIterator_4*) iter)->node);
            }
            case NODE16: {
                return (ARTNode**)(&((ARTNodeIterator_16*) iter)->node);
            }
            case NODE48: {
                return (ARTNode**)(&((ARTNodeIterator_48*) iter)->node);
            }
            case NODE256: {
                return (ARTNode**)(&((ARTNodeIterator_256*) iter)->node);
            }
            default: {
                throw std::runtime_error("iter_get_node()::Unknown node type");
            }
        }
    }

    ARTNode** iter_get_current(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                return ((ARTNodeIterator_4*) iter)->current;
            }
            case NODE16: {
                return ((ARTNodeIterator_16*) iter)->current;
            }
            case NODE48: {
                return &((ARTNode_48*)(((ARTNodeIterator_48*) iter)->node))->children[((ARTNode_48*)(((ARTNodeIterator_48*) iter)->node))->keys[((ARTNodeIterator_48*) iter)->cur_index] - 1];
            }
            case NODE256: {
                return &((ARTNode_256*)(((ARTNodeIterator_256*) iter)->node))->children[((ARTNodeIterator_256*) iter)->cur_index];
            }
            default: {
                throw std::runtime_error("iter_get_node()::Unknown node type");
            }
        }
    }

    ARTNode* iter_get_current_ro(ARTNodeIterator* iter) {
        switch (((ARTNodeIterator_4*) iter)->node->n.type) {
            case NODE4: {
                return *((ARTNodeIterator_4*) iter)->current;
            }
            case NODE16: {
                return *((ARTNodeIterator_16*) iter)->current;
            }
            case NODE48: {
                return ((ARTNode_48*)(((ARTNodeIterator_48*) iter)->node))->children[((ARTNode_48*)(((ARTNodeIterator_48*) iter)->node))->keys[((ARTNodeIterator_48*) iter)->cur_index] - 1];
            }
            case NODE256: {
                return ((ARTNode_256*)(((ARTNodeIterator_256*) iter)->node))->children[((ARTNodeIterator_256*) iter)->cur_index];
            }
            default: {
                throw std::runtime_error("iter_get_node()::Unknown node type");
            }
        }
    }
}