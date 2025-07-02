#pragma once
#include <cmath>
#include <variant>
#include "art_node_iter.h"

namespace container {
    class ARTIterator {
    private:
        ARTLeaf* leaf; // current leaf
        uint8_t path_len; // current node depth
        std::variant<ARTNodeIterator_4, ARTNodeIterator_16, ARTNodeIterator_48, ARTNodeIterator_256> path[5];   // max node depth
    public:
        explicit ARTIterator(ARTNode* root);

        ///@brief old_leaf: 0x2233445566, depth_step(2), new_leaf: 0x2233555566
        ///@return true if success, false if failed
        [[nodiscard]] bool depth_step(uint8_t depth);

        [[nodiscard]] ARTLeaf* target_step(uint64_t target);

        [[nodiscard]] bool path_step(uint8_t path_idx);

        void operator++();

        [[nodiscard]] bool is_valid() const;

        [[nodiscard]] ARTLeaf* get() const;
    };
}