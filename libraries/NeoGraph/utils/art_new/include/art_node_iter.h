#pragma once

#include "art_node.h"

namespace container {
    struct ARTNodeIterator {
        [[nodiscard]] virtual bool is_valid() const = 0;

        virtual ARTNode* operator*() = 0;   // return nullptr if reach the end
        virtual std::pair<uint8_t, ARTNode*> get() = 0;
        virtual ARTNode* get_node() = 0;

        virtual void operator++() = 0;
        virtual void operator++(int) = 0;

        virtual void next_without_skip() = 0;

        virtual ~ARTNodeIterator() = default;
    };

    struct ARTNodeIterator_4: ARTNodeIterator {
        ARTNode_4* node{};
        ARTNode** current{};

        ARTNodeIterator_4() = default;
        explicit ARTNodeIterator_4(ARTNode_4* node);

        ARTNode* operator*() override;

        [[nodiscard]] bool is_valid() const override;

        std::pair<uint8_t, ARTNode*> get() override;

        ARTNode* get_node() override;

        void operator++() override;

        void operator++(int step) override;

        void next_without_skip() override;
    };

    struct ARTNodeIterator_16: ARTNodeIterator {
        ARTNode_16* node{};
        ARTNode** current{};

        ARTNodeIterator_16() = default;
        explicit ARTNodeIterator_16(ARTNode_16* node);

        ARTNode* operator*() override;

        [[nodiscard]] bool is_valid() const override;

        std::pair<uint8_t, ARTNode*> get() override;

        ARTNode* get_node() override;

        void operator++() override;

        void operator++(int step) override;

        void next_without_skip() override;
    };


    struct ARTNodeIterator_48: ARTNodeIterator {
        ARTNode_48* node{};
        Bitmap<4> bitmap{};
        uint64_t cur_index{};

        ARTNodeIterator_48() = default;
        explicit ARTNodeIterator_48(ARTNode_48* node);

        ARTNode* operator*() override;

        [[nodiscard]] bool is_valid() const override;

        std::pair<uint8_t, ARTNode*> get() override;

        ARTNode* get_node() override;

        void operator++() override;

        void operator++(int step) override;

        void next_without_skip() override;
    };

    struct ARTNodeIterator_256: ARTNodeIterator {
        ARTNode_256* node{};
        Bitmap<4> bitmap{};
        uint64_t cur_index{};

        ARTNodeIterator_256() = default;
        explicit ARTNodeIterator_256(ARTNode_256* node);

        ARTNode* operator*() override;

        [[nodiscard]] bool is_valid() const override;

        std::pair<uint8_t, ARTNode*> get() override;

        ARTNode* get_node() override;

        void operator++() override;

        void operator++(int step) override;

        void next_without_skip() override;
    };

    ARTNodeIterator* alloc_iterator(const ARTNode* node);

    void alloc_iterator_ref(const ARTNode* node, std::variant<ARTNodeIterator_4, ARTNodeIterator_16, ARTNodeIterator_48, ARTNodeIterator_256>& iter);

    void destroy_iterator(ARTNodeIterator* iter);

    bool iter_is_valid(ARTNodeIterator* iter);

    void iter_next(ARTNodeIterator* iter);

    ///@brief pointers pointing to the same leaf will not be skipped
    void iter_next_without_skip(ARTNodeIterator* iter);

    std::pair<uint8_t, ARTNode*> iter_get(ARTNodeIterator* iter);

    ARTNode** iter_get_node(ARTNodeIterator* iter);

    ARTNode** iter_get_current(ARTNodeIterator* iter);

    ARTNode* iter_get_current_ro(ARTNodeIterator* iter);
}