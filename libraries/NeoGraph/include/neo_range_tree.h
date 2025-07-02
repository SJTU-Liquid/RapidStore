#pragma once

#include "neo_range_ops.h"
#include "../utils/types.h"
#include "../utils/config.h"

namespace container {
    class RangeTree {
    public:
        std::atomic<uint32_t> ref_cnt{1};

//        std::array<InRangeNode, 32> node_block;
//        std::array<uint64_t, 32> keys;
        std::vector<InRangeNode> node_block;
        std::vector<uint32_t> keys;

        RangeTree();

        RangeTree(RangeElement* elements, Property_t** properties, uint64_t element_num, WriterTraceBlock* trace_block);

        RangeTree(std::vector<RangeElement>& elements, Property_t** properties, uint64_t element_num, WriterTraceBlock* trace_block);

        RangeTree(RangeElement* elements, Property_t* properties, uint64_t element_num, uint64_t new_element, Property_t* property, uint64_t pos, WriterTraceBlock* trace_block);

        [[nodiscard]] bool has_element(uint64_t element) const;

        void range_intersect(RangeElement* range, uint16_t range_size, std::vector<uint64_t>& result) const;

        void intersect(RangeTree* other_tree, std::vector<uint64_t>& result) const;

        uint64_t range_intersect(RangeElement* range, uint16_t range_size) const;

        uint64_t intersect(RangeTree* other_tree) const;

        bool insert(uint64_t src, uint64_t element, Property_t* property, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block);

        RangeTreeInsertElemBatchRes insert_element_batch(uint64_t src, const std::pair<RangeElement, RangeElement> *edges, Property_t** properties, uint64_t count, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block);

        bool remove(uint64_t element, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block);

#if EDGE_PROPERTY_NUM != 0
        [[nodiscard]] Property_t get_property(uint64_t element, uint8_t property_id) const;

        void set_property(uint64_t element, uint8_t property_id, Property_t property, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block);

        void set_property_sa(uint64_t element, Property_t* property, std::vector<GCResourceInfo>& gc_resources);
#endif

        ART* range_tree2art(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block);

        RangeTreeInsertElemBatchRes range_tree2art_batch(uint64_t src, uint64_t degree, const std::pair<RangeElement, RangeElement> *edges, Property_t** properties, uint64_t count, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block);


        template<typename F>
        void for_each(F&& callback);

    private:
        [[nodiscard]] uint8_t find_node(uint64_t element) const;
    };

    RangeTree* copy_range_tree(RangeTree* tree);

    template<typename F>
    void RangeTree::for_each(F&& callback) {
        for(uint8_t idx = 0; idx < node_block.size(); idx++) {
            uint16_t arr_size = node_block[idx].size;
            auto arr =  (RangeElementSegment_t*)node_block[idx].arr_ptr;
            for(uint16_t inner_idx = 0; inner_idx < arr_size; inner_idx ++) {
                callback(arr->value.at(inner_idx), 0.0);
            }
        }
    }
}
