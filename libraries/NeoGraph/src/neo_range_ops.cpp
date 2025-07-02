#include "../include/neo_range_ops.h"

namespace container {
    uint16_t range_segment_find(RangeElement *seg, uint16_t count, RangeElement value) {
        if (count < SEQUENTIAL_SCAN_THRESHOLD) {   // sequential search
            for (auto i = 0; i < count; i++) {
                if (seg[i] == value) {
                    return i;
                }
            }
            return RANGE_LEAF_SIZE;
        } else {    // binary search
            auto pos = std::lower_bound(seg, seg + count, value);
            return *pos == value ? pos - seg : RANGE_LEAF_SIZE;
        }
    }

    void range_segment_set(RangeElementSegment_t *seg, uint16_t pos, RangeElement value) {
        seg->value.at(pos) = value;
    }

    void range_segment_set(RangeElementSegment_t *seg, void* prop_seg, uint16_t pos, RangeElement value, Property_t *prop_value) {
        seg->value.at(pos) = value;
#if EDGE_PROPERTY_NUM != 0
        ((RangePropertyVec_t*)prop_seg)->value.at(pos) = (Property_t)prop_value;
#endif
    }

    void range_segment_insert_copy(RangeElementSegment_t *old_seg, void *old_prop_seg, uint16_t old_seg_size,
                                   RangeElementSegment_t *new_seg, void *new_prop_seg, uint16_t pos,
                                   RangeElement value, Property_t *prop_value) {
        std::copy(old_seg->value.begin(), old_seg->value.begin() + pos, new_seg->value.begin());
        new_seg->value.at(pos) = value;
        std::copy(old_seg->value.begin() + pos, old_seg->value.begin() + old_seg_size, new_seg->value.begin() + pos + 1);
#if EDGE_PROPERTY_NUM != 0
        std::copy(((RangePropertyVec_t*)old_prop_seg)->value.begin(), ((RangePropertyVec_t*)old_prop_seg)->value.begin() + pos, ((RangePropertyVec_t*)new_prop_seg)->value.begin());
        ((RangePropertyVec_t*)new_prop_seg)->value.at(pos) = (Property_t)prop_value;
        std::copy(((RangePropertyVec_t*)old_prop_seg)->value.begin() + pos, ((RangePropertyVec_t*)old_prop_seg)->value.begin() + old_seg_size, ((RangePropertyVec_t*)new_prop_seg)->value.begin() + pos + 1);
#endif
    }

    void range_segment_insert(RangeElementSegment_t *seg, void *prop_seg, uint16_t seg_size,
                              uint16_t pos, RangeElement value, Property_t *prop_value) {
        std::copy_backward(seg->value.begin() + pos, seg->value.begin() + seg_size, seg->value.begin() + seg_size + 1);
        seg->value.at(pos) = value;
#if EDGE_PROPERTY_NUM != 0
        map_insert_range_property(prop_seg, pos, seg_size, prop_value);
#endif
    }

    void range_segment_append(RangeElementSegment_t *seg, void* prop_seg, uint16_t seg_size,
                              RangeElement value, Property_t *prop_value) {
        assert(seg_size < RANGE_LEAF_SIZE);
        seg->value.at(seg_size) = value;
#if EDGE_PROPERTY_NUM != 0
        map_set_sa_range_property(prop_seg, seg_size, prop_value);
#endif
    }

    void range_segment_remove(RangeElementSegment_t *old_seg, void* old_prop_seg, uint16_t old_seg_size,
                              RangeElementSegment_t *new_seg, void* new_prop_seg, uint16_t pos) {
        std::copy(old_seg->value.begin(), old_seg->value.begin() + pos, new_seg->value.begin());
        std::copy(old_seg->value.begin() + pos + 1, old_seg->value.begin() + old_seg_size, new_seg->value.begin() + pos);
#if EDGE_PROPERTY_NUM != 0
        range_property_map_copy(old_prop_seg, new_prop_seg);
        map_remove_range_property(new_prop_seg, pos, old_seg_size);
#endif
    }
    
    void range_segment_split(RangeElementSegment_t *old_seg, void* old_prop_seg, uint16_t old_seg_size,
                             RangeElementSegment_t *new_seg_left, void* new_prop_seg_left,
                             RangeElementSegment_t *new_seg_right, void* new_prop_seg_right,
                             uint16_t split_pos) {
        std::copy(old_seg->value.begin(), old_seg->value.begin() + split_pos, new_seg_left->value.begin());
        std::copy(old_seg->value.begin() + split_pos, old_seg->value.begin() + old_seg_size, new_seg_right->value.begin());
#if EDGE_PROPERTY_NUM == 1
        range_property_map_copy(old_prop_seg, 0, split_pos, new_prop_seg_left, 0);
        range_property_map_copy(old_prop_seg, split_pos, old_seg_size, new_prop_seg_right, 0);
#endif
    }
}