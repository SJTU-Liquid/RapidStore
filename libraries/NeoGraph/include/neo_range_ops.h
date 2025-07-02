#pragma once

#include <iostream>
#include <algorithm>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <chrono>
#include <cstring>
#include <immintrin.h>
#include <atomic>
#include <tbb/parallel_sort.h>

#include "../utils/config.h"
#include "../utils/types.h"
#include "../utils/c_art/include/art.h"
//#include "../utils/art_new/include/art.h"


namespace container {
    ///@brief find the position of the value in the range segment, return RANGE_LEAF_SIZE if not found
    uint16_t range_segment_find(RangeElement *seg, uint16_t count, RangeElement value);

    void range_segment_set(RangeElementSegment_t *seg, uint16_t pos, RangeElement value);

    void range_segment_set(RangeElementSegment_t *seg, void* prop_seg, uint16_t pos, RangeElement value, Property_t *prop_value);

    void range_segment_insert(RangeElementSegment_t *seg, void* prop_seg, uint16_t seg_size,
                              uint16_t pos, RangeElement value, Property_t *prop_value);

    void range_segment_append(RangeElementSegment_t *seg, void* prop_seg, uint16_t seg_size,
                              RangeElement value, Property_t *prop_value);

    void range_segment_insert_copy(RangeElementSegment_t *old_seg, void* old_prop_seg, uint16_t old_seg_size,
                                   RangeElementSegment_t *new_seg, void* new_prop_seg, uint16_t pos,
                                   RangeElement value, Property_t *prop_value);

    void range_segment_remove(RangeElementSegment_t *old_seg, void* old_prop_seg, uint16_t old_seg_size,
                              RangeElementSegment_t *new_seg, void* new_prop_seg, uint16_t pos);

    void range_segment_split(RangeElementSegment_t *old_seg, void* old_prop_seg, uint16_t old_seg_size,
                             RangeElementSegment_t *new_seg_left, void* new_l_prop_seg,
                             RangeElementSegment_t *new_seg_right, void* new_r_prop_seg,
                             uint16_t split_pos);
}