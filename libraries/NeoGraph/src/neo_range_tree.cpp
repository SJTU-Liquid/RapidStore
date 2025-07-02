#include "include/neo_range_tree.h"


namespace container {
    RangeTree::RangeTree() {
    }

    // TODO not used till now
    RangeTree::RangeTree(RangeElement* elements, Property_t** properties, uint64_t element_num, WriterTraceBlock* trace_block) {
        auto arr = trace_block->allocate_range_element_segment();
        for(uint64_t i = 0; i < element_num; i++) {
            arr->value.at(i) = elements[i];
        }
        node_block.at(0) = InRangeNode(element_num, (uint64_t)arr);
        std::fill(keys.begin(), keys.end(), 0);
#if EDGE_PROPERTY_NUM > 0
        if(properties) {
            auto prop_arr = trace_block->allocate_range_prop_vec();
            for(uint64_t i = 0; i < element_num; i++) {
                map_set_sa_range_property(prop_arr, i, properties[i]);
            }
            force_pointer_set(&node_block.at(0).property_map, (void*) prop_arr);
        }
#endif
    }

    RangeTree::RangeTree(std::vector<RangeElement>& elements, Property_t** properties, uint64_t element_num, WriterTraceBlock* trace_block) {
        auto segment_num = (element_num + RANGE_LEAF_SIZE - 1) / RANGE_LEAF_SIZE;
        const uint64_t EXPECTED_SEGMENT_SIZE = (element_num + segment_num - 1) / segment_num;
        assert(EXPECTED_SEGMENT_SIZE >= RANGE_LEAF_SIZE / 3);
#ifndef NDEBUG
        if(EXPECTED_SEGMENT_SIZE < RANGE_LEAF_SIZE / 3 || EXPECTED_SEGMENT_SIZE > RANGE_LEAF_SIZE) {
            std::cerr << "Error: unexpected segment size " << EXPECTED_SEGMENT_SIZE << std::endl;   // DEBUG
            assert(false);
        }
#endif
        // set the size of keys and node_block
        keys.resize(segment_num);
        std::fill(keys.begin(), keys.end(), 0);
        node_block.resize(segment_num);
        for(uint64_t i = 0; i < segment_num; i++) {
            auto arr = trace_block->allocate_range_element_segment();
            uint64_t start = i * EXPECTED_SEGMENT_SIZE;
            uint64_t end = std::min((i + 1) * EXPECTED_SEGMENT_SIZE, element_num);
            std::copy(elements.begin() + start, elements.begin() + end, arr->value.begin());
            node_block.at(i) = InRangeNode(end - start, (uint64_t)arr);
            auto prop_arr = trace_block->allocate_range_prop_vec();
            for (uint64_t j = start; j < end; j++) {
                map_set_sa_range_property(prop_arr, j - start, properties[j]);
            }
            force_pointer_set(&node_block.at(i).property_map, (void *) prop_arr);
            keys.at(i) = arr->value.at(0);
        }
#ifndef NDEBUG
        for(uint64_t i = 0; i < node_block.size(); i++) {
//            assert(node_block.at(i).size >= RANGE_LEAF_SIZE / 2 - 20);
        }
#ifndef NDEBUG
        // check the key order
        for(uint8_t i = 0; i < node_block.size() - 1; i++) {
            assert(keys[i] <= keys[i + 1]);
            for(uint64_t j = 0; j < node_block.at(i).size; j++) {
                assert(((RangeElementSegment_t*)node_block.at(i).arr_ptr)->value.at(j) < keys[i + 1]);
            }
        }
        for(uint8_t i = 0; i < node_block.size(); i++) {
            for(uint64_t j = 0; j < node_block.at(i).size; j++) {
                assert(((RangeElementSegment_t*)node_block.at(i).arr_ptr)->value.at(j) >= keys[i]);
            }
        }
        for(uint64_t i = 0; i < node_block.size(); i++) {
//            assert(node_block.at(i).size >= RANGE_LEAF_SIZE / 2 - 20);
        }
#endif
#endif
        keys.at(0) = 0;
    }

    RangeTree::RangeTree(RangeElement* elements, Property_t* properties, uint64_t element_num, uint64_t new_element, Property_t* property, uint64_t pos, WriterTraceBlock* trace_block) {
        auto arr = (RangeElementSegment_t*)trace_block->allocate_range_element_segment();
        std::fill(arr->value.begin(), arr->value.end(), 0);

        for(uint64_t i = 0; i < element_num; i++) {
            arr->value.at(i) = elements[i];
        }
        std::copy_backward(arr->value.begin() + pos, arr->value.begin() + element_num, arr->value.begin() + element_num + 1);
        arr->value.at(pos) = new_element;

        node_block.emplace_back(element_num + 1, (uint64_t)arr);
#if EDGE_PROPERTY_NUM != 0
        if(properties) {
            auto prop_arr = trace_block->allocate_range_prop_vec();
            for(uint64_t i = 0; i < pos; i++) {
                map_set_sa_range_property(prop_arr, i, (Property_t*)properties[i]);
            }
            map_set_sa_range_property(prop_arr, pos, property);
            for(uint64_t i = pos; i < element_num; i++) {
                map_set_sa_range_property(prop_arr, i + 1, (Property_t*)properties[i]);
            }
            node_block.at(0).property_map = prop_arr;
        }
#endif
        keys.push_back(0);
    }

    uint8_t RangeTree::find_node(uint64_t element) const {
        uint16_t node_idx = 0;
        // find the first node that key is smaller or equal to src but the next node is larger than src
        while (node_idx < node_block.size() && keys.at(node_idx) <= element) {
            node_idx++;
        }
        if(node_idx != 0) {
            node_idx--;
        }
        return node_idx;
    }

    bool RangeTree::has_element(uint64_t element) const {
        auto node = node_block.at(find_node(element));
        auto arr = (RangeElementSegment_t*)node.arr_ptr;
        uint16_t arr_size = node.size;
        auto pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, element);
        return pos != arr->value.begin() + arr_size && *pos == element;
    }

    void RangeTree::range_intersect(RangeElement* range, uint16_t range_size, std::vector<uint64_t>& result) const {
        uint16_t range_idx = 0;
        uint16_t node_idx = 0;
        while(range_idx < range_size && node_idx < node_block.size()) {
            InRangeNode node = node_block.at(node_idx);
            auto arr = (RangeElementSegment_t*)node.arr_ptr;
            uint16_t arr_size = node.size;
            uint16_t arr_idx = 0;
            while(range_idx < range_size && arr_idx < arr_size) {
                if(arr->value.at(arr_idx) < range[range_idx]) {
                    arr_idx++;
                } else if(arr->value.at(arr_idx) > range[range_idx]) {
                    range_idx++;
                } else {
                    result.push_back(arr->value.at(arr_idx));
                    arr_idx++;
                    range_idx++;
                }
            }
            if(arr_idx == arr_size) {
                node_idx++;
            }
        }
    }

    void RangeTree::intersect(RangeTree* other_tree, std::vector<uint64_t>& result) const {
        uint16_t node_idx1 = 0;
        uint16_t node_idx2 = 0;
        while(node_idx1 < node_block.size() && node_idx2 < other_tree->node_block.size()) {
            InRangeNode node1 = node_block.at(node_idx1);
            InRangeNode node2 = other_tree->node_block.at(node_idx2);
            auto arr1 = (RangeElementSegment_t*)node1.arr_ptr;
            auto arr2 = (RangeElementSegment_t*)node2.arr_ptr;
            uint16_t arr_size1 = node1.size;
            uint16_t arr_size2 = node2.size;
            uint16_t arr_idx1 = 0;
            uint16_t arr_idx2 = 0;
            while(arr_idx1 < arr_size1 && arr_idx2 < arr_size2) {
                if(arr1->value.at(arr_idx1) < arr2->value.at(arr_idx2)) {
                    arr_idx1++;
                } else if(arr1->value.at(arr_idx1) > arr2->value.at(arr_idx2)) {
                    arr_idx2++;
                } else {
                    result.push_back(arr1->value.at(arr_idx1));
                    arr_idx1++;
                    arr_idx2++;
                }
            }
            if(arr_idx1 == arr_size1) {
                node_idx1++;
            }
            if(arr_idx2 == arr_size2) {
                node_idx2++;
            }
        }
    }

    uint64_t RangeTree::range_intersect(RangeElement* range, uint16_t range_size) const {
        uint16_t range_idx = 0;
        uint16_t node_idx = 0;
        uint64_t res = 0;
        while(range_idx < range_size && node_idx < node_block.size()) {
            InRangeNode node = node_block.at(node_idx);
            auto arr = (RangeElementSegment_t*)node.arr_ptr;
            uint16_t arr_size = node.size;
            uint16_t arr_idx = 0;
            while(range_idx < range_size && arr_idx < arr_size) {
                if(arr->value.at(arr_idx) < range[range_idx]) {
                    arr_idx++;
                } else if(arr->value.at(arr_idx) > range[range_idx]) {
                    range_idx++;
                } else {
                    res++;
                    arr_idx++;
                    range_idx++;
                }
            }
            if(arr_idx == arr_size) {
                node_idx++;
            }
        }
        return res;
    }

    uint64_t RangeTree::intersect(RangeTree* other_tree) const {
        uint16_t node_idx1 = 0;
        uint16_t node_idx2 = 0;
        uint64_t res = 0;
        while(node_idx1 < node_block.size() && node_idx2 < other_tree->node_block.size()) {
            InRangeNode node1 = node_block.at(node_idx1);
            InRangeNode node2 = other_tree->node_block.at(node_idx2);
            auto arr1 = (RangeElementSegment_t*)node1.arr_ptr;
            auto arr2 = (RangeElementSegment_t*)node2.arr_ptr;
            uint16_t arr_size1 = node1.size;
            uint16_t arr_size2 = node2.size;
            uint16_t arr_idx1 = 0;
            uint16_t arr_idx2 = 0;
            while(arr_idx1 < arr_size1 && arr_idx2 < arr_size2) {
                if(arr1->value.at(arr_idx1) < arr2->value.at(arr_idx2)) {
                    arr_idx1++;
                } else if(arr1->value.at(arr_idx1) > arr2->value.at(arr_idx2)) {
                    arr_idx2++;
                } else {
                    res++;
                    arr_idx1++;
                    arr_idx2++;
                }
            }
            if(arr_idx1 == arr_size1) {
                node_idx1++;
            }
            if(arr_idx2 == arr_size2) {
                node_idx2++;
            }
        }
        return res;
    }

    bool RangeTree::insert(uint64_t src, uint64_t element, Property_t* property, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block) {
        auto node_idx = find_node(element);
        auto& node = node_block.at(node_idx);
        auto arr = (RangeElementSegment_t*)node.arr_ptr;
        uint16_t arr_size = node.size;

        if(arr == nullptr) {
            auto new_arr = (RangeElementSegment_t*)trace_block->allocate_range_element_segment();
            new_arr->value.at(0) = element;
            node.arr_ptr = (uint64_t)new_arr;
            node.size = 1;
            return true;
        }
        uint64_t pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, element) - arr->value.begin();
        if(pos != arr_size && arr->value.at(pos) == element) {
            return false;
        }
        gc_resources.emplace_back(GCResourceInfo{Inner_Segment, (void*)arr});
#if EDGE_PROPERTY_NUM != 0
        if(node.property_map) {
            gc_resources.emplace_back(GCResourceInfo{Range_Property_Map_All_Modified, (void *) node.property_map});
        }
#endif
        if(arr_size < RANGE_LEAF_SIZE) {
            auto new_arr = (RangeElementSegment_t*)trace_block->allocate_range_element_segment();
            std::copy(arr->value.begin(), arr->value.begin() + pos, new_arr->value.begin());
            new_arr->value.at(pos) = element;
            std::copy(arr->value.begin() + pos, arr->value.begin() + arr_size, new_arr->value.begin() + pos + 1);
            node.arr_ptr = (uint64_t)new_arr;

#if EDGE_PROPERTY_NUM != 0
            auto new_property_map = trace_block->allocate_range_prop_vec();
            if(node.property_map) {
                range_property_map_copy(node.property_map, new_property_map);
            }
            map_insert_range_property((void*) new_property_map, pos, arr_size, (void*) property);
            force_pointer_set(&node.property_map, (void*) new_property_map);
#endif
            node.size++;
        } else {
            auto new_l_arr = (RangeElementSegment_t*)trace_block->allocate_range_element_segment();
            auto new_r_arr = (RangeElementSegment_t*)trace_block->allocate_range_element_segment();

            std::copy(arr->value.begin(), arr->value.begin() + RANGE_LEAF_SIZE / 2, new_l_arr->value.begin());
            std::copy(arr->value.begin() + RANGE_LEAF_SIZE / 2, arr->value.begin() + arr_size, new_r_arr->value.begin());

            node.arr_ptr = (uint64_t)new_l_arr;
            node.size = RANGE_LEAF_SIZE / 2;

            auto new_node = InRangeNode(RANGE_LEAF_SIZE / 2, (uint64_t) new_r_arr);
//            std::copy_backward(node_block.begin() + node_idx + 1, node_block.begin() + child_num, node_block.begin() + child_num + 1);
//            node_block.at(node_idx + 1) = new_node;
//            std::copy_backward(keys.begin() + node_idx + 1, keys.begin() + child_num, keys.begin() + child_num + 1);
//            keys.at(node_idx + 1) = new_r_arr->value.at(0);
#if EDGE_PROPERTY_NUM != 0
            auto new_l_property_map = trace_block->allocate_range_prop_vec();
            auto new_r_property_map = trace_block->allocate_range_prop_vec();
            range_property_map_copy(node.property_map, 0, RANGE_LEAF_SIZE / 2, new_l_property_map, 0);
            range_property_map_copy(node.property_map, RANGE_LEAF_SIZE / 2, arr_size, new_r_property_map, 0);
            force_pointer_set(&node.property_map, (void*) new_l_property_map);
            force_pointer_set(&new_node.property_map, (void*) new_r_property_map);
#endif
            node_block.insert(node_block.begin() + node_idx + 1, new_node);
            keys.insert(keys.begin() + node_idx + 1, new_r_arr->value.at(0));

            // re-insert
            if(new_r_arr->value.at(0) <= element) {
                pos -= RANGE_LEAF_SIZE / 2;
                std::copy_backward(new_r_arr->value.begin() + pos, new_r_arr->value.begin() + RANGE_LEAF_SIZE / 2, new_r_arr->value.begin() + RANGE_LEAF_SIZE / 2 + 1);
                new_r_arr->value.at(pos) = element;
#if EDGE_PROPERTY_NUM != 0
                map_insert_range_property((void*) new_r_property_map, pos, RANGE_LEAF_SIZE / 2, (void*) property);
#endif
                node_block.at(node_idx + 1).size++;
            } else  {
                std::copy_backward(new_l_arr->value.begin() + pos, new_l_arr->value.begin() + RANGE_LEAF_SIZE / 2, new_l_arr->value.begin() + RANGE_LEAF_SIZE / 2 + 1);
                new_l_arr->value.at(pos) = element;
#if EDGE_PROPERTY_NUM != 0
                map_insert_range_property((void*) new_l_property_map, pos, RANGE_LEAF_SIZE / 2, (void*) property);
#endif
                node_block.at(node_idx).size++;
            }
        }
        return true;
    }


    RangeTreeInsertElemBatchRes RangeTree::insert_element_batch(uint64_t src, const std::pair<RangeElement, RangeElement> *edges, Property_t** properties, uint64_t count, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block) {
#ifndef NDEBUG
        // check the key order
        for(int i = 0; i < node_block.size() - 1; i++) {
            assert(keys[i] <= keys[i + 1]);
            for(uint64_t j = 0; j < node_block.at(i).size; j++) {
                assert(((RangeElementSegment_t*)node_block.at(i).arr_ptr)->value.at(j) < keys[i + 1]);
            }
        }
        for(int i = 0; i < node_block.size(); i++) {
            for(uint64_t j = 0; j < node_block.at(i).size; j++) {
                assert(((RangeElementSegment_t*)node_block.at(i).arr_ptr)->value.at(j) >= keys[i]);
            }
        }
        if((*edges).first == 934 && (*edges).second == 291921) {
            std::cout << "debug" << std::endl;
        }
#endif
        uint64_t inserted = 0;
        auto new_range_tree = new RangeTree();

        auto child_num = node_block.size();
        int64_t old_node_idx = 0;
        int64_t list_st = 0;
        int64_t list_ed = 0;

        uint64_t new_segment_size = 0;
        auto new_segment = trace_block->allocate_range_element_segment();

        auto new_property_map = trace_block->allocate_range_prop_vec();
        auto move_to_next_node = [&]() {
            new_range_tree->node_block.push_back(InRangeNode{new_segment_size, (uint64_t) new_segment, new_property_map});
            new_range_tree->keys.push_back(new_segment->value.at(0));

            new_segment_size = 0;
            new_segment = trace_block->allocate_range_element_segment();
            new_property_map = trace_block->allocate_range_prop_vec();
        };

        while(old_node_idx < child_num && list_ed < count) {
            auto old_node = node_block.at(old_node_idx);
            auto next_key = old_node_idx != child_num - 1 ? keys[old_node_idx + 1] : std::numeric_limits<uint64_t>::max();
            if(edges[list_st].second >= next_key) {
                new_range_tree->node_block.push_back(old_node);
                new_range_tree->keys.push_back(keys[old_node_idx]);
                old_node_idx += 1;
                continue;
            }

            while (list_ed < count && edges[list_ed].second < next_key) {
                list_ed += 1;
            }
            int64_t list_idx = list_st;
            int64_t leaf_idx = 0;

            auto old_arr = (RangeElementSegment_t*) old_node.arr_ptr;
            gc_resources.emplace_back(GCResourceInfo{Inner_Segment, (void*)old_arr});
//            std::cout << "collect:" << (old_arr) << std::endl;
            auto old_prop_arr = old_node.property_map;
            if(old_prop_arr) {
                gc_resources.emplace_back(GCResourceInfo{Range_Property_Map_All_Modified, (void *) old_prop_arr});
            }

            // calculate expected segment size, this size must ensure all new segments will be big enough
            uint64_t total_size = old_node.size + (list_ed - list_st);
            uint64_t segment_num = (total_size + RANGE_LEAF_SIZE - 1) / RANGE_LEAF_SIZE;
            const uint64_t EXPECTED_SEGMENT_SIZE = (total_size + segment_num - 1) / segment_num;
            assert(EXPECTED_SEGMENT_SIZE >= RANGE_LEAF_SIZE / 4);
#ifndef NDEBUG
            if(EXPECTED_SEGMENT_SIZE < RANGE_LEAF_SIZE / 4 || EXPECTED_SEGMENT_SIZE > RANGE_LEAF_SIZE) {
                std::cerr << "Error: unexpected segment size " << EXPECTED_SEGMENT_SIZE << std::endl;   // DEBUG
                assert(false);
            }
#endif

            while(list_idx < list_ed && leaf_idx < old_node.size) {
                if(edges[list_idx].second < old_arr->value.at(leaf_idx)) {
                    map_set_sa_range_property(new_property_map, new_segment_size, properties[list_idx]);
                    new_segment->value.at(new_segment_size++) = edges[list_idx++].second;
                    inserted++;
                } else if(edges[list_idx].second > old_arr->value.at(leaf_idx)) {
                    if(old_prop_arr) {
                        auto old_property = map_get_all_range_property(old_prop_arr, leaf_idx);
                        map_set_sa_range_property(new_property_map, new_segment_size, old_property);
                    }
                    new_segment->value.at(new_segment_size++) = old_arr->value.at(leaf_idx++);
                } else {
                    map_set_sa_range_property(new_property_map, new_segment_size, properties[list_idx]);
                    new_segment->value.at(new_segment_size++) = edges[list_idx++].second;
                    leaf_idx++;
                }
                if(new_segment_size == EXPECTED_SEGMENT_SIZE) {
                    move_to_next_node();
                }
            }

            while(list_idx < list_ed) {
                map_set_sa_range_property(new_property_map, new_segment_size, properties[list_idx]);
                new_segment->value.at(new_segment_size++) = edges[list_idx++].second;
                inserted++;
                if(new_segment_size == EXPECTED_SEGMENT_SIZE) {
                    move_to_next_node();
                }
            }
            while (leaf_idx < old_node.size) {
                if(old_prop_arr) {
                    auto old_property = map_get_all_range_property(old_prop_arr, leaf_idx);
                    map_set_sa_range_property(new_property_map, new_segment_size, old_property);
                }

                new_segment->value.at(new_segment_size++) = old_arr->value.at(leaf_idx++);
                if(new_segment_size == EXPECTED_SEGMENT_SIZE) {
                    move_to_next_node();
                }
            }

            // mount the last segment
            if(new_segment_size != 0) {
                assert(new_segment_size > RANGE_LEAF_SIZE / 2 - 20);
                move_to_next_node();
            }

            list_st = list_ed;
            old_node_idx += 1;
        }

        if (old_node_idx < child_num) {
            while(old_node_idx < child_num) {
                new_range_tree->node_block.push_back(node_block.at(old_node_idx));
                new_range_tree->keys.push_back(keys.at(old_node_idx));
                old_node_idx += 1;
            }
        }
        else if (list_ed < count) {
            const uint64_t remaining_elements = count - list_ed;
            const uint64_t num_leaves = (remaining_elements + RANGE_LEAF_SIZE - 1) / RANGE_LEAF_SIZE;
            const uint64_t AVG_LEAF_SIZE = (remaining_elements + num_leaves - 1) / num_leaves;
            inserted += (count - list_ed);
            while (list_ed < count) {
                map_set_sa_range_property(new_property_map, new_segment_size, properties[list_ed]);
                new_segment->value.at(new_segment_size++) = edges[list_ed++].second;
                if(new_segment_size == AVG_LEAF_SIZE) {
                    move_to_next_node();
                }
            }
        }

        new_range_tree->keys.at(0) = 0; // The first key is always 0

        // mount the last segment
        if(new_segment_size != 0) {
            assert(new_segment_size > RANGE_LEAF_SIZE / 2 - 20);
            new_range_tree->node_block.push_back(InRangeNode{new_segment_size, (uint64_t) new_segment, new_property_map});
            new_range_tree->keys.push_back(new_segment->value.at(0));
        } else {
            trace_block->deallocate_range_element_segment(new_segment);
            trace_block->deallocate_range_prop_vec(new_property_map);
        }
#ifndef NDEBUG
        // check the key order
        for(int i = 0; i < new_range_tree->node_block.size() - 1; i++) {
            assert(new_range_tree->keys[i] <= new_range_tree->keys[i + 1]);
            for(uint64_t j = 0; j < new_range_tree->node_block.at(i).size; j++) {
                assert(((RangeElementSegment_t*)new_range_tree->node_block.at(i).arr_ptr)->value.at(j) < new_range_tree->keys[i + 1]);
            }
        }
        for(int i = 0; i < new_range_tree->node_block.size(); i++) {
            for(uint64_t j = 0; j < new_range_tree->node_block.at(i).size; j++) {
                assert(((RangeElementSegment_t*)new_range_tree->node_block.at(i).arr_ptr)->value.at(j) >= new_range_tree->keys[i]);
            }
        }
        for(uint64_t i = 0; i < new_range_tree->node_block.size(); i++) {
            assert(new_range_tree->node_block.at(i).size >= RANGE_LEAF_SIZE / 2 - 20);
        }
#endif
        return RangeTreeInsertElemBatchRes{inserted, (void *)new_range_tree};
    }


    bool RangeTree::remove(uint64_t element, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block)  {
        auto node_idx = find_node(element);
        auto& node = node_block.at(node_idx);
        auto arr = (RangeElementSegment_t*)node.arr_ptr;
        uint16_t arr_size = node.size;
//        uint16_t next_arr_size = node_idx != child_num - 1 ? node_block.at(node_idx + 1).size : std::numeric_limits<uint16_t>::max();

#if EDGE_PROPERTY_NUM > 0
        auto prop_arr = node.property_map;
#endif
        uint64_t pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, element) - arr->value.begin();
        if(pos == arr_size || arr->value.at(pos) != element) {
            return false;
        }

        gc_resources.emplace_back(GCResourceInfo{Inner_Segment, (void*)arr});
#if EDGE_PROPERTY_NUM > 0
        if(prop_arr) {
            gc_resources.emplace_back(GCResourceInfo{Range_Property_Vec, (void *) prop_arr});
        }
#endif
        if(arr_size == 1) {
            // remove the whole node
            if(node_block.size() == 1) {
                node_block.at(0) = InRangeNode{0, 0};
                keys.at(0) = 0;
            } else {
                if(node_idx == 0) {
                    keys.at(1) = 0;
                }
                node_block.erase(node_block.begin() + node_idx);
                keys.erase(keys.begin() + node_idx);
            }
        } else {
            // remove the element
            auto new_arr = (RangeElementSegment_t*)trace_block->allocate_range_element_segment();
            std::copy(arr->value.begin(), arr->value.begin() + pos, new_arr->value.begin());
            std::copy(arr->value.begin() + pos + 1, arr->value.begin() + arr_size, new_arr->value.begin() + pos);
#if EDGE_PROPERTY_NUM > 0
            auto new_prop_arr = (RangePropertyVec_t *)trace_block->allocate_range_prop_vec();
            std::copy(prop_arr->value.begin(), prop_arr->value.begin() + pos, new_prop_arr->value.begin());
            std::copy(prop_arr->value.begin() + pos + 1, prop_arr->value.begin() + arr_size, new_prop_arr->value.begin() + pos);
            node.property_map = new_prop_arr;
#endif
            node.arr_ptr = (uint64_t)new_arr;
            node.size--;
        }
        return true;
    }

#if EDGE_PROPERTY_NUM != 0
    Property_t RangeTree::get_property(uint64_t element, uint8_t property_id) const {
        auto node = node_block.at(find_node(element));
        auto arr = (RangeElementSegment_t*)node.arr_ptr;
        uint16_t arr_size = node.size;
        auto pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, element);
        if(pos == arr->value.begin() + arr_size || *pos != element) {
            return Property_t();
        }
        auto pos_idx = pos - arr->value.begin();
        return map_get_range_property(node.property_map, pos_idx, property_id);
    }

    void RangeTree::set_property(uint64_t element, uint8_t property_id, Property_t property, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block)  {
        auto &node = node_block.at(find_node(element));
        auto arr = (RangeElementSegment_t*)node.arr_ptr;
        uint16_t arr_size = node.size;
        auto pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, element);
        if(pos == arr->value.begin() + arr_size || *pos != element) {
            abort();
            return;
        }
        auto pos_idx = pos - arr->value.begin();
        auto old_prop_map = node.property_map;
        RangePropertyVec_t* new_prop_map = nullptr;
        if(old_prop_map) {
            new_prop_map = trace_block->allocate_range_prop_vec();
            std::copy(old_prop_map->value.begin(), old_prop_map->value.begin() + arr_size, new_prop_map->value.begin());
            gc_resources.emplace_back(GCResourceInfo{Range_Property_Vec, (void*) old_prop_map});
        } else {
            new_prop_map = trace_block->allocate_range_prop_vec();
        }
        map_set_range_property(new_prop_map, pos_idx, property_id, property);
        node.property_map = new_prop_map;
    }

    void RangeTree::set_property_sa(uint64_t element, Property_t* property, std::vector<GCResourceInfo>& gc_resources) {
        auto node = node_block.at(find_node(element));
        auto arr = (RangeElementSegment_t*)node.arr_ptr;
        uint16_t arr_size = node.size;
        auto pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, element);
        if(pos == arr->value.begin() + arr_size || *pos != element) {
            return;
        }
        auto pos_idx = pos - arr->value.begin();
        auto old_prop_map = node.property_map;
        void* new_prop_map = nullptr;
        if(old_prop_map) {
//            new_prop_map = alloc_range_property_map_with_vec();
            range_property_map_copy(old_prop_map, new_prop_map);
#if EDGE_PROPERTY_NUM == 1
            gc_resources.emplace_back(GCResourceInfo{Range_Property_Vec, (void*) old_prop_map});
#else
            gc_resources.emplace_back(GCResourceInfo{Range_Property_Map_All_Modified, (void*) old_prop_map});
#endif
        } else {
//            new_prop_map = alloc_range_property_map_with_vec();
        }
        map_set_sa_range_property(new_prop_map, pos_idx, property);
        force_pointer_set(&node.property_map, new_prop_map);
    }
#endif

    ART* RangeTree::range_tree2art(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block) {
        ART* new_art = new ART();
        delete new_art->root;

        std::vector<RangeElement> extract_list;
        extract_list.reserve(degree + 1);

        for (auto i = 0; i < node_block.size(); i++) {
            auto arr = (RangeElementSegment_t*)node_block.at(i).arr_ptr;
            auto arr_size = node_block.at(i).size;
            for (auto j = 0; j < arr_size; j++) {
                extract_list.push_back(arr->value.at(j));
            }
        }
        auto new_element_pos = std::lower_bound(extract_list.begin(), extract_list.end(), new_element);
        if(*new_element_pos == new_element) {
            delete new_art;
            return nullptr;
        }

        extract_list.insert(new_element_pos, new_element);
#if EDGE_PROPERTY_NUM != 0
        std::vector<Property_t*> extract_prop_list;
        extract_prop_list.reserve(degree + 1);

        for (auto i = 0; i < node_block.size(); i++) {
            auto prop_arr = node_block.at(i).property_map;
            auto arr_size = node_block.at(i).size;
            if(prop_arr) {
                for (auto j = 0; j < arr_size; j++) {
                    extract_prop_list.push_back(map_get_all_range_property(prop_arr, j));
                }
            } else {
                for (auto j = 0; j < arr_size; j++) {
                    extract_prop_list.push_back(nullptr);
                }
            }
        }
        extract_prop_list.insert(extract_prop_list.begin() + std::distance(extract_list.begin(), new_element_pos), new_property);
#endif

        // build art
        assert(extract_list.size() == degree + 1);
#if EDGE_PROPERTY_NUM != 0
        batch_subtree_build<false>(&new_art->root, 0, extract_list.data(), extract_prop_list.data(), degree + 1, trace_block);
#if EDGE_PROPERTY_NUM > 1
        for (auto i = 0; i < extract_prop_list.size(); i++) {
            delete [] extract_prop_list[i];
        }
#endif
#else
        batch_subtree_build<false>(&new_art->root, 0, extract_list.data(), nullptr, degree + 1, trace_block);
#endif
        return new_art;
    }


    RangeTreeInsertElemBatchRes RangeTree::range_tree2art_batch(uint64_t src, uint64_t degree, const std::pair<RangeElement, RangeElement> *edges, Property_t** properties, uint64_t count, std::vector<GCResourceInfo>& gc_resources, WriterTraceBlock* trace_block) {
        ART* new_art = new ART();
        delete new_art->root;
        uint64_t inserted = 0;
        std::vector<RangeElement> insert_list;
        insert_list.reserve(count + degree);
        std::vector<Property_t*> insert_prop_list;
        insert_prop_list.reserve(count + degree);

        uint16_t old_node_idx = 0;
        uint64_t old_idx = 0;
        auto old_arr = (RangeElementSegment_t*) node_block.at(old_node_idx).arr_ptr;
        uint64_t list_idx = 0;
        auto child_num = node_block.size();

        auto old_prop_arr = node_block.at(old_node_idx).property_map;
        while(old_node_idx < child_num && list_idx < count) {
            if(old_arr->value.at(old_idx) > edges[list_idx].second) {
                insert_prop_list.push_back(properties[list_idx]);
                insert_list.push_back(edges[list_idx++].second);
                inserted++;
            } else if (old_arr->value.at(old_idx) < edges[list_idx].second) {
                insert_list.push_back(old_arr->value.at(old_idx++));
                insert_prop_list.push_back(old_prop_arr ? map_get_all_range_property(old_prop_arr, old_idx) : nullptr);
            } else {
                insert_prop_list.push_back(properties[list_idx]);
                insert_list.push_back(edges[list_idx++].second);
                old_idx++;
            }

            if(old_idx == node_block.at(old_node_idx).size) {
                old_node_idx++;
                old_idx = 0;
                if(old_node_idx < child_num) {
                    old_arr = (RangeElementSegment_t*)node_block.at(old_node_idx).arr_ptr;
                    old_prop_arr = node_block.at(old_node_idx).property_map;
                }
            }
        }

        if (old_node_idx < child_num) {
            while(old_node_idx < child_num) {
                insert_list.push_back(old_arr->value.at(old_idx++));
                insert_prop_list.push_back(old_prop_arr ? map_get_all_range_property(old_prop_arr, old_idx) : nullptr);

                if(old_idx == node_block.at(old_node_idx).size) {
                    old_node_idx++;
                    old_idx = 0;
                    if(old_node_idx < child_num) {
                        old_arr = (RangeElementSegment_t *) node_block.at(old_node_idx).arr_ptr;
                        old_prop_arr = node_block.at(old_node_idx).property_map;
                    }
                }
            }
        } else if (list_idx < count) {
            inserted += (count - list_idx);
            while (list_idx < count) {
                insert_prop_list.push_back(properties[list_idx]);
                insert_list.push_back(edges[list_idx++].second);
            }
        }

        // build art
        batch_subtree_build<false>(&new_art->root, 0, insert_list.data(), insert_prop_list.data(), insert_list.size(), trace_block);
        return {inserted, new_art};
    }

    RangeTree* copy_range_tree(RangeTree* tree) {
        // just copy the tree structure, not the data
        auto new_tree = new RangeTree();
        new_tree->node_block.resize(tree->node_block.size());
        std::copy(tree->node_block.begin(), tree->node_block.end(), new_tree->node_block.begin());
        new_tree->keys.resize(tree->keys.size());
        std::copy(tree->keys.begin(), tree->keys.end(), new_tree->keys.begin());

        return new_tree;
    }
}