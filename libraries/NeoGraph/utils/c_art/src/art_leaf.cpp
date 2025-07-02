#include "include/helper.h"
#include "include/art_node.h"
#include "include/art_leaf.h"

namespace container {
    ARTLeaf::ARTLeaf(ARTKey key, uint8_t depth, bool is_single_byte) :
    key(ARTKey{key, depth, is_single_byte}), depth(depth), size(0), is_single_byte(is_single_byte)
#if EDGE_PROPERTY_NUM != 0
     , property_map(nullptr)
#endif
    {}

#if EDGE_PROPERTY_NUM != 0
    Property_t ARTLeaf::get_property(uint16_t pos_idx, uint8_t property_id) const {
        assert(pos_idx < ART_LEAF_SIZE);
        return map_get_art_property((void*) property_map, pos_idx, property_id);
    }

    void ARTLeaf::set_property(uint16_t pos_idx, uint8_t property_id, Property_t property) {
        map_set_art_property((void*) property_map, pos_idx, property_id, property);
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    void ARTLeaf::get_properties(uint16_t pos_idx, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
        for(unsigned char property_id : *property_ids) {
            res.push_back(map_get_art_property((void*) property_map, pos_idx, property_id));
        }
    }

    void ARTLeaf::set_properties(uint16_t pos_idx, std::vector<uint8_t>* property_ids, Property_t* properties) {
        for(int i = 0; i < property_ids->size(); i++) {
            map_set_art_property((void*) property_map, pos_idx, property_ids->at(i), properties[i]);
        }
    }
#endif

//    void ARTLeaf::leaf_check() const {
//        int cnt = 0;
//        if(size > 0 && at(0) == 0) cnt += 1;
//        for(int i = 0; i < ART_LEAF_SIZE; i++) {
//            if(at(i)) cnt += 1;
//        }
//        assert(cnt == size);
//    }

    void ARTLeaf8::leaf_check() const {
//        int cnt = 0;
//        for(int i = 0; i < 256; i++) {
//            if(value.get(i)) cnt += 1;
//        }
//        assert(cnt == size);
    };

    void ARTLeaf16::leaf_check() const {
//        int cnt = 0;
//        if(size > 0 && value->at(0) == 0) cnt += 1;
//        for(int i = 0; i < ART_LEAF_SIZE; i++) {
//            if(value->at(i)) cnt += 1;
//        }
//        assert(cnt == size);
    }

    void ARTLeaf32::leaf_check() const {
//        int cnt = 0;
//        if(size > 0 && value->at(0) == 0) cnt += 1;
//        for(int i = 0; i < ART_LEAF_SIZE; i++) {
//            if(value->at(i)) cnt += 1;
//        }
//        assert(cnt == size);
    }

    void ARTLeaf64::leaf_check() const {
        int cnt = 0;
        if(size > 0 && value->at(0) == 0) cnt += 1;
        for(int i = 0; i < ART_LEAF_SIZE; i++) {
            if(value->at(i)) cnt += 1;
        }
        assert(cnt == size);
    }

    ARTLeaf8::ARTLeaf8(ARTKey key, uint8_t depth, bool is_single_byte): ARTLeaf(key, depth, is_single_byte), value() {
    }

    ARTLeaf16::ARTLeaf16(ARTKey key, uint8_t depth, bool is_single_byte) : ARTLeaf(key, depth, is_single_byte), value() {
    }

    ARTLeaf32::ARTLeaf32(ARTKey key, uint8_t depth, bool is_single_byte) : ARTLeaf(key, depth, is_single_byte), value() {
    }

    ARTLeaf64::ARTLeaf64(ARTKey key, uint8_t depth, bool is_single_byte) : ARTLeaf(key, depth, is_single_byte), value() {
    }

    uint64_t ARTLeaf8::at(uint16_t pos_idx) const {
        assert(pos_idx < ART_LEAF_SIZE);
        return value.at(pos_idx) | key.key;
    }

    uint64_t ARTLeaf16::at(uint16_t pos_idx) const {
        assert(pos_idx < ART_LEAF_SIZE);
        return value->at(pos_idx) | key.key;
    }

    uint64_t ARTLeaf32::at(uint16_t pos_idx) const {
        assert(pos_idx < ART_LEAF_SIZE);
        return value->at(pos_idx) | key.key;
    }

    uint64_t ARTLeaf64::at(uint16_t pos_idx) const {
        assert(pos_idx < ART_LEAF_SIZE);
        return value->at(pos_idx) | key.key;
    }

    bool  ARTLeaf8::has_element(uint64_t element, uint8_t begin_idx) const {
        uint8_t target = element & 0xFF;
        return value.get(target);
    }

    bool  ARTLeaf16::has_element(uint64_t element, uint8_t begin_idx) const {
        uint16_t target = element & 0xFFFF;
        auto iter = std::lower_bound(value->begin() + begin_idx, value->begin() + size, target) - value->begin();
        return iter != size && value->at(iter) == target;
    }

    bool  ARTLeaf32::has_element(uint64_t element, uint8_t begin_idx) const {
        uint32_t target = element & 0xFFFFFFFF;
        auto iter = std::lower_bound(value->begin() + begin_idx, value->begin() + size, target) - value->begin();
        return iter != size && value->at(iter) == target;
    }

    bool  ARTLeaf64::has_element(uint64_t element, uint8_t begin_idx) const {
        auto iter = std::lower_bound(value->begin() + begin_idx, value->begin() + size, element) - value->begin();
        return iter != size && value->at(iter) == element;
    }

    uint16_t ARTLeaf::find(uint64_t element, uint8_t begin_idx) const {
        uint16_t l = begin_idx, r = size;
        while (l < r) {
            uint16_t mid = l + (r - l) / 2;
            assert(mid < size);
            uint64_t cur = at(mid);
            if (cur == element) {
                return mid;
            } else if (cur < element) {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        assert(l == 0 || (at(l - 1) < element && (l == size || at(l) > element)));
        return l;
    }

    uint16_t ARTLeaf8::find(uint64_t element, uint8_t begin_idx) const {
        if((element & ~0xFF) > key.key) {
            return size;
        }
        return this->value.lower_bound(element, key.key);
    }

    uint16_t ARTLeaf8::get_byte_num(uint8_t depth) const {
        assert(depth <= 4);
        return 1;
    }

    uint16_t ARTLeaf16::get_byte_num(uint8_t depth) const {
        assert(depth <= 4);

        uint16_t cur_diff_byte_num = 1;
        uint8_t cur_byte = get_key_byte(value->at(0), depth);

        for(uint64_t i = 1; i < size; i++) {
            if(get_key_byte(value->at(i), depth) != cur_byte) {
                cur_byte = get_key_byte(value->at(i), depth);
                cur_diff_byte_num++;
            }
        }

        return cur_diff_byte_num;
    }

    uint16_t ARTLeaf32::get_byte_num(uint8_t depth) const {
        assert(depth <= 4);

        uint16_t cur_diff_byte_num = 1;
        uint8_t cur_byte = get_key_byte(value->at(0), depth);

        for(uint64_t i = 1; i < size; i++) {
            if(get_key_byte(value->at(i), depth) != cur_byte) {
                cur_byte = get_key_byte(value->at(i), depth);
                cur_diff_byte_num++;
            }
        }

        return cur_diff_byte_num;
    }

    uint16_t ARTLeaf64::get_byte_num(uint8_t depth) const {
        assert(depth <= 4);

        uint16_t cur_diff_byte_num = 1;
        uint8_t cur_byte = get_key_byte(value->at(0), depth);

        for(uint64_t i = 1; i < size; i++) {
            if(get_key_byte(value->at(i), depth) != cur_byte) {
                cur_byte = get_key_byte(value->at(i), depth);
                cur_diff_byte_num++;
            }
        }

        return cur_diff_byte_num;
    }

    void ARTLeaf8::insert(uint64_t element, Property_t* property, uint16_t pos_idx) {
        assert(pos_idx <= ART_LEAF_SIZE);
        uint8_t target = element & 0xFF;
        value.set(target);
#if EDGE_PROPERTY_NUM != 0
        map_insert_art_property((void*) property_map, pos_idx, size, property);
#endif
        size += 1;
    }

    void ARTLeaf16::insert(uint64_t element, Property_t* property, uint16_t pos_idx) {
        assert(pos_idx <= ART_LEAF_SIZE);
#ifndef NDEBUG
        leaf_check();
#endif
        uint16_t target = element & 0xFFFF;
        std::copy_backward(value->begin() + pos_idx, value->begin() + size, value->begin() + size + 1);
        value->at(pos_idx) = target;
#if EDGE_PROPERTY_NUM != 0
        map_insert_art_property((void*) property_map, pos_idx, size, property);
#endif
        size += 1;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    void ARTLeaf32::insert(uint64_t element, Property_t* property, uint16_t pos_idx) {
        assert(pos_idx <= ART_LEAF_SIZE);

#ifndef NDEBUG
        leaf_check();
#endif
        uint32_t target = element & 0xFFFFFFFF;
        std::copy_backward(value->begin() + pos_idx, value->begin() + size, value->begin() + size + 1);
        value->at(pos_idx) = target;
#if EDGE_PROPERTY_NUM != 0
        map_insert_art_property((void*) property_map, pos_idx, size, property);
#endif
        size += 1;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    void ARTLeaf64::insert(uint64_t element, Property_t* property, uint16_t pos_idx) {
        assert(pos_idx <= ART_LEAF_SIZE);

#ifndef NDEBUG
        leaf_check();
#endif
        std::copy_backward(value->begin() + pos_idx, value->begin() + size, value->begin() + size + 1);
        value->at(pos_idx) = element;
#if EDGE_PROPERTY_NUM != 0
        map_insert_art_property((void*) property_map, pos_idx, size, property);
#endif
        size += 1;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    void ARTLeaf8::remove(uint16_t pos_idx, uint8_t target_byte) {
        assert(pos_idx < ART_LEAF_SIZE);
        value.reset(target_byte);
#if EDGE_PROPERTY_NUM != 0
        map_remove_art_property((void*) property_map, pos_idx, size);
#endif
        size -= 1;
    }

    void ARTLeaf16::remove(uint16_t pos_idx, uint8_t target_byte) {
        assert(pos_idx < ART_LEAF_SIZE);
        std::copy(value->begin() + pos_idx + 1, value->begin() + size, value->begin() + pos_idx);
#if EDGE_PROPERTY_NUM != 0
        map_remove_art_property((void*) property_map, pos_idx, size);
#endif
        size -= 1;
#ifndef NDEBUG
//        value->at(size) = 0;
//        leaf_check();
#endif
    }

    void ARTLeaf32::remove(uint16_t pos_idx, uint8_t target_byte) {
        assert(pos_idx < ART_LEAF_SIZE);
        std::copy(value->begin() + pos_idx + 1, value->begin() + size, value->begin() + pos_idx);
#if EDGE_PROPERTY_NUM != 0
        map_remove_art_property((void*) property_map, pos_idx, size);
#endif
        size -= 1;
#ifndef NDEBUG
//        value->at(size) = 0;
//        leaf_check();
#endif
    }

    void ARTLeaf64::remove(uint16_t pos_idx, uint8_t target_byte) {
        assert(pos_idx < ART_LEAF_SIZE);
        std::copy(value->begin() + pos_idx + 1, value->begin() + size, value->begin() + pos_idx);
#if EDGE_PROPERTY_NUM != 0
        map_remove_art_property((void*) property_map, pos_idx, size);
#endif
        size -= 1;
#ifndef NDEBUG
//        value->at(size) = 0;
//        leaf_check();
#endif
    }

    void ARTLeaf8::copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const {
#ifndef NDEBUG
        leaf_check();
#endif
        auto cur_dst_idx = dst_idx;
        switch (dst->depth + dst->is_single_byte) {
            case 0:
            case 1: {
                auto dstLeaf = static_cast<ARTLeaf32*>(dst);
                value.for_each([&](uint8_t idx) {
                    dstLeaf->value->at(cur_dst_idx++) = (idx | key.key) & 0xFFFFFFFF;
                }, begin_idx, end_idx);
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            case 2: {
                auto dstLeaf = static_cast<ARTLeaf16*>(dst);
                value.for_each([&](uint8_t idx) {
                    dstLeaf->value->at(cur_dst_idx++) = (idx | key.key) & 0xFFFF;
                }, begin_idx, end_idx);
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            case 3: {
                auto dstLeaf = static_cast<ARTLeaf8*>(dst);
                if(begin_idx == 0 && end_idx == size) {
                    dstLeaf->value = value;
                } else {
                    value.for_each([&](uint8_t idx) {
                        dstLeaf->value.set(idx);
                    }, begin_idx, end_idx);
                }
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            default:
                assert(false);
                throw std::runtime_error("ARTLeaf::copy_to_leaf(): Invalid depth");
        }
#if EDGE_PROPERTY_NUM != 0
        if(dst->property_map) {
            art_property_map_copy((void *) property_map, begin_idx, end_idx, dst->property_map, dst_idx);
        }
#endif
    }

    void ARTLeaf16::copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const {
#ifndef NDEBUG
        leaf_check();
#endif
        auto cur_dst_idx = dst_idx;
        switch (dst->depth + dst->is_single_byte) {
            case 0:
            case 1: {
                auto dstLeaf = static_cast<ARTLeaf32*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value->at(cur_dst_idx) = (value->at(i) | key.key) & 0xFFFFFFFF;
                }
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            case 2: {
                auto dstLeaf = static_cast<ARTLeaf16*>(dst);
                std::copy(value->begin() + begin_idx, value->begin() + end_idx, dstLeaf->value->begin() + dst_idx);
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            case 3: {
                auto dstLeaf = static_cast<ARTLeaf8*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value.set(value->at(i) & 0xFF);
                }
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            default:
                assert(false);
                throw std::runtime_error("ARTLeaf::copy_to_leaf(): Invalid depth");
        }
#if EDGE_PROPERTY_NUM != 0
        if(dst->property_map) {
            art_property_map_copy((void *) property_map, begin_idx, end_idx, dst->property_map, dst_idx);
        }
#endif
    }

    void ARTLeaf32::copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const {
#ifndef NDEBUG
        leaf_check();
#endif
#if COMPRESSION_ENABLE != 0
        auto cur_dst_idx = dst_idx;
        switch (dst->depth + dst->is_single_byte) {
            case 0:
            case 1: {
                auto dstLeaf = static_cast<ARTLeaf32*>(dst);
                std::copy(value->begin() + begin_idx, value->begin() + end_idx, dstLeaf->value->begin() + dst_idx);
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            case 2: {
                auto dstLeaf = static_cast<ARTLeaf16*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value->at(cur_dst_idx) = value->at(i) & 0xFFFF;
                }
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            case 3: {
                auto dstLeaf = static_cast<ARTLeaf8*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value.set(value->at(i) & 0xFF);
                }
#ifndef NDEBUG
                dstLeaf->leaf_check();
#endif
                break;
            }
            default:
                assert(false);
                throw std::runtime_error("ARTLeaf::copy_to_leaf(): Invalid depth");
        }
#else
        std::copy(value->begin() + begin_idx, value->begin() + end_idx, static_cast<ARTLeaf32*>(dst)->value->begin() + dst_idx);
#endif

#if EDGE_PROPERTY_NUM != 0
        if(dst->property_map) {
            art_property_map_copy((void *) property_map, begin_idx, end_idx, dst->property_map, dst_idx);
        }
#endif
    }

    void ARTLeaf64::copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const {
#ifndef NDEBUG
        leaf_check();
#endif
        auto cur_dst_idx = dst_idx;
#if COMPRESSION_ENABLE != 0
        switch (dst->depth + dst->is_single_byte) {
//            case 0:
//            case 1: {
//                std::copy(value->begin() + begin_idx, value->begin() + end_idx, static_cast<ARTLeaf64*>(dst)->value->begin() + dst_idx);
//                break;
//            }
            case 0:
            case 1: {
                auto dstLeaf = static_cast<ARTLeaf32*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value->at(cur_dst_idx) = value->at(i) & 0xFFFFFFFF;
                }
                break;
            }
            case 2: {
                auto dstLeaf = static_cast<ARTLeaf16*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value->at(cur_dst_idx) = value->at(i) & 0xFFFF;
                }
                break;
            }
            case 3: {
                auto dstLeaf = static_cast<ARTLeaf8*>(dst);
                for (uint16_t i = begin_idx; i < end_idx; i++, cur_dst_idx++) {
                    dstLeaf->value.set(value->at(i) & 0xFF);
                }
                break;
            }
            default:
                assert(false);
                throw std::runtime_error("ARTLeaf::copy_to_leaf(): Invalid depth");
        }
#else
        std::copy(value->begin() + begin_idx, value->begin() + end_idx, static_cast<ARTLeaf64*>(dst)->value->begin() + dst_idx);
#endif

#if EDGE_PROPERTY_NUM != 0
        if(dst->property_map) {
            art_property_map_copy((void *) property_map, begin_idx, end_idx, (void *) dst->property_map, dst_idx);
        }
#endif
    }

    void ARTLeaf8::append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) {
#ifndef NDEBUG
        leaf_check();
#endif
        assert(count + size <= ART_LEAF_SIZE);
        for(int i = 0; i < count; i++) {
            value.set(elem_list[i] & 0xFF);
#if EDGE_PROPERTY_NUM > 1
            map_set_sa_art_property((void*) property_map, size + i, prop_list[i]);
#elif EDGE_PROPERTY_NUM == 1
//            static_cast<ARTPropertyVec_t*>(property_map)->append_from_list(size, (Property_t*) prop_list, count);
#endif
        }
        size += count;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    void ARTLeaf16::append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) {
#ifndef NDEBUG
        leaf_check();
#endif
        assert(count + size <= ART_LEAF_SIZE);
        for(int i = 0; i < count; i++) {
            value->at(size + i) = elem_list[i] & 0xFFFF;
#if EDGE_PROPERTY_NUM > 1
            map_set_sa_art_property((void*) property_map, size + i, prop_list[i]);
#elif EDGE_PROPERTY_NUM == 1
//            static_cast<ARTPropertyVec_t*>(property_map)->append_from_list(size, (Property_t*) prop_list, count);
#endif
        }
        size += count;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    void ARTLeaf32::append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) {
#ifndef NDEBUG
        leaf_check();
#endif
        assert(count + size <= ART_LEAF_SIZE);
        for(int i = 0; i < count; i++) {
            value->at(size + i) = elem_list[i] & 0xFFFFFFFF;
#if EDGE_PROPERTY_NUM > 1
            map_set_sa_art_property((void*) property_map, size + i, prop_list[i]);
#elif EDGE_PROPERTY_NUM == 1
//            static_cast<ARTPropertyVec_t*>(property_map)->append_from_list(size, (Property_t*) prop_list, count);
#endif
        }
        size += count;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    void ARTLeaf64::append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) {
#ifndef NDEBUG
        leaf_check();
#endif
        assert(count + size <= ART_LEAF_SIZE);
        std::copy(elem_list, elem_list + count, value->begin() + size);
        for(int i = 0; i < count; i++) {
#if EDGE_PROPERTY_NUM > 1
            map_set_sa_art_property((void*) property_map, size + i, prop_list[i]);
#elif EDGE_PROPERTY_NUM == 1
//            static_cast<ARTPropertyVec_t*>(property_map)->append_from_list(size, (Property_t*) prop_list, count);
#endif
        }
        size += count;
#ifndef NDEBUG
        leaf_check();
#endif
    }

    ARTLeaf* alloc_leaf(ARTKey key, uint8_t depth, bool is_single_byte, bool not_empty, WriterTraceBlock* trace_block) {
        ARTLeaf *res = nullptr;
#if COMPRESSION_ENABLE != 0
        if(not_empty) {
            switch (depth + is_single_byte) {
                case 0:
                case 1: {
                    res = new ARTLeaf32(key, depth, is_single_byte);
                    ((ARTLeaf32 *) res)->value = trace_block->allocate_art_leaf32();
                    res->type = LEAF32;
                    break;
                }
                case 2: {
                    res = new ARTLeaf16(key, depth, is_single_byte);
                    ((ARTLeaf16 *) res)->value = new std::array<uint16_t, ART_LEAF_SIZE>();
                    memset(((ARTLeaf16 *) res)->value->data(), 0, ART_LEAF_SIZE);
                    res->type = LEAF16;
                    break;
                }
                case 3: {
                    res = new ARTLeaf8(key, depth, is_single_byte);
                    res->type = LEAF8;
                    break;
                }
                default:
                    throw std::runtime_error("alloc_leaf(): Invalid depth");
            }
            assert(res->ref_cnt == 1);
#if EDGE_PROPERTY_NUM != 0
            force_pointer_set(&res->property_map, trace_block->allocate_art_prop_vec());
#endif
        } else {
            switch (depth + is_single_byte) {
                case 0:
                case 1: {
                    res = new ARTLeaf32(key, depth, is_single_byte);
                    res->type = LEAF32;
                    break;
                }
                case 2: {
                    res = new ARTLeaf16(key, depth, is_single_byte);
                    res->type = LEAF16;
                    break;
                }
                case 3: {
                    res = new ARTLeaf8(key, depth, is_single_byte);
                    res->type = LEAF8;
                    break;
                }
                default:
                    throw std::runtime_error("alloc_leaf(): Invalid depth");
            }
        }
#else
        if(not_empty) {
            res = new ARTLeaf32(key, depth, is_single_byte);
            ((ARTLeaf32 *) res)->value = trace_block->allocate_art_leaf32();
            res->type = LEAF32;
            assert(res->ref_cnt == 1);
#if EDGE_PROPERTY_NUM != 0
            force_pointer_set(&res->property_map, trace_block->allocate_art_prop_vec());
#endif
        } else {
            res = new ARTLeaf32(key, depth, is_single_byte);
            res->type = LEAF32;
        }
#endif
        return res;
    }

    void leaf_clean(ARTLeaf* leaf, WriterTraceBlock* trace_block) {
#if COMPRESSION_ENABLE != 0
        switch (leaf->depth + leaf->is_single_byte) {
            case 0:
            case 1: {
                trace_block->deallocate_art_leaf32(((ARTLeaf32*)leaf)->value);
#if EDGE_PROPERTY_NUM != 0
                trace_block->deallocate_art_prop_vec(((ARTLeaf32*)leaf)->property_map);
#endif
                break;
            }
            case 2: {
                delete ((ARTLeaf16*)leaf)->value;
#if EDGE_PROPERTY_NUM != 0
                trace_block->deallocate_art_prop_vec(((ARTLeaf16*)leaf)->property_map);
#endif
                break;
            }
            case 3: {
#if EDGE_PROPERTY_NUM != 0
                trace_block->deallocate_art_prop_vec(((ARTLeaf8*)leaf)->property_map);
#endif
                break;
            }
            default:
                throw std::runtime_error("alloc_leaf(): Invalid depth");
        }
#else
        trace_block->deallocate_art_leaf32(((ARTLeaf32*)leaf)->value);
#if EDGE_PROPERTY_NUM != 0
        trace_block->deallocate_art_prop_vec(((ARTLeaf32*)leaf)->property_map);
#endif
#endif
    }

    void leaf_destroy(ARTLeaf* leaf) {
#if COMPRESSION_ENABLE != 0
        switch (leaf->depth + leaf->is_single_byte) {
            case 0:
            case 1: {
                delete ((ARTLeaf32*)leaf)->value;
#if EDGE_PROPERTY_NUM != 0
                delete ((ARTLeaf32*)leaf)->property_map;
#endif
                break;
            }
            case 2: {
                delete ((ARTLeaf16*)leaf)->value;
#if EDGE_PROPERTY_NUM != 0
                delete ((ARTLeaf16*)leaf)->property_map;
#endif
                break;
            }
            case 3: {
#if EDGE_PROPERTY_NUM != 0
                delete ((ARTLeaf8*)leaf)->property_map;
#endif
                break;
            }
            default:
                throw std::runtime_error("alloc_leaf(): Invalid depth");
        }
#else
        delete ((ARTLeaf32*)leaf)->value;
#if EDGE_PROPERTY_NUM != 0
        delete ((ARTLeaf32*)leaf)->property_map;
#endif
#endif
    }

    uint64_t get_list_byte_num(uint64_t* list, uint64_t size, uint8_t depth) {
        uint16_t cur_diff_byte_num = 1;
        uint8_t cur_byte = get_key_byte(list[0], depth);

        for(uint64_t i = 1; i < size; i++) {
            if(get_key_byte(list[i], depth) != cur_byte) {
                cur_byte = get_key_byte(list[i], depth);
                cur_diff_byte_num++;
            }
        }

        return cur_diff_byte_num;
    }
}