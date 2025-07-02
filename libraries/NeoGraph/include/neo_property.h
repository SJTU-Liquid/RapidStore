#pragma once
#include <cstdint>
#include <memory>
#include <limits>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <immintrin.h>
#include <cassert>
#include <array>
#include <atomic>
#include "utils/config.h"

namespace container {
    using Property_t = uint64_t;

// ---------------------- Vertex Property ----------------------
    template<uint64_t Size>
    struct PropertyVec {
        std::array<Property_t, Size> value{};
        std::atomic<uint32_t> ref_cnt{1};

        explicit PropertyVec() = default;

        void copy_to(PropertyVec *dst) const;

        void copy_to(uint64_t begin_idx, uint64_t end_idx, PropertyVec *dst, uint64_t dst_idx) const;

        [[nodiscard]] Property_t get(uint64_t idx) const;

        void set(uint64_t idx, Property_t value);

        void set_string(uint64_t idx, std::string&& value);

        void insert(uint64_t pos_idx, uint64_t size, Property_t value);

        void insert_copy(PropertyVec* target, uint64_t pos_idx, uint64_t size, Property_t value);

        void remove(uint64_t pos_idx, uint64_t size);

        void append_from_list(uint64_t begin_idx, Property_t* values, uint64_t size) {
            for (int i = 0; i < size; i++) {
                this->value[begin_idx + i] = values[i];
            }
        }
    };

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    struct MultiPropertyVec_t {
        std::atomic<uint32_t> ref_cnt{1};
        std::array<PropertyVec<Size>*, PropertyNum> properties{};

        MultiPropertyVec_t() = delete;

        explicit MultiPropertyVec_t(bool create_new_vec);

        MultiPropertyVec_t(const MultiPropertyVec_t &other);

        void copy_to(MultiPropertyVec_t *dst) const;

        void copy_to(uint64_t begin_idx, uint64_t end_idx, MultiPropertyVec_t *dst, uint64_t dst_idx) const;

        [[nodiscard]] Property_t get(uint64_t idx, uint8_t property_id) const;

        void get_sm(uint64_t idx, uint8_t* property_ids, uint8_t property_num, std::vector<Property_t>& results) const;

        void get_sa(uint64_t idx, std::vector<Property_t>& results) const;

        Property_t* get_sa(uint64_t idx) const;

        void get_ms(uint64_t* idxes, uint8_t idx_num, uint8_t property_id, std::vector<Property_t>& results) const;

        void set(uint64_t idx, uint8_t property_id, Property_t value);

        void insert(uint64_t pos_idx, uint64_t size, Property_t* values);

        void remove(uint64_t pos_idx, uint64_t size);

        void append_from_list(uint64_t begin_idx, Property_t** values, uint64_t size);

        void set_string(uint64_t idx, uint8_t property_id, std::string&& value);

        void set_sm(uint64_t idx, uint8_t* property_ids, Property_t* values, uint8_t property_num);

        void set_sa(uint64_t idx, Property_t* values);

        void set_ms(uint64_t* idxes, uint8_t idx_num, uint8_t property_id, Property_t* values);
    };

    using VertexPropertyVec_t = PropertyVec<256>;
    using MultiVertexPropertyVec_t = MultiPropertyVec_t<VERTEX_PROPERTY_NUM, 256, 0>;
    using RangePropertyVec_t = PropertyVec<RANGE_LEAF_SIZE>;
    using MultiRangePropertyVec_t = MultiPropertyVec_t<EDGE_PROPERTY_NUM, RANGE_LEAF_SIZE, 1>;
    using ARTPropertyVec_t = PropertyVec<ART_LEAF_SIZE>;
    using MultiARTPropertyVec_t = MultiPropertyVec_t<EDGE_PROPERTY_NUM, ART_LEAF_SIZE, 2>;

    void force_pointer_set(void *src, void *target);

    void* allocate_property_vec(uint8_t type);

    void* alloc_vertex_property_vec();

    void* alloc_vertex_property_vec_copy(void* other);

    void* alloc_vertex_property_map_with_vec();

    void* alloc_vertex_property_map_mount(void* other);

    void gc_vertex_property_map_copied(void* map);

    void gc_vertex_property_map_ref(void* map);

    void destroy_vertex_property_map(void* map);

    void vertex_property_map_copy(void *src, void *dst);

    Property_t map_get_vertex_property(void *map, uint64_t vertex, uint8_t property_id);

    void map_set_vertex_property(void *map, uint64_t vertex, uint8_t property_id, Property_t value);

    void map_set_sa_vertex_property(void *map, uint64_t vertex, void* value);

    void map_set_vertex_string_property(void *map, uint64_t vertex, uint8_t property_id, std::string&& value);

// ---------------------- Edge Property ----------------------
    void* alloc_range_property_vec();

    void* alloc_range_property_vec_copy(void* other);

    void* alloc_art_property_vec();

    void* alloc_art_property_vec_copy(void* other);

//    void* alloc_range_property_map_with_vec(WriterTraceBlock* trace_block);
//
//    void* alloc_range_property_map_mount(void* other, WriterTraceBlock* trace_block);
//
//    void* alloc_art_property_map_with_vec(WriterTraceBlock* trace_block);

    void* alloc_art_property_map_mount(void* other);

//    void gc_range_property_map_copied(void* map, WriterTraceBlock* trace_block);
//
//    void gc_art_property_map_copied(void* map, WriterTraceBlock* trace_block);
//
//    void gc_range_property_map_ref(void* map, WriterTraceBlock* trace_block);
//
//    void gc_art_property_map_ref(void* map, WriterTraceBlock* trace_block);

//    void destroy_range_property_map(void* map);
//
//    void destroy_art_property_map(void* map);

    void range_property_map_copy(void *src, void *dst);

    void art_property_map_copy(void *src, void *dst);

    void range_property_map_copy(void *src, uint64_t begin_idx, uint64_t end_idx, void *dst, uint64_t dst_idx);

    void art_property_map_copy(void *src, uint64_t begin_idx, uint64_t end_idx, void *dst, uint64_t dst_idx);

    Property_t map_get_range_property(void *map, uint64_t idx, uint8_t property_id);

    Property_t map_get_art_property(void *map, uint64_t idx, uint8_t property_id);

    Property_t* map_get_all_range_property(void *map, uint64_t idx);

    Property_t* map_get_all_art_property(void *map, uint64_t idx);

    void map_set_range_property(void *map, uint64_t idx, uint8_t property_id, Property_t value);

    void map_set_art_property(void *map, uint64_t idx, uint8_t property_id, Property_t value);

    void map_set_sa_range_property(void *map, uint64_t idx, void* value);

    void map_set_sa_art_property(void *map, uint64_t idx, void* value);

    void map_insert_range_property(void *map, uint64_t pos_idx, uint64_t size, void* value);

    void map_insert_art_property(void *map, uint64_t pos_idx, uint64_t size, void* value);

    void map_remove_range_property(void *map, uint64_t pos_idx, uint64_t size);

    void map_remove_art_property(void *map, uint64_t pos_idx, uint64_t size);

    void map_append_list_range_property(void *map, uint64_t begin_idx, void* values, uint64_t size);

    void map_append_list_art_property(void *map, uint64_t begin_idx, void* values, uint64_t size);

    void map_set_range_string_property(void *map, uint64_t idx, uint8_t property_id, std::string&& value);

    void map_set_art_string_property(void *map, uint64_t idx, uint8_t property_id, std::string&& value);
}

/// --------------------------- IMPLEMENTATION ---------------------------
namespace container {
    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    MultiPropertyVec_t<PropertyNum, Size, Type>::MultiPropertyVec_t(bool create_new_vec) {
        if(create_new_vec) {
            for (int i = 0; i < PropertyNum; i++) {
                this->properties[i] = (PropertyVec<Size> *) allocate_property_vec(Type);
            }
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    MultiPropertyVec_t<PropertyNum, Size, Type>::MultiPropertyVec_t(const MultiPropertyVec_t& other) {
        for (int i = 0; i < PropertyNum; i++) {
            this->properties[i] = other.properties[i];
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::copy_to(MultiPropertyVec_t *dst) const {
        for (int i = 0; i < PropertyNum; i++) {
            properties[i]->copy_to(dst->properties[i]);
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::copy_to(uint64_t begin_idx, uint64_t end_idx, MultiPropertyVec_t *dst, uint64_t dst_idx) const {
        for (int i = 0; i < PropertyNum; i++) {
            properties[i]->copy_to(begin_idx, end_idx, dst->properties[i], dst_idx);
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    Property_t MultiPropertyVec_t<PropertyNum, Size, Type>::get(uint64_t idx, uint8_t property_id) const {
        assert(idx < this->properties.at(0)->value.size());
        assert(property_id < PropertyNum);
        return this->properties[property_id]->value[idx];
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::set(uint64_t idx, uint8_t property_id, Property_t value) {
        assert(idx < this->properties.at(0)->value.size());
        assert(property_id < PropertyNum);
        this->properties[property_id]->value[idx] = value;
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::insert(uint64_t pos_idx, uint64_t size, Property_t* values) {
        if(!values) {
            for (int i = 0; i < PropertyNum; i++) {
                this->properties[i]->insert(pos_idx, size, 0);
            }
        } else {
            for (int i = 0; i < PropertyNum; i++) {
                this->properties[i]->insert(pos_idx, size, values[i]);
            }
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::remove(uint64_t pos_idx, uint64_t size) {
        for (int i = 0; i < PropertyNum; i++) {
            this->properties[i]->remove(pos_idx, size);
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::append_from_list(uint64_t begin_idx, Property_t** values, uint64_t size) {
        for (int i = 0; i < PropertyNum; i++) {
            for(int j = 0; j < size; j++) {
                this->properties[i]->value.at(begin_idx + j) = values[j][i];
            }
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::set_string(uint64_t idx, uint8_t property_id, std::string&& value) {
        // not implemented
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::get_sm(uint64_t idx, uint8_t* property_ids, uint8_t property_num, std::vector<Property_t>& results) const {
        assert(idx < this->properties.at(0)->value.size());
        assert(property_num < PropertyNum);
        assert(results.size() == property_num);
#ifndef NDEBUG
        for (int i = 0; i < property_num; i++) {
            assert(property_ids[i] < PropertyNum);
        }
#endif
        for (int i = 0; i < property_num; i++) {
            results[i] = this->properties[property_ids[i]]->value[idx];
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::get_sa(uint64_t idx, std::vector<Property_t>& results) const {
        assert(idx < this->properties.at(0)->value.size());
        assert(results.size() == PropertyNum);
        for (int i = 0; i < PropertyNum; i++) {
            results[i] = this->properties[i]->value[idx];
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    Property_t* MultiPropertyVec_t<PropertyNum, Size, Type>::get_sa(uint64_t idx) const {
        assert(idx < this->properties.at(0)->value.size());
        auto results = new Property_t[PropertyNum];
        for (int i = 0; i < PropertyNum; i++) {
            results[i] = this->properties[i]->value[idx];
        }
        return results;
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::get_ms(uint64_t* idxes, uint8_t idx_num, uint8_t property_id, std::vector<Property_t>& results) const {
        assert(results.size() == idx_num);
        assert(property_id < PropertyNum);
#ifndef NDEBUG
        for (int i = 0; i < idx_num; i++) {
            assert(idxes[i] < this->properties.at(0)->value.size());
        }
#endif
        for (int i = 0; i < idx_num; i++) {
            results[i] = this->properties[property_id]->value[idxes[i]];
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::set_sm(uint64_t idx, uint8_t* property_ids, Property_t* values, uint8_t property_num) {
        assert(idx < this->properties.at(0)->value.size());
        assert(property_num < PropertyNum);
#ifndef NDEBUG
        for (int i = 0; i < property_num; i++) {
            assert(property_ids[i] < PropertyNum);
        }
#endif
        for (int i = 0; i < property_num; i++) {
            this->properties[property_ids[i]]->value[idx] = values[i];
        }
    }

    // NOTE only is called in insert_vertex() now
    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::set_sa(uint64_t idx, Property_t* values) {
        assert(idx < this->properties.at(0)->value.size());
        if(!values) {
            return;
        }
        for (int i = 0; i < PropertyNum; i++) {
            this->properties[i]->value[idx] = values[i];
        }
    }

    template<uint64_t PropertyNum, uint64_t Size, uint8_t Type>
    void MultiPropertyVec_t<PropertyNum, Size, Type>::set_ms(uint64_t* idxes, uint8_t idx_num, uint8_t property_id, Property_t* values) {
        assert(property_id < PropertyNum);
#ifndef NDEBUG
        for (int i = 0; i < idx_num; i++) {
            assert(idxes[i] < this->properties.at(0)->value.size());
        }
#endif
        for (int i = 0; i < idx_num; i++) {
            this->properties[property_id]->value[idxes[i]] = values[i];
        }
    }
}