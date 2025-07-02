#include "include/neo_property.h"
#include "include/neo_reader_trace.h"

namespace container {
    template<uint64_t Size>
    void PropertyVec<Size>::copy_to(PropertyVec* dst) const {
        std::copy(this->value.begin(), this->value.end(), dst->value.begin());
    }

    template<uint64_t Size>
    void PropertyVec<Size>::copy_to(uint64_t begin_idx, uint64_t end_idx, PropertyVec *dst, uint64_t dst_idx) const {
        std::copy(this->value.begin() + begin_idx, this->value.begin() + end_idx, dst->value.begin() + dst_idx);
    }

    template<uint64_t Size>
    Property_t PropertyVec<Size>::get(uint64_t idx) const {
        assert(idx < this->value.size());
        return this->value[idx];
    }

    template<uint64_t Size>
    void PropertyVec<Size>::set(uint64_t idx, Property_t value) {
        assert(idx < this->value.size());
        this->value[idx] = value;
    }

    template<uint64_t Size>
    void PropertyVec<Size>::set_string(uint64_t idx, std::string&& value) {
        // not implemented
    }

    template<uint64_t Size>
    void PropertyVec<Size>::insert(uint64_t pos_idx, uint64_t size, Property_t value) {
        std::move_backward(this->value.begin() + pos_idx, this->value.begin() + size, this->value.begin() + size + 1);
        this->value[pos_idx] = value;
    }

    template<uint64_t Size>
    void PropertyVec<Size>::insert_copy(PropertyVec* target, uint64_t pos_idx, uint64_t size, Property_t value) {
        std::copy(this->value.begin(), this->value.begin() + pos_idx, target->value.begin());
        target->value[pos_idx] = value;
        std::copy(this->value.begin() + pos_idx, this->value.begin() + size, target->value.begin() + pos_idx + 1);
    }

    template<uint64_t Size>
    void PropertyVec<Size>::remove(uint64_t pos_idx, uint64_t size) {
        std::copy(this->value.begin() + pos_idx + 1, this->value.begin() + size, this->value.begin() + pos_idx);
    }

    void force_pointer_set(void *src, void *target) {
        *((uint64_t **) src) = (uint64_t*) target;
    }

    void gc_vertex_property_map_ref(void* map) {
        if(!map) {
            return;
        }
#if VERTEX_PROPERTY_NUM == 1
        auto vertex_property_map = static_cast<VertexPropertyVec_t *>(map);
        if (vertex_property_map->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
            deallocate_vertex_property_vec(vertex_property_map);
        }
#elif VERTEX_PROPERTY_NUM > 1
        auto vertex_property_map = static_cast<MultiVertexPropertyVec_t*>(map);
        if(vertex_property_map->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
            for(auto& vec: vertex_property_map->properties) {
                if(vec->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                    deallocate_vertex_property_vec(vec);
                }
            }
            delete vertex_property_map;
        }
#endif
    }

    void destroy_vertex_property_map(void* map) {
        if(!map) {
            return;
        }
#if VERTEX_PROPERTY_NUM == 1
        auto vertex_property_map = static_cast<VertexPropertyVec_t *>(map);
        deallocate_vertex_property_vec(vertex_property_map);
#elif VERTEX_PROPERTY_NUM > 1
        auto vertex_property_map = static_cast<MultiVertexPropertyVec_t*>(map);
        for(auto& vec: vertex_property_map->properties) {
            deallocate_vertex_property_vec(vec);
        }
        delete vertex_property_map;
#endif
    }

    void vertex_property_map_copy(void *src, void *dst) {
#if VERTEX_PROPERTY_NUM > 1
        for (int i = 0; i < VERTEX_PROPERTY_NUM; i++) {
            static_cast<MultiVertexPropertyVec_t*>(src)->properties[i]->copy_to(static_cast<MultiVertexPropertyVec_t*>(dst)->properties[i]);
        }
#else
        static_cast<VertexPropertyVec_t*>(src)->copy_to(static_cast<VertexPropertyVec_t*>(dst));
#endif
    }

    Property_t map_get_vertex_property(void *map, uint64_t vertex, uint8_t property_id) {
#if VERTEX_PROPERTY_NUM > 1
        return static_cast<MultiVertexPropertyVec_t*>(map)->get(vertex, property_id);
#else
        return static_cast<VertexPropertyVec_t*>(map)->get(vertex);
#endif
    }

    void map_set_vertex_property(void *map, uint64_t vertex, uint8_t property_id, Property_t value) {
#if VERTEX_PROPERTY_NUM > 1
        auto vertex_prop_map = static_cast<MultiVertexPropertyVec_t*>(map);
        auto new_vertex_prop_vec = allocate_vertex_property_vec();
        vertex_prop_map->properties[property_id]->copy_to(new_vertex_prop_vec);
        vertex_prop_map->properties[property_id] = new_vertex_prop_vec;
        static_cast<MultiVertexPropertyVec_t*>(map)->set(vertex, property_id, value);
#else
        static_cast<VertexPropertyVec_t*>(map)->set(vertex, value);
#endif
    }

    void map_set_sa_vertex_property(void *map, uint64_t vertex, void* value) {
#if VERTEX_PROPERTY_NUM > 1
        static_cast<MultiVertexPropertyVec_t*>(map)->set_sa(vertex, (Property_t*) value);
#else
        static_cast<VertexPropertyVec_t*>(map)->set(vertex, (Property_t) value);
#endif
    }

    void map_set_vertex_string_property(void *map, uint64_t vertex, uint8_t property_id, std::string&& value) {
#if VERTEX_PROPERTY_NUM > 1
        auto vertex_prop_map = static_cast<MultiVertexPropertyVec_t*>(map);
        auto new_vertex_prop_vec = allocate_vertex_property_vec();
        vertex_prop_map->properties[property_id]->copy_to(new_vertex_prop_vec);
        vertex_prop_map->properties[property_id] = new_vertex_prop_vec;
        static_cast<MultiVertexPropertyVec_t*>(map)->set_string(vertex, property_id, std::move(value));
#else
        static_cast<VertexPropertyVec_t*>(map)->set_string(vertex, std::move(value));
#endif
    }

// --------------------------------------- EDGE PROPERTY ---------------------------------------
    void* alloc_range_property_vec() {
        std::cout << "not implemented" << std::endl;
        return nullptr;
    }

    void* alloc_range_property_vec_copy(void* other) {
        std::cout << "not implemented" << std::endl;
        return nullptr;
    }

    void* alloc_art_property_vec() {
        std::cout << "not implemented" << std::endl;
        return nullptr;
    }

    void* alloc_art_property_vec_copy(void* other) {
        std::cout << "not implemented" << std::endl;
        return nullptr;
    }

    void* alloc_range_property_map_with_vec(WriterTraceBlock* trace_block) {
#if EDGE_PROPERTY_NUM > 1
        return new MultiRangePropertyVec_t(true);
#elif EDGE_PROPERTY_NUM  == 1
        return trace_block->allocate_range_prop_vec();
#endif
    }

    void* alloc_range_property_map_mount(void* other, WriterTraceBlock* trace_block) {
#if EDGE_PROPERTY_NUM > 1
        auto other_map = static_cast<MultiRangePropertyVec_t*>(other);
        return new MultiRangePropertyVec_t(other_map);
#else
        std::cout << "not implemented" << std::endl;
        return nullptr;
#endif
    }

    void* alloc_art_property_map_with_vec(WriterTraceBlock* trace_block) {
#if EDGE_PROPERTY_NUM > 1
        return new MultiARTPropertyVec_t(true);
#elif EDGE_PROPERTY_NUM  == 1
        return trace_block->allocate_art_prop_vec();
#endif
    }

    void* alloc_art_property_map_mount(void* other) {
#if EDGE_PROPERTY_NUM > 1
        auto other_map = static_cast<MultiARTPropertyVec_t*>(other);
        return new MultiARTPropertyVec_t(other_map);
#else
        std::cout << "not implemented" << std::endl;
        return nullptr;
#endif
    }

    void gc_range_property_map_copied(void* map, WriterTraceBlock* trace_block) {
        if(!map) {
            return;
        }
#if EDGE_PROPERTY_NUM > 1
        auto real_map = (MultiRangePropertyVec_t *)map;
        for(size_t i = 0; i < EDGE_PROPERTY_NUM; i++) {
            deallocate_range_property_vec(real_map->properties.at(i));
        }
        delete real_map;
#elif EDGE_PROPERTY_NUM  == 1
        auto real_map = (RangePropertyVec_t*)map;
        return trace_block->deallocate_range_prop_vec(real_map);
#endif
    }

    void gc_art_property_map_copied(void* map, WriterTraceBlock* trace_block) {
        if(!map) {
            return;
        }
#if EDGE_PROPERTY_NUM > 1
        auto real_map = (MultiARTPropertyVec_t *)map;
        for(size_t i = 0; i < EDGE_PROPERTY_NUM; i++) {
            deallocate_art_property_vec(real_map->properties.at(i));
        }
        delete real_map;
#elif EDGE_PROPERTY_NUM  == 1
        auto real_map = (ARTPropertyVec_t*)map;
        return trace_block->deallocate_art_prop_vec(real_map);
#endif
    }

    void gc_range_property_map_ref(void* map, WriterTraceBlock* trace_block) {
        if(!map) {
            return;
        }
#if EDGE_PROPERTY_NUM == 1
        auto range_property_map = static_cast<RangePropertyVec_t *>(map);
        if (range_property_map->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
            return trace_block->deallocate_range_prop_vec(range_property_map);
        }
#elif EDGE_PROPERTY_NUM > 1
        auto range_property_map = static_cast<MultiRangePropertyVec_t*>(map);
        if(range_property_map->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
            for(auto& vec: range_property_map->properties) {
                if(vec->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                    deallocate_range_property_vec(vec);
                }
            }
            delete range_property_map;
        }
#endif
    }

    void gc_art_property_map_ref(void* map, WriterTraceBlock* trace_block) {
        if(!map) {
            return;
        }
#if EDGE_PROPERTY_NUM == 1
        auto art_property_map = static_cast<ARTPropertyVec_t *>(map);
        if (art_property_map->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
            return trace_block->deallocate_art_prop_vec(art_property_map);
        }
#elif EDGE_PROPERTY_NUM > 1
        auto art_property_map = static_cast<MultiARTPropertyVec_t*>(map);
        if(art_property_map->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
            for(auto& vec: art_property_map->properties) {
                if(vec->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                    deallocate_art_property_vec(vec);
                }
            }
            delete art_property_map;
        }
#endif
    }

    void destroy_range_property_map(void* map) {
        if(!map) {
            return;
        }
#if EDGE_PROPERTY_NUM == 1
        auto range_property_map = static_cast<RangePropertyVec_t *>(map);
        delete range_property_map;
#elif EDGE_PROPERTY_NUM > 1
        auto range_property_map = static_cast<MultiRangePropertyVec_t*>(map);
        for(auto& vec: range_property_map->properties) {
            deallocate_range_property_vec(vec);
        }
        delete range_property_map;
#endif
    }

    void destroy_art_property_map(void* map) {
        if(!map) {
            return;
        }
#if EDGE_PROPERTY_NUM == 1
        auto art_property_map = static_cast<ARTPropertyVec_t *>(map);
        delete art_property_map;
#elif EDGE_PROPERTY_NUM > 1
        auto art_property_map = static_cast<MultiARTPropertyVec_t*>(map);
        for(auto& vec: art_property_map->properties) {
            deallocate_art_property_vec(vec);
        }
        delete art_property_map;
#endif
    }

    void range_property_map_copy(void *src, void *dst) {
#if EDGE_PROPERTY_NUM > 1
        for (int i = 0; i < EDGE_PROPERTY_NUM; i++) {
            static_cast<MultiRangePropertyVec_t*>(src)->properties[i]->copy_to(static_cast<MultiRangePropertyVec_t*>(dst)->properties[i]);
        }
#else
        static_cast<RangePropertyVec_t*>(src)->copy_to(static_cast<RangePropertyVec_t*>(dst));
#endif
    }

    void art_property_map_copy(void *src, void *dst) {
#if EDGE_PROPERTY_NUM > 1
        for (int i = 0; i < EDGE_PROPERTY_NUM; i++) {
            static_cast<MultiARTPropertyVec_t*>(src)->properties[i]->copy_to(static_cast<MultiARTPropertyVec_t*>(dst)->properties[i]);
        }
#else
        static_cast<ARTPropertyVec_t*>(src)->copy_to(static_cast<ARTPropertyVec_t*>(dst));
#endif
    }

    void range_property_map_copy(void *src, uint64_t begin_idx, uint64_t end_idx, void *dst, uint64_t dst_idx) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiRangePropertyVec_t*>(src)->copy_to(begin_idx, end_idx, static_cast<MultiRangePropertyVec_t*>(dst), dst_idx);
#else
        static_cast<RangePropertyVec_t*>(src)->copy_to(begin_idx, end_idx, static_cast<RangePropertyVec_t*>(dst), dst_idx);
#endif
    }

    void art_property_map_copy(void *src, uint64_t begin_idx, uint64_t end_idx, void *dst, uint64_t dst_idx) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiARTPropertyVec_t*>(src)->copy_to(begin_idx, end_idx, static_cast<MultiARTPropertyVec_t*>(dst), dst_idx);
#else
        static_cast<ARTPropertyVec_t*>(src)->copy_to(begin_idx, end_idx, static_cast<ARTPropertyVec_t*>(dst), dst_idx);
#endif
    }

    Property_t map_get_range_property(void *map, uint64_t idx, uint8_t property_id) {
#if EDGE_PROPERTY_NUM > 1
        return static_cast<MultiRangePropertyVec_t*>(map)->get(idx, property_id);
#else
        return static_cast<RangePropertyVec_t*>(map)->get(idx);
#endif
    }

    Property_t map_get_art_property(void *map, uint64_t idx, uint8_t property_id) {
#if EDGE_PROPERTY_NUM > 1
        return static_cast<MultiARTPropertyVec_t*>(map)->get(idx, property_id);
#else
        return static_cast<ARTPropertyVec_t*>(map)->get(idx);
#endif
    }

    Property_t* map_get_all_range_property(void *map, uint64_t idx) {
#if EDGE_PROPERTY_NUM > 1
        return static_cast<MultiRangePropertyVec_t*>(map)->get_sa(idx);
#else
        return (Property_t*)static_cast<RangePropertyVec_t*>(map)->get(idx);
#endif
    }

    Property_t* map_get_all_art_property(void *map, uint64_t idx) {
#if EDGE_PROPERTY_NUM > 1
        return static_cast<MultiARTPropertyVec_t*>(map)->get_sa(idx);
#else
        return (Property_t*)static_cast<ARTPropertyVec_t*>(map)->get(idx);
#endif
    }

    void map_set_range_property(void *map, uint64_t idx, uint8_t property_id, Property_t value) {
#if EDGE_PROPERTY_NUM > 1
        auto prop_map = static_cast<MultiRangePropertyVec_t*>(map);
        auto new_prop_vec = allocate_range_property_vec();
        prop_map->properties[property_id]->copy_to(new_prop_vec);
        prop_map->properties[property_id] = new_prop_vec;
        static_cast<MultiRangePropertyVec_t*>(map)->set(idx, property_id, value);
#else
        static_cast<RangePropertyVec_t*>(map)->set(idx, value);
#endif
    }

    void map_set_art_property(void *map, uint64_t idx, uint8_t property_id, Property_t value) {
#if EDGE_PROPERTY_NUM > 1
        auto prop_map = static_cast<MultiARTPropertyVec_t*>(map);
        auto new_prop_vec = allocate_art_property_vec();
        prop_map->properties[property_id]->copy_to(new_prop_vec);
        prop_map->properties[property_id] = new_prop_vec;
        static_cast<MultiARTPropertyVec_t*>(map)->set(idx, property_id, value);
#else
        static_cast<ARTPropertyVec_t*>(map)->set(idx, value);
#endif
    }

    void map_set_sa_range_property(void *map, uint64_t idx, void* value) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiRangePropertyVec_t*>(map)->set_sa(idx, (Property_t*) value);
#else
        static_cast<RangePropertyVec_t*>(map)->set(idx, (Property_t) value);
#endif
    }

    void map_set_sa_art_property(void *map, uint64_t idx, void* value) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiARTPropertyVec_t*>(map)->set_sa(idx, (Property_t*) value);
#else
        static_cast<ARTPropertyVec_t*>(map)->set(idx, (Property_t) value);
#endif
    }

    void map_insert_range_property(void *map, uint64_t pos_idx, uint64_t size, void* value) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiRangePropertyVec_t*>(map)->insert(pos_idx, size, (Property_t*) value);
#else
        static_cast<RangePropertyVec_t*>(map)->insert(pos_idx, size, (Property_t) value);
#endif
    }

    void map_insert_art_property(void *map, uint64_t pos_idx, uint64_t size, void* value) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiARTPropertyVec_t*>(map)->insert(pos_idx, size, (Property_t*) value);
#else
        static_cast<ARTPropertyVec_t*>(map)->insert(pos_idx, size, (Property_t) value);
#endif
    }

    void map_remove_range_property(void *map, uint64_t pos_idx, uint64_t size) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiRangePropertyVec_t*>(map)->remove(pos_idx, size);
#else
        static_cast<RangePropertyVec_t*>(map)->remove(pos_idx, size);
#endif
    }

    void map_remove_art_property(void *map, uint64_t pos_idx, uint64_t size) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiARTPropertyVec_t*>(map)->remove(pos_idx, size);
#else
        static_cast<ARTPropertyVec_t*>(map)->remove(pos_idx, size);
#endif
    }

    void map_append_list_range_property(void *map, uint64_t begin_idx, void* values, uint64_t size) {
#if EDGE_PROPERTY_NUM > 1
        static_cast<MultiRangePropertyVec_t*>(map)->append_from_list(begin_idx, (Property_t**) values, size);
#else
        static_cast<RangePropertyVec_t*>(map)->append_from_list(begin_idx, (Property_t*) values, size);
#endif
    }

    void map_append_list_art_property(void *map, uint64_t begin_idx, void* values, uint64_t size) {
#if EDGE_PROPERTY_NUM > 1
        for(int idx = 0; idx < size; idx++) {
            map_set_sa_art_property(map, begin_idx + idx, ((Property_t**)values) + begin_idx + idx);
        }
#else
        static_cast<ARTPropertyVec_t*>(map)->append_from_list(begin_idx, (Property_t*) values, size);
#endif
    }

    void map_set_range_string_property(void *map, uint64_t idx, uint8_t property_id, std::string&& value) {
#if EDGE_PROPERTY_NUM > 1
        auto prop_map = static_cast<MultiRangePropertyVec_t*>(map);
        auto new_prop_vec = allocate_range_property_vec();
        prop_map->properties[property_id]->copy_to(new_prop_vec);
        prop_map->properties[property_id] = new_prop_vec;
        static_cast<MultiRangePropertyVec_t*>(map)->set_string(idx, property_id, std::move(value));
#else
        static_cast<RangePropertyVec_t*>(map)->set_string(idx, std::move(value));
#endif
    }

    void map_set_art_string_property(void *map, uint64_t idx, uint8_t property_id, std::string&& value) {
#if EDGE_PROPERTY_NUM > 1
        auto prop_map = static_cast<MultiARTPropertyVec_t*>(map);
        auto new_prop_vec = allocate_art_property_vec();
        prop_map->properties[property_id]->copy_to(new_prop_vec);
        prop_map->properties[property_id] = new_prop_vec;
        static_cast<MultiARTPropertyVec_t*>(map)->set_string(idx, property_id, std::move(value));
#else
        static_cast<ARTPropertyVec_t*>(map)->set_string(idx, std::move(value));
#endif
    }
}