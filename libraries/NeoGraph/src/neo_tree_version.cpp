#include <fstream>
#include <set>
#include "include/neo_tree_version.h"
#include "utils/helper.h"
#include "include/neo_property.h"

namespace container {
#if FROM_CLUSTERED_TO_SMALL_VEC_ENABLE != 0
    NeoTreeVersion::NeoTreeVersion(NeoTreeVersion* prev, WriterTraceBlock* trace_block): next(prev) {
        ref_cnt = VERSION_HEAD_MASK;
        this->vertex_map = trace_block->allocate_vertex_map();
        resources = new std::vector<GCResourceInfo>{};
        resources->reserve(2);
        this->node_block = new std::vector<NeoRangeNode>{NeoRangeNode{0, 0, 0, nullptr}};
        this->node_block->resize(VERTEX_GROUP_SIZE);
        if(next == nullptr || next->node_block->empty()) {
            if(next != nullptr) {
                std::copy(next->vertex_map->begin(), next->vertex_map->end(), this->vertex_map->begin());
#if VERTEX_PROPERTY_NUM >= 1
                this->vertex_property_map = next->vertex_property_map;  // directly copy the pointer
                resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Mounted, next->vertex_property_map});
#endif
                this->independent_map = next->independent_map;
            }
            return;
        } else {
            assert(prev->node_block->at(0).key == 0);
            // copy necessary info from previous version
            if(!next->node_block->empty()) {
                std::copy(next->node_block->begin(), next->node_block->end(), this->node_block->begin());
            }
            std::copy(next->vertex_map->begin(), next->vertex_map->end(), this->vertex_map->begin());
#if VERTEX_PROPERTY_NUM >= 1
            this->vertex_property_map = next->vertex_property_map;  // directly copy the pointer
            resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Mounted, next->vertex_property_map});
#endif
            this->independent_map = next->independent_map;
        }
    }
#else
    NeoTreeVersion::NeoTreeVersion(NeoTreeVersion* prev, WriterTraceBlock* trace_block): next(prev) {
        ref_cnt = VERSION_HEAD_MASK;
        this->vertex_map = trace_block->allocate_vertex_map();
        resources = new std::vector<GCResourceInfo>{};
        resources->reserve(2);
        if(next == nullptr || next->node_block->empty()) {
            this->node_block = new std::vector<NeoRangeNode>{NeoRangeNode{0, 0, 0, nullptr}};
            if(next != nullptr) {
                std::copy(next->vertex_map->begin(), next->vertex_map->end(), this->vertex_map->begin());
#if VERTEX_PROPERTY_NUM >= 1
                this->vertex_property_map = next->vertex_property_map;  // directly copy the pointer
                resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Mounted, next->vertex_property_map});
#endif
                this->independent_map = next->independent_map;
            }
            return;
        } else {
            assert(prev->node_block->at(0).key == 0);
            this->node_block = new std::vector<NeoRangeNode>(next->node_block->size());
            // copy necessary info from previous version
            if(!next->node_block->empty()) {
                std::copy(next->node_block->begin(), next->node_block->end(), this->node_block->begin());
            }
            std::copy(next->vertex_map->begin(), next->vertex_map->end(), this->vertex_map->begin());
#if VERTEX_PROPERTY_NUM >= 1
            this->vertex_property_map = next->vertex_property_map;  // directly copy the pointer
            resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Mounted, next->vertex_property_map});
#endif
            this->independent_map = next->independent_map;
        }
    }
#endif

    void NeoTreeVersion::clean(WriterTraceBlock* trace_block) {
        trace_block->deallocate_vertex_map(vertex_map);
    }

    NeoTreeVersion::~NeoTreeVersion() {
        delete node_block;
        delete resources;
    }

    bool NeoTreeVersion::has_vertex(uint64_t vertex) const {
        return vertex_map->at(vertex & VERTEX_GROUP_MASK).exist;
    }

    bool NeoTreeVersion::has_edge(uint64_t src, uint64_t dest) const {
        NeoVertex vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t degree = vertex.degree;
        auto neighbor = (RangeElement*) vertex.neighborhood_ptr;
        if(degree == 0 || !neighbor) {
            return false;
        }

        if(!vertex.is_independent) {
            assert(!independent_map.get(src & VERTEX_GROUP_MASK));
             return range_segment_find(neighbor + vertex.neighbor_offset, degree, dest) != RANGE_LEAF_SIZE;
        } else if (!vertex.is_art) {
            return ((RangeTree*) neighbor)->has_element(dest);
        } else {
            return ((ART*)neighbor)->has_element(dest);
        }
    }

    uint64_t NeoTreeVersion::get_degree(uint64_t vertex) const {
        return vertex_map->at(vertex & VERTEX_GROUP_MASK).degree;
    }

    RangeElement *NeoTreeVersion::get_neighbor_addr(uint64_t vertex) const {
        std::cout << "NeoTreeVersion::get_neighbor_addr(): no offset for Range now" << std::endl;
        return (RangeElement*)vertex_map->at(vertex & VERTEX_GROUP_MASK).neighborhood_ptr;
    }

#if VERTEX_PROPERTY_NUM >= 1
    Property_t NeoTreeVersion::get_vertex_property(uint64_t vertex, uint8_t property_id) const {
        return map_get_vertex_property((void*) vertex_property_map, vertex & VERTEX_GROUP_MASK, property_id);
    }
#endif

#if VERTEX_PROPERTY_NUM > 1
    void NeoTreeVersion::get_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
        assert(res.size() == property_ids->capacity());
        assert(VERTEX_PROPERTY_NUM >= property_ids->size());
        vertex_property_map->get_sm(vertex, property_ids->data(), property_ids->size(), res);
    }
#endif

#if EDGE_PROPERTY_NUM != 0
    Property_t NeoTreeVersion::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const {
        NeoVertex vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t degree = vertex.degree;
        auto neighbor = (RangeElement*)vertex.neighborhood_ptr;
        if(degree == 0) {
            return Property_t();
        }

        switch(vertex.is_independent + vertex.is_art) {
            case 0: {
                auto real_neighbor = neighbor + vertex.neighbor_offset;
                auto inner_seg_idx = range_segment_find(real_neighbor, degree, dest);
                if(inner_seg_idx == RANGE_LEAF_SIZE) {
                    return Property_t();
                }
                return map_get_range_property(node_block->at(vertex.range_node_idx).property, vertex.neighbor_offset, property_id);
            }
            case 1: {
                return ((RangeTree*) neighbor)->get_property(dest, property_id);
            }
            case 2: {
                return ((ART*) neighbor)->get_property(dest, property_id);
            }
            default: {
                std::cerr << "NeoTreeVersion::get_edge_property(): Invalid storage type" << std::endl;
                abort();
            }
        }
    }
#endif

    bool NeoTreeVersion::get_neighbor(uint64_t src, std::vector<uint64_t> &neighbor) const {
        auto collect = [&] (uint64_t dest, double weight) {
            neighbor.push_back(dest);
            return 0;
        };
        edges(src, collect);
        return !neighbor.empty();
    }

    void NeoTreeVersion::intersect(NeoTreeVersion* version1, uint64_t src1, NeoTreeVersion* version2, uint64_t src2, std::vector<uint64_t> &result) {
        NeoVertex vertex1 = version1->vertex_map->at(src1 & VERTEX_GROUP_MASK);
        NeoVertex vertex2 = version2->vertex_map->at(src2 & VERTEX_GROUP_MASK);
        uint8_t storage_type1 = vertex1.is_independent + vertex1.is_art;
        uint8_t storage_type2 = vertex2.is_independent + vertex2.is_art;

        if(storage_type1 < storage_type2) {
            std::swap(vertex1, vertex2);
        }
        if(vertex1.degree == 0 || vertex2.degree == 0) {
            return;
        }

        switch(storage_type1) {
            case 0: {   // Outer range storage
                assert(storage_type2 == 0);
                auto neighbor1 = (RangeElement*)vertex1.neighborhood_ptr + vertex1.neighbor_offset;
                auto neighbor2 = (RangeElement*)vertex2.neighborhood_ptr + vertex2.neighbor_offset;
                uint64_t idx1 = 0;
                uint64_t idx2 = 0;
                while(idx1 < vertex1.degree && idx2 < vertex2.degree) {
                    if(neighbor1[idx1] == neighbor2[idx2]) {
                        result.push_back(neighbor1[idx1]);
                        idx1++;
                        idx2++;
                    } else if(neighbor1[idx1] < neighbor2[idx2]) {
                        idx1++;
                    } else {
                        idx2++;
                    }
                }
                break;
            }
            case 1: {   // Inner range storage
                auto neighbor1 = (RangeTree*)vertex1.neighborhood_ptr;
                if(storage_type2 == 0) {
                    auto neighbor2 = (RangeElement*)vertex2.neighborhood_ptr + vertex2.neighbor_offset;
                    neighbor1->range_intersect(neighbor2, vertex2.degree, result);
                } else {
                    auto neighbor2 = (RangeTree*)vertex2.neighborhood_ptr;
                    neighbor1->intersect(neighbor2, result);
                }
                break;
            }
            case 2: {   // ART storage
                auto neighbor1 = (ART*)vertex1.neighborhood_ptr;
                if(storage_type2 == 0) {
                    auto neighbor2 = (RangeElement*)vertex2.neighborhood_ptr + vertex2.neighbor_offset;
                    neighbor1->range_intersect(neighbor2, vertex2.degree, result);
                } else if(storage_type2 == 1) {
                    auto neighbor2 = (RangeTree*)vertex2.neighborhood_ptr;
                    for(int i = 0; i < neighbor2->node_block.size(); i++) {
                        neighbor1->range_intersect(((RangeElementSegment_t*)neighbor2->node_block.at(i).arr_ptr)->value.begin(), neighbor2->node_block.at(i).size, result);
                    }
                } else {
                    ART* neighbor2 = (ART*)vertex2.neighborhood_ptr;
                    neighbor1->intersect(neighbor2, result);
                }
                break;
            }
            default: {
                std::cerr << "Invalid storage type" << std::endl;
                abort();
            }
        }
    }

    uint64_t NeoTreeVersion::intersect(NeoTreeVersion* version1, uint64_t src1, NeoTreeVersion* version2, uint64_t src2) {
//        if(src1 == 92 && src2 == 934) {
//            std::cout << "debug" << std::endl;
//        }
        NeoVertex vertex1 = version1->vertex_map->at(src1 & VERTEX_GROUP_MASK);
        NeoVertex vertex2 = version2->vertex_map->at(src2 & VERTEX_GROUP_MASK);
        uint8_t storage_type1 = vertex1.is_independent + vertex1.is_art;
        uint8_t storage_type2 = vertex2.is_independent + vertex2.is_art;

        if(storage_type1 < storage_type2) {
            std::swap(vertex1, vertex2);
            std::swap(storage_type1, storage_type2);
        }
        if(vertex1.degree == 0 || vertex2.degree == 0) {
            return 0;
        }
        uint64_t res = 0;

        switch(storage_type1) {
            case 0: {   // Outer range storage
                assert(storage_type2 == 0);
                auto neighbor1 = (RangeElement*)vertex1.neighborhood_ptr + vertex1.neighbor_offset;
                auto neighbor2 = (RangeElement*)vertex2.neighborhood_ptr + vertex2.neighbor_offset;
                uint64_t idx1 = 0;
                uint64_t idx2 = 0;
                while(idx1 < vertex1.degree && idx2 < vertex2.degree) {
                    if(neighbor1[idx1] == neighbor2[idx2]) {
                        res++;
                        idx1++;
                        idx2++;
                    } else if(neighbor1[idx1] < neighbor2[idx2]) {
                        idx1++;
                    } else {
                        idx2++;
                    }
                }
                break;
            }
            case 1: {   // Inner range storage
                auto neighbor1 = (RangeTree*)vertex1.neighborhood_ptr;
                if(storage_type2 == 0) {
                    auto neighbor2 = (RangeElement*)vertex2.neighborhood_ptr + vertex2.neighbor_offset;
                    res = neighbor1->range_intersect(neighbor2, vertex2.degree);
                } else {
                    auto neighbor2 = (RangeTree*)vertex2.neighborhood_ptr;
                    res = neighbor1->intersect(neighbor2);
                }
                break;
            }
            case 2: {   // ART storage
                auto neighbor1 = (ART*)vertex1.neighborhood_ptr;
                if(storage_type2 == 0) {
                    auto neighbor2 = (RangeElement*)vertex2.neighborhood_ptr + vertex2.neighbor_offset;
                    res = neighbor1->range_intersect(neighbor2, vertex2.degree);
                } else if(storage_type2 == 1) {
                    auto neighbor2 = (RangeTree*)vertex2.neighborhood_ptr;
                    for(int i = 0; i < neighbor2->node_block.size(); i++) {
                        res += neighbor1->range_intersect(((RangeElementSegment_t*)neighbor2->node_block.at(i).arr_ptr)->value.begin(), neighbor2->node_block.at(i).size);
                    }
                } else {
                    ART* neighbor2 = (ART*)vertex2.neighborhood_ptr;
                    res = neighbor1->intersect(neighbor2);
                }
                break;
            }
            default: {
                std::cerr << "Invalid storage type" << std::endl;
                abort();
            }
        }
        return res;
    }

    std::pair<uint64_t, uint64_t> NeoTreeVersion::get_filling_info() const {
        std::pair<uint64_t, uint64_t> res = std::make_pair(0, 0);
        for(uint16_t idx = 0; idx < 256; idx++) {
            auto vertex = vertex_map->at(idx);
            if(vertex.is_art) {
                auto art = ((ART*)vertex.neighborhood_ptr);
                auto info = art->get_filling_info();
                res.first += info.first;
                res.second += info.second;
            }
        }
        return res;
    }

    std::vector<uint16_t>* NeoTreeVersion::get_vertices_in_node(uint16_t node_idx) {
        auto target = node_block->at(node_idx).arr_ptr;
        uint16_t start_vertex = node_block->at(node_idx).key;
        uint16_t end_vertex = node_idx == node_block->size() - 1 ? VERTEX_GROUP_SIZE : node_block->at(node_idx + 1).key;

        auto vertices = new std::vector<uint16_t>();
        vertices->reserve(end_vertex - start_vertex);

        for(int cur = start_vertex; cur < end_vertex; cur++) {
            if(vertex_map->at(cur).neighborhood_ptr == target) {
                vertices->push_back(cur);
            }
        }
        return vertices;
    }

    std::vector<std::pair<uint16_t, uint16_t>>* NeoTreeVersion::get_vertices_in_node_with_offset(uint16_t node_idx) {
        auto target = node_block->at(node_idx).arr_ptr;
        auto vertices = new std::vector<std::pair<uint16_t, uint16_t>>;
        if(target == 0) {
            return vertices;
        }
        uint16_t start_vertex = node_block->at(node_idx).key;
        uint16_t end_vertex = node_idx == node_block->size() - 1 ? VERTEX_GROUP_SIZE : node_block->at(node_idx + 1).key;
        vertices->reserve(end_vertex - start_vertex);

        for(int cur = start_vertex; cur < end_vertex; cur++) {
            if(vertex_map->at(cur).neighborhood_ptr == target) {
                vertices->emplace_back(cur, (uint16_t) vertex_map->at(cur).neighbor_offset);
            }
        }
        return vertices;
    }

    void NeoTreeVersion::insert_vertex(uint64_t vertex, Property_t* property) {
        vertex_map->at(vertex & VERTEX_GROUP_MASK).exist = true;
        assert(vertex_map->at(vertex & VERTEX_GROUP_MASK).degree == 0);

#if VERTEX_PROPERTY_NUM != 0
        void* new_vertex_prop_map = alloc_vertex_property_map_with_vec();
        if(this->next) {
            vertex_property_map_copy(vertex_property_map, new_vertex_prop_map);
            this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Map_All_Modified, (void *) vertex_property_map});
        }
        map_set_sa_vertex_property((void *) new_vertex_prop_map, vertex & VERTEX_GROUP_MASK, (void *) property);
        force_pointer_set(&(this->vertex_property_map), new_vertex_prop_map);
#endif
    }

    void NeoTreeVersion::insert_vertex_batch(const uint64_t* vertices, Property_t ** properties, uint64_t count) {
        if(properties == nullptr) {
            for (auto i = 0; i < count; i++) {
                vertex_map->at(vertices[i] & VERTEX_GROUP_MASK).exist = true;
                assert(vertex_map->at(vertices[i] & VERTEX_GROUP_MASK).degree == 0);
            }
        } else {
#if VERTEX_PROPERTY_NUM != 0
            void* new_vertex_prop_map = alloc_vertex_property_map_with_vec();
            if(this->next) {
                vertex_property_map_copy(vertex_property_map, new_vertex_prop_map);
            }
            for (auto i = 0; i < count; i++) {
                auto vertex = vertices[i];
                auto property = properties[i];
                vertex_map->at(vertex & VERTEX_GROUP_MASK).exist = true;
                assert(vertex_map->at(vertex & VERTEX_GROUP_MASK).degree == 0);
                map_set_sa_vertex_property((void *) new_vertex_prop_map, vertex & VERTEX_GROUP_MASK, (void *) property);
            }
            if(this->next) {
                this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Map_All_Modified, (void *) vertex_property_map});
            }
            force_pointer_set(&(this->vertex_property_map), new_vertex_prop_map);
#else
            for (auto i = 0; i < count; i++) {
                vertex_map->at(vertices[i] & VERTEX_GROUP_MASK).exist = true;
                assert(vertex_map->at(vertices[i] & VERTEX_GROUP_MASK).degree == 0);
            }
#endif
        }
    }

#if VERTEX_PROPERTY_NUM != 0
    void NeoTreeVersion::set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property) {
        auto new_vertex_prop_map = alloc_vertex_property_map_mount(vertex_property_map);
        map_set_vertex_property(new_vertex_prop_map, vertex & VERTEX_GROUP_MASK, property_id, (Property_t)property);

#if VERTEX_PROPERTY_NUM == 1
        this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Vec, (void*)vertex_property_map});
#else
        this->next->resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Copied, (void*)vertex_property_map});
        this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Vec, (void*)(vertex_property_map->properties.at(property_id))});
#endif
        force_pointer_set(&(this->vertex_property_map), new_vertex_prop_map);
    }

    void NeoTreeVersion::set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property) {
        auto new_vertex_prop_map = alloc_vertex_property_map_mount(vertex_property_map);
        map_set_vertex_string_property(new_vertex_prop_map, vertex & VERTEX_GROUP_MASK, property_id, std::move(property));
#if VERTEX_PROPERTY_NUM == 1
        this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Vec, (void*)vertex_property_map});
#else
        this->next->resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Copied, (void*)vertex_property_map});
        this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Vec, (void*)(vertex_property_map->properties.at(property_id))});
#endif
        force_pointer_set(&(this->vertex_property_map), new_vertex_prop_map);
    }
#endif

#if VERTEX_PROPERTY_NUM > 1
    void NeoTreeVersion::set_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, Property_t* properties) {
        this->next->resources->emplace_back(GCResourceInfo{Multi_Vertex_Property_Vec_Copied, (void*)vertex_property_map});
        auto new_vertex_prop_map = (MultiVertexPropertyVec_t*)alloc_vertex_property_map_mount(vertex_property_map);
        for(auto property_id: *property_ids) {
            auto new_vertex_prop_vec = alloc_vertex_property_vec_copy(vertex_property_map->properties[property_id]);
            force_pointer_set(&(new_vertex_prop_map->properties[property_id]), new_vertex_prop_map);
            this->next->resources->emplace_back(GCResourceInfo{Vertex_Property_Vec, (void*)(vertex_property_map->properties.at(property_id))});
        }
        new_vertex_prop_map->set_sm(vertex, property_ids->data(), properties, property_ids->size());
        force_pointer_set(&(this->vertex_property_map), new_vertex_prop_map);
    }
#endif

#if FROM_CLUSTERED_TO_SMALL_VEC_ENABLE != 0
    void NeoTreeVersion::insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block) {
//        if((src & VERTEX_GROUP_MASK) != (6138 & VERTEX_GROUP_MASK)) return;
#ifndef NDEBUG
        {
            int cur_node_idx = 0;
            for (int i = 0; i < VERTEX_GROUP_SIZE; i++) {
                auto ver = vertex_map->at(i);
                if(ver.degree == 0 || ver.is_independent) continue;
                assert(cur_node_idx <= ver.range_node_idx);
                cur_node_idx = ver.range_node_idx;
                assert(ver.range_node_idx < node_block->size());
            }
        }
//        if(dest != 456513) {
//            return;
//        }
        if(dest == 1674845) {
            std::cout << "debug" << std::endl;
        }
//        std::cout << src << " " << dest << std::endl;
#endif

        NeoVertex &vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        assert(vertex.exist);
        uint64_t degree = vertex.degree;

        if (!vertex.is_independent) {    // Use clustered range tree to store
            NeoRangeNode &node = node_block->at(src & VERTEX_GROUP_MASK);
            uint64_t node_idx = &node - node_block->data();
            if ((RangeElementSegment_t *) node.arr_ptr == nullptr) {
                // Create new array, don't need to update array m_size
                auto arr = trace_block->allocate_range_element_segment();
                arr->value.at(0) = dest;
                node.arr_ptr = (uint64_t) arr;
                node.size = 0;
                // vertex update
                vertex.neighborhood_ptr = (uint64_t) arr->value.begin();
                vertex.neighbor_offset = 0;
                vertex.range_node_idx = node_idx;
                vertex.degree++;
#if EDGE_PROPERTY_NUM != 0
                auto new_property_map = trace_block->allocate_range_prop_vec();
                force_pointer_set(&(node.property), new_property_map);
#endif
                return;
            }
            else if (degree == RANGE_LEAF_SIZE - 1) {
                auto new_art = direct2art(src, degree, dest, property, node, trace_block);
                if(new_art == nullptr) {
                    return;
                }

                vertex.neighborhood_ptr = (uint64_t) new_art;
                vertex.neighbor_offset = 0;
                vertex.is_independent = true;
                vertex.is_art = true;
                vertex.degree++;
                vertex.range_node_idx = 0;
                independent_map.set(src & VERTEX_GROUP_MASK);
            }
            else {
                auto arr = (RangeElementSegment_t *) node.arr_ptr;
                uint16_t arr_size = node.size + 1;

                // Insert the target vertex
                auto pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, dest);
                if (pos != arr->value.begin() + arr_size && *pos == dest) {    // exist
                    return;
                }
                int pos_idx = std::distance(arr->value.begin(), pos);
//                assert(pos_idx != 512);
                auto new_arr = trace_block->allocate_range_element_segment();
#if EDGE_PROPERTY_NUM != 0
                auto new_property_map = trace_block->allocate_range_prop_vec();
                range_segment_insert_copy(arr, node.property, arr_size, new_arr, new_property_map, pos_idx, dest, property);
                force_pointer_set(&(node.property), new_property_map);
#else
                range_segment_insert_copy(arr, nullptr, arr_size, new_arr, nullptr, pos_idx, dest, nullptr);
#endif
                // node update
                node.size++;
                node.arr_ptr = (uint64_t) new_arr;
                vertex.neighborhood_ptr = (uint64_t) new_arr;

                // vertex update
                if(vertex.degree == 0) {
                    vertex.neighbor_offset = 0;
                    vertex.range_node_idx = node_idx;
                }
                vertex.degree++;
            }
            this->next->resources->emplace_back(GCResourceInfo{Outer_Segment, (void*) next->node_block->at(node_idx).arr_ptr});
#if EDGE_PROPERTY_NUM != 0
            if(next->node_block->at(node_idx).property) {
                this->next->resources->emplace_back(GCResourceInfo{Range_Property_Map_All_Modified, (void*) next->node_block->at(node_idx).property});
            }
#endif
        }
        else if (!vertex.is_art) {    // Use independent range storage to store
            if(degree == ART_EXTRACT_THRESHOLD - 1) {
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Upgraded, (void*) vertex.neighborhood_ptr});
                auto new_art = ((RangeTree*)vertex.neighborhood_ptr)->range_tree2art(src, degree, dest, property, *this->next->resources, trace_block);
                if (new_art == nullptr) {    // already exists
                    this->next->resources->pop_back();
                    return;
                }
                vertex.neighborhood_ptr = (uint64_t) new_art;
                vertex.is_art = true;
                vertex.degree++;
            } else {
                auto vertex_range_tree = (RangeTree*) vertex.neighborhood_ptr;
                auto new_range_tree = copy_range_tree(vertex_range_tree);
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
                if(!new_range_tree->insert(src, dest, property, *this->next->resources, trace_block)) {
                    delete new_range_tree;
                    this->next->resources->pop_back();
                    return;
                }
                
                vertex.neighborhood_ptr = (uint64_t) new_range_tree;
                vertex.degree++;
            }
        }
        else {    // use independent ART to store
//            return;
#ifndef NDEBUG
            if(src == 2 && dest == 14 && degree == 0) {
                std::cout << "debug" << std::endl;
            }
#endif
            assert(vertex.is_art);
            auto vertex_art = (ART*) vertex.neighborhood_ptr;

            ARTInsertElemCopyRes res = vertex_art->insert_element_copy(src, dest, property, trace_block);
            if (res.is_new) {
                vertex.degree++;
                vertex.neighborhood_ptr = (uint64_t) res.art_ptr;
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
            }
        }
    }
#else
    void NeoTreeVersion::insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block) {
//        if((src & VERTEX_GROUP_MASK) != (6138 & VERTEX_GROUP_MASK)) return;
#ifndef NDEBUG
        {
            int cur_node_idx = 0;
            for (int i = 0; i < VERTEX_GROUP_SIZE; i++) {
                auto ver = vertex_map->at(i);
                if(ver.degree == 0 || ver.is_independent) continue;
                assert(cur_node_idx <= ver.range_node_idx);
                cur_node_idx = ver.range_node_idx;
                assert(ver.range_node_idx < node_block->size());
            }
        }
//        if(src != 934) {
//            return;
//        }
#endif
//        if(src == 130049 && dest == 1357896) {
//            std::cout << "src: " << src << " dest: " << dest << std::endl;
//        }

        NeoVertex &vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        assert(vertex.exist);
        uint64_t degree = vertex.degree;

        if (!vertex.is_independent) {    // Use clustered range tree to store
            NeoRangeNode &node = *find_range_node(src & VERTEX_GROUP_MASK);
            uint64_t node_idx = &node - node_block->data();
            if ((RangeElementSegment_t *) node.arr_ptr == nullptr) {
                // Create new array, don't need to update array m_size
                auto arr = trace_block->allocate_range_element_segment();
                arr->value.at(0) = dest;
                node.arr_ptr = (uint64_t) arr;
                node.size = 0;
                // vertex update
                vertex.neighborhood_ptr = (uint64_t) arr->value.begin();
                vertex.neighbor_offset = 0;
                vertex.range_node_idx = node_idx;
                vertex.degree++;
#if EDGE_PROPERTY_NUM != 0
                auto new_property_map = trace_block->allocate_range_prop_vec();
                force_pointer_set(&(node.property), new_property_map);
#endif

//                std::vector<RangeElement> extract_list;
//                extract_list.push_back(dest);
//#if EDGE_PROPERTY_NUM != 0
//                std::vector<Property_t*> extract_prop_list;
//                extract_prop_list.push_back(property);
//#endif
//                ART* new_art = new ART();
//                delete new_art->root;
//                batch_subtree_build<false>(&new_art->root, 0, extract_list.data(), extract_prop_list.data(), degree + 1, trace_block);
//
//                if(new_art == nullptr) {
//                    return;
//                }
//                vertex.neighborhood_ptr = (uint64_t) new_art;
//                vertex.neighbor_offset = 0;
//                vertex.is_independent = true;
//                vertex.is_art = true;
//                vertex.degree++;
//                vertex.range_node_idx = 0;
//                independent_map.set(src & VERTEX_GROUP_MASK);
                return;
            }
            else if (degree == RANGE_LEAF_SIZE / 2 - 1) {
                auto new_range_tree = extract2range_tree(src, degree, dest, property, node, trace_block);
                if(new_range_tree == nullptr) {
                    return;
                }
                vertex.neighborhood_ptr = (uint64_t) new_range_tree;
                vertex.neighbor_offset = 0;
                vertex.is_independent = true;
                vertex.degree++;
                vertex.range_node_idx = 0;
                independent_map.set(src & VERTEX_GROUP_MASK);

            }
            else {
                auto arr = (RangeElementSegment_t *) node.arr_ptr;
                uint16_t arr_size = node.size + 1;
                std::vector<uint16_t>* vertices = get_vertices_in_node(node_idx);

                // Insert the target vertex
                if (arr_size != RANGE_LEAF_SIZE) {
                    int pos_idx = find_position_to_be_inserted(src, dest, vertex, arr, arr_size, vertices);   // global position index of the target edge
                    if (pos_idx == -1) {    // exist
                        delete vertices;
                        return;
                    }
                    auto new_arr = trace_block->allocate_range_element_segment();
#if EDGE_PROPERTY_NUM != 0
                    auto new_property_map = trace_block->allocate_range_prop_vec();
                    range_segment_insert_copy(arr, node.property, arr_size, new_arr, new_property_map, pos_idx, dest, property);
                    force_pointer_set(&(node.property), new_property_map);
#else
                    range_segment_insert_copy(arr, nullptr, arr_size, new_arr, nullptr, pos_idx, dest, nullptr);
#endif
                    // node update
                    node.size++;
                    node.arr_ptr = (uint64_t) new_arr;

                    // vertex_map_update(new_arr->value.begin(), arr_size + 1, node_idx);
                    vertex_map_update_split(new_arr, vertices->data(), vertices->size(), node_idx, src & VERTEX_GROUP_MASK, 1);

                    // vertex update
                    if(vertex.degree == 0) {
                        vertex.neighbor_offset = pos_idx;
                        vertex.neighborhood_ptr = (uint64_t) new_arr;
                        vertex.range_node_idx = node_idx;
                    }
                    vertex.degree++;
                }
                else {
                    // split
                    uint16_t split_vertex_idx = find_mid_count(vertices);
                    uint16_t split_vertex = vertices->at(split_vertex_idx);
                    int split_pos = vertex_map->at(split_vertex).neighbor_offset;
                    assert(split_pos < arr_size && split_pos != 0);
                    auto new_l_arr = trace_block->allocate_range_element_segment();
                    auto new_r_arr = trace_block->allocate_range_element_segment();
#if EDGE_PROPERTY_NUM != 0
                    auto new_l_prop_map = trace_block->allocate_range_prop_vec();
                    auto new_r_prop_map = trace_block->allocate_range_prop_vec();
                    range_segment_split(arr, node.property, arr_size, new_l_arr, new_l_prop_map, new_r_arr, new_r_prop_map, split_pos);
                    force_pointer_set(&(node.property), new_l_prop_map);
#else
                    range_segment_split(arr, nullptr, arr_size, new_l_arr, nullptr, new_r_arr, nullptr, split_pos);
#endif

                    // update node
                    node.arr_ptr = (uint64_t) new_l_arr;
                    node.size = split_pos - 1;

                    // insert new_r_arr into the block
#if EDGE_PROPERTY_NUM != 0
                    NeoRangeNode new_node{split_vertex, (uint64_t) (arr_size - split_pos - 1), (uint64_t) new_r_arr, new_r_prop_map};
#else
                    NeoRangeNode new_node{split_vertex, (uint64_t) (arr_size - split_pos - 1), (uint64_t) new_r_arr, nullptr};
#endif

                    assert(this->node_block->at(0).key == 0);
                    node_block->insert(node_block->begin() + node_idx + 1, new_node);

                    // move the following nodes back
                    for(int i = vertices->at(vertices->size() - 1) + 1; i < VERTEX_GROUP_SIZE; i++) {
                        if(vertex_map->at(i).degree > 0 && !vertex_map->at(i).is_independent) {
                            vertex_map->at(i).range_node_idx++;
                        }
                    }

                    // update vertex_map
                    // vertex_map_update(new_l_arr->value.begin(), split_pos, node_idx);
                    // vertex_map_update(new_r_arr->value.begin(), arr_size - split_pos, node_idx + 1);
                    vertex_map_update_split(new_l_arr, vertices->data(), split_vertex_idx, node_idx, split_vertex, 0);
                    vertex_map_update_split(new_r_arr, vertices->data() + split_vertex_idx, vertices->size() - split_vertex_idx, node_idx + 1, 0, -split_pos);  // 0 - apply offset to every vertex

                    // insert target
                    second_insert_edge(src, dest, vertex, property, trace_block);
                }
                delete vertices;
            }

#ifndef NDEBUG
            {
                int cur_node_idx = 0;
                for (int i = 0; i < VERTEX_GROUP_SIZE; i++) {
                    auto ver = vertex_map->at(i);
                    if(ver.degree == 0 || ver.is_independent) continue;
                    assert(cur_node_idx <= ver.range_node_idx);
                    cur_node_idx = ver.range_node_idx;
                    assert(ver.range_node_idx < node_block->size());
                }
            }
#endif
            this->next->resources->emplace_back(GCResourceInfo{Outer_Segment, (void*) next->node_block->at(node_idx).arr_ptr});
#if EDGE_PROPERTY_NUM != 0
            if(next->node_block->at(node_idx).property) {
                this->next->resources->emplace_back(GCResourceInfo{Range_Property_Map_All_Modified, (void*) next->node_block->at(node_idx).property});
            }
#endif
        }
        else if (!vertex.is_art) {    // Use independent range storage to store
            if(degree == ART_EXTRACT_THRESHOLD - 1) {
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Upgraded, (void*) vertex.neighborhood_ptr});
                auto new_art = ((RangeTree*)vertex.neighborhood_ptr)->range_tree2art(src, degree, dest, property, *this->next->resources, trace_block);
                if (new_art == nullptr) {    // already exists
                    this->next->resources->pop_back();
                    return;
                }
                vertex.neighborhood_ptr = (uint64_t) new_art;
                vertex.is_art = true;
                vertex.degree++;
            } else {
                auto vertex_range_tree = (RangeTree*) vertex.neighborhood_ptr;
                auto new_range_tree = copy_range_tree(vertex_range_tree);
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
                if(!new_range_tree->insert(src, dest, property, *this->next->resources, trace_block)) {
                    delete new_range_tree;
                    this->next->resources->pop_back();
                    return;
                }

                vertex.neighborhood_ptr = (uint64_t) new_range_tree;
                vertex.degree++;
            }
        }
        else {    // use independent ART to store
//            return;
#ifndef NDEBUG
            if(src == 2 && dest == 14 && degree == 0) {
                std::cout << "debug" << std::endl;
            }
#endif
            assert(vertex.is_art);
            auto vertex_art = (ART*) vertex.neighborhood_ptr;

            ARTInsertElemCopyRes res = vertex_art->insert_element_copy(src, dest, property, trace_block);
            if (res.is_new) {
                vertex.degree++;
                vertex.neighborhood_ptr = (uint64_t) res.art_ptr;
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
            }
        }
    }
#endif

    void NeoTreeVersion::second_insert_edge(uint64_t src, RangeElement target, NeoVertex& vertex, Property_t* property, WriterTraceBlock* trace_block) {
        NeoRangeNode &node = *find_range_node(src & VERTEX_GROUP_MASK);
        uint64_t node_idx = &node - node_block->data();
        std::vector<uint16_t>* vertices = get_vertices_in_node(node_idx);

        auto arr = (RangeElementSegment_t*) node.arr_ptr;
        uint16_t arr_size = node.size + 1;

        int pos_idx = find_position_to_be_inserted(src, target, vertex, arr, arr_size, vertices);
        if(pos_idx == -1) {
            return;
        }

#if EDGE_PROPERTY_NUM != 0
        range_segment_insert(arr, node.property, arr_size, pos_idx, target, property);
#else
        range_segment_insert(arr, nullptr, arr_size, pos_idx, target, nullptr);
#endif
        node.size++;

        // vertex update
        if (vertex.degree == 0) {   // vertex does not exist in the vertices
            vertex.neighbor_offset = pos_idx;
            vertex.neighborhood_ptr = (uint64_t)arr;
            vertex.range_node_idx = node_idx;
        }
        vertex.degree++;

        // vertex_map_update(arr->value.begin(), arr_size + 1, node_idx);

        vertex_map_update_split(arr, vertices->data(), vertices->size(), node_idx, src & VERTEX_GROUP_MASK, 1);
        delete vertices;
    }

    NeoRangeNode* NeoTreeVersion::find_range_node(uint64_t vertex) const {
        uint16_t node_idx = 0;
        // find the first node that key is smaller or equal to src but the next node is larger than src
        while (node_idx < node_block->size() && node_block->at(node_idx).key <= vertex) {
            node_idx++;
        }
        if(node_idx != 0) {
            node_idx--;
        }
        if(node_idx >= node_block->size()) {
            throw std::runtime_error("Node index out of range");
        }
        return &node_block->at(node_idx);
    }

    int NeoTreeVersion::find_position_to_be_inserted(uint64_t src, uint64_t dest, NeoVertex vertex, RangeElementSegment_t* arr, uint16_t arr_size, std::vector<uint16_t>* vertices) {
        int res = 0;
        // Fine the insertion position
        if(vertex.degree == 0) {
            // First, try to find the target vertex in vertices in the node
            auto vertex_pos = std::lower_bound(vertices->begin(), vertices->end(), src & VERTEX_GROUP_MASK);
            // Second, if the target vertex is not in the node, just set the pos as the start of the next vertex
            if(vertex_pos == vertices->end()) { // no next vertex
                res = arr_size;
            } else { // there is a next vertex
                res = vertex_map->at(*vertex_pos).neighbor_offset;
            }
        } else {
            auto neighbor_ptr = (RangeElement*)vertex.neighborhood_ptr + vertex.neighbor_offset;
            auto pos = std::lower_bound(neighbor_ptr, neighbor_ptr + vertex.degree, dest);
            if (pos != neighbor_ptr + vertex.degree && *pos == dest) {
                res = -1;
            } else {
                res = std::distance(arr->value.begin(), pos);
            }
        }
        return res;
    }

    uint16_t NeoTreeVersion::find_mid_count(std::vector<uint16_t>* vertices) {
        uint16_t cur_idx = 0;
        while(cur_idx < vertices->size() - 1) {
            cur_idx += 1;
            auto cur_ver = vertices->at(cur_idx);
            if(vertex_map->at(cur_ver).neighbor_offset >= RANGE_LEAF_SIZE / 2) {
                break;
            }
        }
        return cur_idx;
    }

    void NeoTreeVersion::remove_node(NeoRangeNode& node, uint64_t node_idx, uint16_t nodes_move_begin_point) {
        // move the following nodes forward
        for(int i = nodes_move_begin_point; i < VERTEX_GROUP_SIZE; i++) {
            if(vertex_map->at(i).degree > 0 && !vertex_map->at(i).is_independent) {
                vertex_map->at(i).range_node_idx--;
            }
        }
        if(node.key == 0) {
            // turn the next node's key as 0
            if(node_block->size() > 1) {
                node_block->at(1).key = 0;
                node_block->erase(node_block->begin() + node_idx);
            } else {
                node.arr_ptr = 0;
                node.size = 0;
#if EDGE_PROPERTY_NUM != 0
                node.property = nullptr;
#endif
            }
        } else {
            node_block->erase(node_block->begin() + node_idx);
        }
    }

    ///@brief Update vertex_map's neighbor_ptr according to the given segment
    void NeoTreeVersion::vertex_map_update(RangeElementSegment_t* segment, uint16_t* vertices, uint16_t vertex_num, uint16_t node_idx, int offset) {
        uint16_t* cur_vertex = vertices;
        for(int i = 0; i < vertex_num; i++, cur_vertex++) {
            vertex_map->at(*cur_vertex).neighborhood_ptr = (uint64_t)(segment);
            vertex_map->at(*cur_vertex).neighbor_offset += offset;
            vertex_map->at(*cur_vertex).range_node_idx = node_idx;
        }
    }

    void NeoTreeVersion::vertex_map_update_split(RangeElementSegment_t* segment, uint16_t* vertices, uint16_t vertex_num, uint16_t node_idx, uint16_t last_vertex_unchanged, int offset) {
        assert(node_idx < node_block->size());
        uint16_t* cur_vertex = vertices;
        for(int i = 0; i < vertex_num; i++, cur_vertex++) {
            vertex_map->at(*cur_vertex).neighborhood_ptr = (uint64_t )segment;  // TODO NOTE this is safe because value is the first field of RangeElementSegment
            vertex_map->at(*cur_vertex).neighbor_offset += *cur_vertex > last_vertex_unchanged ? offset : 0;
            vertex_map->at(*cur_vertex).range_node_idx = node_idx;
        }
    }

#if FROM_CLUSTERED_TO_SMALL_VEC_ENABLE != 0
    ///@brief Extract a neighbor from range system to RangeTree
    RangeTree* NeoTreeVersion::extract2range_tree(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, NeoRangeNode& range_node, WriterTraceBlock* trace_block) {
        auto arr = (RangeElementSegment_t *) range_node.arr_ptr;
        auto vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t arr_size = range_node.size + 1;
        uint64_t node_idx = &range_node - node_block->data();
        assert(node_idx == (src & VERTEX_GROUP_MASK));

        // find the position to be inserted

        auto pos = std::lower_bound(arr->value.begin(), arr->value.begin() + arr_size, new_element);
        if (pos != arr->value.begin() + arr_size && *pos == new_element) {    // exist
            return nullptr;
        }
        int pos_idx = std::distance(arr->value.begin(), pos);
        RangeElement* neighbor_begin = (RangeElement*)vertex.neighborhood_ptr;

#if EDGE_PROPERTY_NUM != 0
        Property_t* prop_arr = range_node.property->value.begin() + vertex.neighbor_offset;
        auto new_range_tree = new RangeTree{neighbor_begin, prop_arr, degree, new_element, new_property, static_cast<uint64_t>(pos_idx), trace_block};
#else
        auto new_range_tree = new RangeTree{neighbor_begin, nullptr, degree, new_element, nullptr, (uint64_t)pos_idx, trace_block};
#endif
        return new_range_tree;
    }
#else
    ///@brief Extract a neighbor from range system to RangeTree
    RangeTree* NeoTreeVersion::extract2range_tree(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, NeoRangeNode& range_node, WriterTraceBlock* trace_block) {
        auto arr = (RangeElementSegment_t *) range_node.arr_ptr;
        auto vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t arr_size = range_node.size + 1;
        uint64_t node_idx = &range_node - node_block->data();

        // find the position to be inserted
        std::vector<uint16_t>* vertices = get_vertices_in_node(node_idx);
        int pos_idx = find_position_to_be_inserted(src, new_element, vertex, arr, arr_size, vertices);  // pos_idx is outer offset now, but it should be inner offset
        if(pos_idx == -1) {
            return nullptr;
        }
        RangeElement* neighbor_begin = (RangeElement*)vertex.neighborhood_ptr + vertex.neighbor_offset;
        pos_idx = pos_idx - vertex.neighbor_offset; // turn outer offset to inner offset

#ifndef NDEBUG
        {
            int cur_node_idx = 0;
            for (int i = 0; i < VERTEX_GROUP_SIZE; i++) {
                auto ver = vertex_map->at(i);
                if(ver.degree == 0 || ver.is_independent) continue;
                assert(cur_node_idx <= ver.range_node_idx);
                cur_node_idx = ver.range_node_idx;
                assert(ver.range_node_idx < node_block->size());
            }
        }
#endif
#if EDGE_PROPERTY_NUM != 0
        Property_t* prop_arr = range_node.property->value.begin() + vertex.neighbor_offset;
        auto new_range_tree = new RangeTree{neighbor_begin, prop_arr, degree, new_element, new_property, static_cast<uint64_t>(pos_idx), trace_block};
#else
        auto new_range_tree = new RangeTree{neighbor_begin, nullptr, degree, new_element, nullptr, (uint64_t)pos_idx, trace_block};
#endif

        // clean old array
        if(arr_size > degree) {
            auto new_arr = trace_block->allocate_range_element_segment();
            range_node.arr_ptr = (uint64_t) new_arr;
            range_node.size = arr_size - degree - 1;

            std::copy(arr->value.begin(), neighbor_begin, new_arr->value.begin());
            std::copy(neighbor_begin + degree, arr->value.begin() + arr_size, new_arr->value.begin() + vertex.neighbor_offset);
#if EDGE_PROPERTY_NUM != 0
            if(prop_arr) {
                auto new_prop_arr = trace_block->allocate_range_prop_vec();
                range_property_map_copy(prop_arr, 0, vertex.neighbor_offset, new_prop_arr, 0);
                range_property_map_copy(prop_arr, vertex.neighbor_offset + degree, arr_size, new_prop_arr,
                                        vertex.neighbor_offset);
                range_node.property = new_prop_arr;
            }
#endif

            // vertex_map_update(new_arr->value.begin(), arr_size - degree, &range_node - node_block->data());
            // extracted vertex will be modified outside
            vertex_map_update_split(new_arr, vertices->data(), vertices->size(), node_idx, src & VERTEX_GROUP_MASK, -(int)vertex.degree);
            if(range_node.key != 0 && vertex.neighbor_offset == 0) {
                assert(vertices->at(0) == (src & VERTEX_GROUP_MASK) && vertices->size() > 1);
                range_node.key = vertices->at(1);
            }
        } else {
            remove_node(range_node, node_idx, vertices->at(vertices->size() - 1) + 1);
        }
#ifndef NDEBUG
        {
            int cur_node_idx = 0;
            for (int i = 0; i < VERTEX_GROUP_SIZE; i++) {
                auto ver = vertex_map->at(i);
                if(ver.degree == 0 || ver.is_independent || i == (src & VERTEX_GROUP_MASK)) continue;
                assert(cur_node_idx <= ver.range_node_idx);
                cur_node_idx = ver.range_node_idx;
                assert(ver.range_node_idx < node_block->size());
            }
        }
#endif
        delete vertices;
        return new_range_tree;
    }
#endif

#if FROM_CLUSTERED_TO_SMALL_VEC_ENABLE != 0
    ART* NeoTreeVersion::direct2art(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, NeoRangeNode& range_node, WriterTraceBlock* trace_block) {
        auto arr = (RangeElementSegment_t *) range_node.arr_ptr;
        auto vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t arr_size = range_node.size + 1;

#if EDGE_PROPERTY_NUM != 0
        Property_t* prop_arr = range_node.property->value.begin() + vertex.neighbor_offset;
#endif

        std::vector<RangeElement> extract_list;
        extract_list.reserve(degree + 1);
        for (auto i = 0; i < degree; i++) {
            extract_list.push_back(arr->value.at(i));
        }
        auto new_element_pos = std::lower_bound(extract_list.begin(), extract_list.end(), new_element);
        if(*new_element_pos == new_element) {
            return nullptr;
        }
        extract_list.insert(new_element_pos, new_element);


#if EDGE_PROPERTY_NUM != 0
        std::vector<Property_t*> extract_prop_list;
        extract_prop_list.reserve(degree + 1);

        for (auto i = 0; i < degree; i++) {
            if(prop_arr) {
                extract_prop_list.push_back(map_get_all_range_property(prop_arr, i));
            } else {
                extract_prop_list.push_back(nullptr);
            }
        }
        extract_prop_list.insert(extract_prop_list.begin() + std::distance(extract_list.begin(), new_element_pos), new_property);
#endif

        ART* new_art = new ART();
        delete new_art->root;
        // build art
        assert(extract_list.size() == degree + 1);
#if EDGE_PROPERTY_NUM != 0
        batch_subtree_build<false>(&new_art->root, 0, extract_list.data(), extract_prop_list.data(), degree + 1, trace_block);
#else
        batch_subtree_build<false>(&new_art->root, 0, extract_list.data(), nullptr, degree + 1, trace_block);
#endif
        return new_art;
    }
#else

    ART* NeoTreeVersion::direct2art(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, NeoRangeNode& range_node, WriterTraceBlock* trace_block) {
        auto arr = (RangeElementSegment_t *) range_node.arr_ptr;
        auto vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t arr_size = range_node.size + 1;
        uint64_t node_idx = &range_node - node_block->data();

        // find the position to be inserted
        std::vector<uint16_t>* vertices = get_vertices_in_node(node_idx);
        int pos_idx = find_position_to_be_inserted(src, new_element, vertex, arr, arr_size, vertices);  // pos_idx is outer offset now, but it should be inner offset
        if(pos_idx == -1) {
            return nullptr;
        }
        RangeElement* neighbor_begin = (RangeElement*)vertex.neighborhood_ptr + vertex.neighbor_offset;
        pos_idx = pos_idx - vertex.neighbor_offset; // turn outer offset to inner offset

#ifndef NDEBUG
        {
            int cur_node_idx = 0;
            for (int i = 0; i < VERTEX_GROUP_SIZE; i++) {
                auto ver = vertex_map->at(i);
                if(ver.degree == 0 || ver.is_independent) continue;
                assert(cur_node_idx <= ver.range_node_idx);
                cur_node_idx = ver.range_node_idx;
                assert(ver.range_node_idx < node_block->size());
            }
        }
#endif
#if EDGE_PROPERTY_NUM != 0
        Property_t* prop_arr = range_node.property->value.begin() + vertex.neighbor_offset;
//        auto new_range_tree = new RangeTree{neighbor_begin, prop_arr, degree, new_element, new_property, static_cast<uint64_t>(pos_idx), trace_block};
#else
//        auto new_range_tree = new RangeTree{neighbor_begin, nullptr, degree, new_element, nullptr, (uint64_t)pos_idx, trace_block};
#endif

        ART* new_art = new ART();
        delete new_art->root;

        std::vector<RangeElement> extract_list;
        extract_list.reserve(degree + 1);
        for (auto i = 0; i < degree; i++) {
            extract_list.push_back(neighbor_begin[i]);
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

        for (auto i = 0; i < degree; i++) {
            if(prop_arr) {
                extract_prop_list.push_back(map_get_all_range_property(prop_arr, i));
            } else {
                extract_prop_list.push_back(nullptr);
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

        // clean old array
        if(arr_size > degree) {
            auto new_arr = trace_block->allocate_range_element_segment();
            range_node.arr_ptr = (uint64_t) new_arr;
            range_node.size = arr_size - degree - 1;

            std::copy(arr->value.begin(), neighbor_begin, new_arr->value.begin());
            std::copy(neighbor_begin + degree, arr->value.begin() + arr_size, new_arr->value.begin() + vertex.neighbor_offset);
#if EDGE_PROPERTY_NUM != 0
            if(prop_arr) {
                auto new_prop_arr = trace_block->allocate_range_prop_vec();
                range_property_map_copy(prop_arr, 0, vertex.neighbor_offset, new_prop_arr, 0);
                range_property_map_copy(prop_arr, vertex.neighbor_offset + degree, arr_size, new_prop_arr,
                                        vertex.neighbor_offset);
                range_node.property = new_prop_arr;
            }
#endif

            // vertex_map_update(new_arr->value.begin(), arr_size - degree, &range_node - node_block->data());
            // extracted vertex will be modified outside
            vertex_map_update_split(new_arr, vertices->data(), vertices->size(), node_idx, src & VERTEX_GROUP_MASK, -(int)vertex.degree);
            if(range_node.key != 0 && vertex.neighbor_offset == 0) {
                assert(vertices->at(0) == (src & VERTEX_GROUP_MASK) && vertices->size() > 1);
                range_node.key = vertices->at(1);
            }
        } else {
            remove_node(range_node, node_idx, vertices->at(vertices->size() - 1) + 1);
        }
        delete vertices;
        return new_art;
    }

#endif

    void NeoTreeVersion::remove_edge(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block) {
#ifndef NDEBUG
        if(src != 2) {
            return;
        }
#endif
        NeoVertex& vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        assert(vertex.exist);

        switch(vertex.is_independent + vertex.is_art) {
            case 0: {   // Outer Range
                NeoRangeNode &node = *find_range_node(src & VERTEX_GROUP_MASK);
                uint64_t node_idx = &node - node_block->data();

                if ((RangeElementSegment_t *) node.arr_ptr == nullptr) {
                    return; // edge not found
                } else {
                    auto arr = (RangeElementSegment_t *) vertex.neighborhood_ptr;
                    auto neighbor = (RangeElement *) vertex.neighborhood_ptr + vertex.neighbor_offset;
                    uint64_t arr_size = node.size + 1;

                    auto pos = std::lower_bound(neighbor, neighbor + vertex.degree, dest);
                    if(pos == neighbor + vertex.degree || *pos != dest) {
                        return; // edge not found
                    }

                    if (arr_size != 1) {
                        auto vertices = get_vertices_in_node(node_idx);
                        auto new_arr = trace_block->allocate_range_element_segment();
                        auto pos_idx = std::distance(arr->value.begin(), pos);
#if EDGE_PROPERTY_NUM != 0
                        auto new_property_map = trace_block->allocate_range_prop_vec();
                        range_segment_remove(arr, node.property, arr_size, new_arr, new_property_map, pos_idx);
                        force_pointer_set(&(node.property), new_property_map);
#else
                        range_segment_remove(arr, nullptr, arr_size, new_arr, nullptr, pos_idx);
#endif
                        // node update
                        node.size -= 1;
                        node.arr_ptr = (uint64_t) new_arr;

                        // vertex update
                        vertex_map_update_split(new_arr, vertices->data(), vertices->size(), node_idx, src & VERTEX_GROUP_MASK, -1);
                        delete vertices;
                    } else {
                        remove_node(node, node_idx, (src & VERTEX_GROUP_MASK) + 1);
                    }
                    vertex.degree -= 1;
                    if (vertex.degree == 0) {
                        vertex.neighborhood_ptr = 0;
                        vertex.neighbor_offset = 0;
                        vertex.range_node_idx = 0;
                    }
#if EDGE_PROPERTY_NUM != 0
                    this->next->resources->emplace_back(GCResourceInfo{Range_Property_Map_All_Modified, (void *) next->node_block->at(node_idx).property});
#endif
                    this->next->resources->emplace_back(GCResourceInfo{Outer_Segment, (void *) next->node_block->at(node_idx).arr_ptr});
                }
                break;
            }
            case 1: {   // Inner Range
                auto vertex_range_tree = (RangeTree*)vertex.neighborhood_ptr;
                auto new_range_tree = copy_range_tree(vertex_range_tree);
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
                if(!new_range_tree->remove(dest, *this->next->resources, trace_block)) {
                    delete new_range_tree;
                    this->next->resources->pop_back();
                    return;
                }

                vertex.neighborhood_ptr = (uint64_t)new_range_tree;
                vertex.degree--;
                break;
            }
            case 2: {   // ART
                auto vertex_art = (ART*)vertex.neighborhood_ptr;
#ifndef NDEBUG
                if(src == 2 && vertex.degree == 1) {
                    std::cout << "debug" << std::endl;
                }
#endif
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
                ARTRemoveElemCopyRes res = vertex_art->remove_element_copy(src, dest, trace_block);
                if(res.found) {
                    vertex.degree--;
                    vertex.neighborhood_ptr = (uint64_t)res.tree_ptr;
                    assert(!((ART*)res.tree_ptr)->has_element(dest));
                } else {
                    this->next->resources->pop_back();
                    assert(false);
                }
                break;
            }
            default: {
                std::cerr << "NeoTreeVersion::remove_edge(): Invalid storage type" << std::endl;
                break;
            }
        }
    }


    void NeoTreeVersion::remove_vertex(uint64_t vertex, bool is_directed, WriterTraceBlock* trace_block) {
        // remove reverse edges
        NeoVertex& vertex_entry = vertex_map->at(vertex);
        if(!is_directed) {
            edges(vertex, [&] (uint64_t dest, double weight) {
                remove_edge(dest, vertex, trace_block);
                return 0;
            });
        }

        switch (vertex_entry.is_independent + vertex_entry.is_art) {
            case 0: {   // Outer Range
                edges(vertex, [&] (uint64_t dest, double weight) {
                    remove_edge(vertex, dest, trace_block);
                    return 0;
                });
                break;
            }
            case 1: {   // Inner Range
                auto vertex_range_tree = (RangeTree*)vertex_entry.neighborhood_ptr;
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Upgraded, (void*) vertex_range_tree});
                for(int i = 0; i < vertex_range_tree->node_block.size(); i++) {
                    resources->emplace_back(GCResourceInfo{Inner_Segment, (void*) vertex_range_tree->node_block.at(i).arr_ptr});
                }
                break;
            }
            case 2: {   // ART
                auto vertex_art = (ART*)vertex_entry.neighborhood_ptr;
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
                break;
            }
            default: {
                std::cerr << "NeoTreeVersion::remove_vertex(): Invalid storage type" << std::endl;
                break;
            }
        }

        vertex_entry.degree = 0;
        vertex_entry.exist = false;
        vertex_entry.neighborhood_ptr = 0;
        vertex_entry.neighbor_offset = 0;
    }

#if EDGE_PROPERTY_NUM != 0
    void NeoTreeVersion::set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property, WriterTraceBlock* trace_block) {
        NeoVertex &vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t degree = vertex.degree;
        auto neighbor = (RangeElement*)vertex.neighborhood_ptr;
        if(degree == 0) {
            return;
        }

        switch(vertex.is_independent + vertex.is_art) {
            case 0: {
                auto arr_size = node_block->at(vertex.range_node_idx).size + 1;
                auto old_prop_map = node_block->at(vertex.range_node_idx).property;
                auto inner_idx = range_segment_find(neighbor + vertex.neighbor_offset, degree, dest);
                if(inner_idx == RANGE_LEAF_SIZE) {
                    abort();
                    return;
                }
                auto outer_idx = vertex.neighbor_offset + inner_idx;

                RangePropertyVec_t* new_prop_map = nullptr;
                if(old_prop_map) {
                    new_prop_map = trace_block->allocate_range_prop_vec();
                    std::copy(old_prop_map->value.begin(), old_prop_map->value.begin() + arr_size, new_prop_map->value.begin());
                    this->next->resources->emplace_back(GCResourceInfo{Range_Property_Vec, (void*) old_prop_map});
                } else {
                    new_prop_map = trace_block->allocate_range_prop_vec();
                }

                map_set_range_property(new_prop_map, outer_idx, property_id, property);
                node_block->at(vertex.range_node_idx).property = new_prop_map;
                break;
            }
            case 1: {
                auto vertex_range_tree = (RangeTree*) vertex.neighborhood_ptr;
                auto new_range_tree = copy_range_tree(vertex_range_tree);
                this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
                new_range_tree->set_property(dest, property_id, property, *this->next->resources, trace_block);
                vertex.neighborhood_ptr = (uint64_t)new_range_tree;
                break;
            }
            case 2: {
                auto vertex_art = (ART*) vertex.neighborhood_ptr;
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
                auto res = vertex_art->set_property(dest, property_id, property, trace_block);
                if(res == nullptr) {
                    this->next->resources->pop_back();
                    return;
                }
                vertex.neighborhood_ptr = (uint64_t)res;
                break;
            }
            default: {
                std::cerr << "NeoTreeVersion::set_edge_property(): Invalid storage type" << std::endl;
                abort();
            }
        }
    }

    void NeoTreeVersion::set_edge_string_property(uint64_t src, uint64_t dest, uint8_t property_id, std::string&& property) {
        // TODO
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    void NeoTreeVersion::set_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, Property_t* properties) {
        assert(false);
        NeoVertex vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        uint64_t degree = vertex.degree;
        auto neighbor = (RangeElement*)vertex.neighbor_ptr;
        if(degree == 0) {
            return;
        }

        if(degree < RANGE_LEAF_SIZE / 2) {
            auto inner_seg_idx = range_segment_find(neighbor, degree, RangeElement{src, dest});
            if(inner_seg_idx == RANGE_LEAF_SIZE) {
                return;
            }
            uint16_t outer_seg_idx = neighbor - (RangeElement*)node_block->at(vertex.range_node_idx).arr_ptr + inner_seg_idx;

            for(auto i = 0; i < property_ids->size(); i++) {
//                node_block->at(vertex.range_node_idx).property[property_ids->at(i)]->at(outer_seg_idx) = properties[i];
            }
        } else {
            ((ART*) neighbor)->set_properties(dest, property_ids, properties);
        }
    }
#endif

    void NeoTreeVersion::gc_copied(WriterTraceBlock* trace_block) {
        // handle gc resources
        for(int idx = 0; idx < resources->size(); idx++) {
            auto res = resources->at(idx);
            switch(res.type) {
                case Outer_Segment: {
                    trace_block->deallocate_range_element_segment((RangeElementSegment_t*)res.ptr);
                    break;
                }
                case Inner_Segment: {
                    trace_block->deallocate_range_element_segment((RangeElementSegment_t*)res.ptr);
                    break;
                }
                case Range_Tree_Copied: {
                    delete (RangeTree*)res.ptr;
                    break;
                }
                case Range_Tree_Upgraded: {
                    for(int i = 0; i < ((RangeTree*)res.ptr)->node_block.size(); i++) {
                        auto node = ((RangeTree*)res.ptr)->node_block.at(i);
                        trace_block->deallocate_range_element_segment((RangeElementSegment_t*)node.arr_ptr);
#if EDGE_PROPERTY_NUM != 0
                        trace_block->deallocate_range_prop_vec(node.property_map);
#endif
                    }
                    delete (RangeTree*)res.ptr;
                    break;
                }
                case ART_Tree: {
                    auto art = (ART*)res.ptr;
                    art->handle_resources_copied(trace_block);
//                    art->gc_copied();
                    delete (ART *) res.ptr;
                    break;
                }
#if VERTEX_PROPERTY_NUM != 0
                case Vertex_Property_Vec: {
                    deallocate_vertex_property_vec((VertexPropertyVec_t*)res.ptr);
                    break;
                }
                case Vertex_Property_Map_All_Modified: {
                    gc_vertex_property_map_copied(res.ptr);
                    break;
                }
                case Multi_Vertex_Property_Vec_Copied: {
                    delete (MultiVertexPropertyVec_t*)res.ptr;
                    break;
                }
                case Multi_Vertex_Property_Vec_Mounted: {
                    break;
                }
#endif
#if EDGE_PROPERTY_NUM != 0
                case Range_Property_Vec: {
                    trace_block->deallocate_range_prop_vec((RangePropertyVec_t*)res.ptr);
                    break;
                }
                case Range_Property_Map_All_Modified: {
                    trace_block->deallocate_range_prop_vec((RangePropertyVec_t*)res.ptr);
                    break;
                }
#endif
                default:
                    std::cerr << "Invalid type type" << std::endl;
                    abort();
            }
        }

        delete resources;
        resources = nullptr;
    }

    void NeoTreeVersion::handle_resources_ref() {
        if(!resource_handled) {
            resource_handled = true;
#if VERTEX_PROPERTY_NUM >= 1
            vertex_property_map->ref_cnt += 1;
#endif
            if (!node_block->empty()) {
                if (node_block->at(0).arr_ptr != 0) {
                    for (auto node: *node_block) {
                        auto arr = (RangeElementSegment_t *) node.arr_ptr;
                        arr->ref_cnt += 1;
#if EDGE_PROPERTY_NUM >= 1
                        auto prop_arr = node.property;
                        if(prop_arr) {
                            prop_arr->ref_cnt += 1;
                        }
#endif
                    }
                }
            }
            auto add_ref = [&](uint16_t idx) {
                if (vertex_map->at(idx).is_art) {
                    ((ART *) vertex_map->at(idx).neighborhood_ptr)->ref_cnt += 1;
                } else {
                    ((RangeTree *) vertex_map->at(idx).neighborhood_ptr)->ref_cnt += 1;
                }
            };
            independent_map.for_each(add_ref);
        }

        if(resources->empty()) {
            return;
        }

        for(GCResourceInfo& res: *resources) {
            switch(res.type) {
                case Outer_Segment: {
                    ((RangeElementSegment_t *) res.ptr)->ref_cnt -= 1;
                    assert(((RangeElementSegment_t *) res.ptr)->ref_cnt != 0);
                    break;
                }
                case Inner_Segment: {
                    ((RangeElementSegment_t*)res.ptr)->ref_cnt -= 1;
                    assert(((RangeElementSegment_t*)res.ptr)->ref_cnt != 0);
                    break;
                }
                case Range_Tree_Copied: {
                    auto range_tree = (RangeTree*)res.ptr;
                    range_tree->ref_cnt -= 1;
                    assert(range_tree->ref_cnt != 0);
                    for(int i = 0; i < range_tree->node_block.size(); i++) {
                        ((RangeElementSegment_t*)range_tree->node_block.at(i).arr_ptr)->ref_cnt += 1;
#if EDGE_PROPERTY_NUM != 0
                        if(range_tree->node_block.at(i).property_map) {
                            range_tree->node_block.at(i).property_map->ref_cnt += 1;
                        }
#endif
                    }
                    break;
                }
                case Range_Tree_Upgraded: {
                    auto range_tree = (RangeTree*)res.ptr;
                    range_tree->ref_cnt -= 1;
                    break;
                }
                case ART_Tree: {
                    ((ART *) res.ptr)->handle_resources_ref();
                    ((ART *) res.ptr)->ref_cnt -= 1;
                    break;
                }
#if VERTEX_PROPERTY_NUM != 0
                case Vertex_Property_Vec: {
                    ((VertexPropertyVec_t*)res.ptr)->ref_cnt -= 1;
                    break;
                }
                case Vertex_Property_Map_All_Modified: {
#if VERTEX_PROPERTY_NUM > 1
                    auto map = (MultiVertexPropertyVec_t *)res.ptr;
#else
                    auto map = (VertexPropertyVec_t*)res.ptr;
#endif
                    map->ref_cnt -= 1;
                    break;
                }
                case Multi_Vertex_Property_Vec_Copied: {
                    auto map = (MultiVertexPropertyVec_t*)res.ptr;
                    for(auto& vec: map->properties) {
                        vec->ref_cnt += 1;
                    }
                    map->ref_cnt -= 1;
                    break;
                }
                case Multi_Vertex_Property_Vec_Mounted: {
                    auto map = (MultiVertexPropertyVec_t*)res.ptr;
                    map->ref_cnt + 1;
                    break;
                }
#endif
#if EDGE_PROPERTY_NUM != 0
                case Range_Property_Vec: {
                    ((RangePropertyVec_t*)res.ptr)->ref_cnt -= 1;
                    break;
                }
                case Range_Property_Map_All_Modified: {
#if EDGE_PROPERTY_NUM > 1
                    auto map = (MultiRangePropertyVec_t *)res.ptr;
#else
                    auto map = (RangePropertyVec_t*)res.ptr;
#endif
                    map->ref_cnt -= 1;
                    break;
                }
#endif
                default:
                    std::cerr << "Invalid type type" << std::endl;
                    abort();
            }
        }
        resources->clear();
    }

    void NeoTreeVersion::gc_ref(WriterTraceBlock* trace_block) {
        // deref
#if VERTEX_PROPERTY_NUM != 0
        gc_vertex_property_map_ref(vertex_property_map);
#endif

        if(node_block->at(0).arr_ptr != 0) {
            for (auto node: *node_block) {
                auto arr = (RangeElementSegment_t *) node.arr_ptr;
                if(arr->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                    trace_block->deallocate_range_element_segment(arr);
#if EDGE_PROPERTY_NUM != 0
                    trace_block->deallocate_range_prop_vec(node.property);
#endif
                }
            }
        }
        auto dec_ref = [&](uint16_t idx) {
            if(vertex_map->at(idx).is_art) {
                auto art = (ART*)vertex_map->at(idx).neighborhood_ptr;
                if(art->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                    art->gc_ref(trace_block);
                    delete art;
                }
            } else {
                auto range_tree = (RangeTree*)vertex_map->at(idx).neighborhood_ptr;
                if(range_tree->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                    for(int i = 0; i < range_tree->node_block.size(); i++) {
                        auto arr = (RangeElementSegment_t*)range_tree->node_block.at(i).arr_ptr;
                        if(arr->ref_cnt.fetch_sub(1, std::memory_order_release) == 1) {
                            trace_block->deallocate_range_element_segment((RangeElementSegment_t*)arr);
#if EDGE_PROPERTY_NUM != 0
                            trace_block->deallocate_range_prop_vec(range_tree->node_block.at(i).property_map);
#endif
                        }
                    }
                    delete range_tree;
                }
            }
        };
        independent_map.for_each(dec_ref);
    }

    void NeoTreeVersion::destroy() {
#if VERTEX_PROPERTY_NUM != 0
        destroy_vertex_property_map(vertex_property_map);
#endif

        for(auto & it : *vertex_map) {
            if(it.is_independent) {
                if(!it.is_art) {
                    auto tree = (RangeTree*)it.neighborhood_ptr;
                    for(int i = 0; i < tree->node_block.size(); i++) {
                        delete ((RangeElementSegment_t*)tree->node_block.at(i).arr_ptr);
#if EDGE_PROPERTY_NUM != 0
                        delete (tree->node_block.at(i).property_map);
#endif
                    }
                    delete (RangeTree*)it.neighborhood_ptr;
                    it.neighborhood_ptr = 0;
                    it.range_node_idx = 0;
                } else {
                    ((ART*)it.neighborhood_ptr)->destroy();
                    delete ((ART*)it.neighborhood_ptr);
                    it.neighborhood_ptr = 0;
                    it.range_node_idx = 0;
                }
            }
        }

        // delete block
        for(auto & node : *node_block) {
            delete ((RangeElementSegment_t*)node.arr_ptr);
            node.arr_ptr = 0;
#if EDGE_PROPERTY_NUM != 0
            delete (node.property);
            node.property = nullptr;
#endif
        }

        delete resources;
        resources = nullptr;

        delete node_block;
        node_block = nullptr;
        delete vertex_map;
        vertex_map = nullptr;
    }

    // TODO need to fix GC
    void NeoTreeVersion::append_new_list(uint64_t cur_node_num, std::vector<NeoRangeNode> &new_nodes, const std::pair<RangeElement, RangeElement>* edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        uint64_t new_edge_st = 0;
        uint64_t new_edge_ed = 0;

        uint64_t new_segment_size = 0;
        auto new_segment = trace_block->allocate_range_element_segment();
#if EDGE_PROPERTY_NUM > 0
        auto new_prop_segment = trace_block->allocate_range_prop_vec();
#endif
        uint16_t cur_least_vertex = edges[0].first & VERTEX_GROUP_MASK;

        auto move_to_next_node = [&]() {
            assert(new_segment_size != 0);
            new_nodes.emplace_back(NeoRangeNode{cur_least_vertex, new_segment_size - 1, (uint64_t)new_segment, new_prop_segment});
            new_segment_size = 0;

            new_segment = trace_block->allocate_range_element_segment();
#if EDGE_PROPERTY_NUM > 0
            new_prop_segment = trace_block->allocate_range_prop_vec();
#endif
        };

        while(new_edge_ed < count) {
            uint8_t cur_vertex = edges[new_edge_st].first & VERTEX_GROUP_MASK;
            auto &vertex = vertex_map->at(cur_vertex);
            vertex.exist = true;
            while (new_edge_ed < count && (edges[new_edge_ed].first & VERTEX_GROUP_MASK) == cur_vertex) {
                new_edge_ed++;
            }

            // check if the new edges are needed to be inserted into an independent tree
            if (vertex.is_art) {    // To ART
                auto vertex_art = (ART *) vertex.neighborhood_ptr;
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
                ARTInsertElemBatchRes res = vertex_art->insert_element_batch(edges + new_edge_st,
                                                                             properties + new_edge_st,
                                                                             new_edge_ed - new_edge_st, trace_block);

                vertex.neighborhood_ptr = (uint64_t) res.art_ptr;
                vertex.degree += res.new_inserted;

                new_edge_st = new_edge_ed;
                continue;
            } else if (vertex.degree >= RANGE_LEAF_SIZE / 2) {  // To RangeTree
                auto vertex_range_tree = (RangeTree *) vertex.neighborhood_ptr;

                RangeTreeInsertElemBatchRes res{};
                if (vertex.degree + new_edge_ed - new_edge_st >= ART_EXTRACT_THRESHOLD) {
                    this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Upgraded, (void*) vertex_range_tree});
                    res = vertex_range_tree->range_tree2art_batch(cur_vertex, vertex.degree, edges + new_edge_st,
                                                                  properties + new_edge_st,
                                                                  new_edge_ed - new_edge_st,
                                                                  *this->next->resources, trace_block);
                    vertex.is_art = true;
                } else {
                    this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
                    res = vertex_range_tree->insert_element_batch(cur_vertex, edges + new_edge_st,
                                                                  properties + new_edge_st,
                                                                  new_edge_ed - new_edge_st,
                                                                  *this->next->resources, trace_block);
                }
                vertex.is_independent = true;
                vertex.neighborhood_ptr = (uint64_t) res.tree_ptr;
                vertex.degree += res.new_inserted;

                new_edge_st = new_edge_ed;
                continue;
            }


            // check if there is need to extract to an independent tree
            if (new_edge_ed - new_edge_st >= RANGE_LEAF_SIZE / 2) {
                // Extract to an independent tree
                std::vector<RangeElement> new_edges;
                new_edges.reserve(new_edge_ed - new_edge_st);
#if EDGE_PROPERTY_NUM > 0
                std::vector<Property_t*> new_props;
                new_props.reserve(new_edge_ed - new_edge_st);
#endif
                while (new_edge_st < new_edge_ed) {
                    new_edges.push_back(edges[new_edge_st].second);
#if EDGE_PROPERTY_NUM > 0
                    new_props.push_back(properties[new_edge_st]);
#endif
                    new_edge_st++;
                }

                void *new_tree = nullptr;
                if (new_edges.size() >= ART_EXTRACT_THRESHOLD) {    // To ART
                    new_tree = new ART();
                    delete ((ART*)new_tree)->root;
#if EDGE_PROPERTY_NUM > 0
                    batch_subtree_build<false>(&((ART *) new_tree)->root, 0, new_edges.data(), new_props.data(), new_edges.size(), trace_block);
#else
                    batch_subtree_build<false>(&((ART *) new_tree)->root, 0, new_edges.data(), nullptr, new_edges.size(), trace_block);
#endif
                    vertex.is_art = true;
                } else {    // To RangeTree
#if EDGE_PROPERTY_NUM > 0
                    new_tree = new RangeTree(new_edges, new_props.data(), new_edges.size(), trace_block);
#else
                    new_tree = new RangeTree(new_edges, nullptr, new_edges.size(), trace_block);
#endif
                }
                vertex.is_independent = true;
                vertex.neighborhood_ptr = (uint64_t) new_tree;
                vertex.degree = new_edges.size();
                vertex.neighbor_offset = 0;
                vertex.range_node_idx = 0;
                independent_map.set(cur_vertex);

                continue;
            }

            // insert the new edges into the current segment
            if (new_edge_ed - new_edge_st + new_segment_size >= RANGE_LEAF_SIZE) {
                move_to_next_node();
                cur_least_vertex = edges[new_edge_st].first & VERTEX_GROUP_MASK;
            }

            vertex.neighborhood_ptr = (uint64_t) new_segment;
            vertex.neighbor_offset = new_segment_size;
            vertex.range_node_idx = cur_node_num + new_nodes.size();
            vertex.degree += new_edge_ed - new_edge_st;
            while (new_edge_st < new_edge_ed) {
                range_segment_append(new_segment, new_prop_segment, new_segment_size, edges[new_edge_st].second, properties[new_edge_st]);
                new_edge_st++;
                new_segment_size++;
            }
        }

        // mount the last segment
        if (new_segment_size != 0) {
            new_nodes.emplace_back(NeoRangeNode{cur_least_vertex, new_segment_size - 1, (uint64_t)new_segment, nullptr});
            force_pointer_set(&(new_nodes.back().property), new_prop_segment);
        } else {
            trace_block->deallocate_range_element_segment(new_segment);
            trace_block->deallocate_range_prop_vec(new_prop_segment);
        }
    }

    // TODO handle the degree
    // TODO handle the vertices
    // TODO GC
    void NeoTreeVersion::node_insert_edge_batch(uint16_t node_idx, std::vector<NeoRangeNode> &new_nodes, uint64_t cur_node_num, const std::pair<RangeElement, RangeElement>* edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
//        if(((*edges).first >> VERTEX_GROUP_BITS) != (482 >> VERTEX_GROUP_BITS)) {
//            return;
//        }
#ifndef NDEBUG
        if((*edges).first == 482 && (*edges).second == 1953700) {
            std::cout << "debug" << std::endl;
        }
#endif
        std::vector< std::future<bool> > results;
        auto node = node_block->at(node_idx);
        auto old_segment = (RangeElementSegment_t*)node.arr_ptr;
        uint64_t old_edge_st = 0;
        uint64_t old_edge_ed = 0;
        uint64_t old_edge_count = old_segment ? node.size + 1 : 0;

        uint64_t new_edge_st = 0;
        uint64_t new_edge_ed = 0;

        uint64_t new_segment_size = 0;
        auto new_segment = trace_block->allocate_range_element_segment();

        auto old_prop_segment = node.property;
        auto new_prop_segment = trace_block->allocate_range_prop_vec();

        auto vertices = get_vertices_in_node_with_offset(node_idx);
#ifndef NDEBUG
        if(!vertices->empty()) {
            for(int i = 0; i < vertices->size() - 1; i++) {
                assert(vertices->at(i).first < vertices->at(i + 1).first);
                assert(vertices->at(i).second < vertices->at(i + 1).second);
            }
        }
#endif
        auto vertex_idx = 0;
        auto cur_vertices = new std::vector<std::pair<uint16_t, uint16_t>>();

        auto move_to_next_node = [&]() {
            auto next_segment = trace_block->allocate_range_element_segment();
            auto next_prop_segment = trace_block->allocate_range_prop_vec();

            // find mid_count
            uint16_t split_vtx_idx = 0;
            while(split_vtx_idx < cur_vertices->size() - 1) {
                split_vtx_idx += 1;
                auto cur_ver = cur_vertices->at(split_vtx_idx);
                if(cur_ver.second >= RANGE_LEAF_SIZE / 2) {
                    break;
                }
            }
            auto split_vertex = cur_vertices->at(split_vtx_idx);

            uint64_t split_idx = split_vertex.second;
            // bind vertices
            for(uint16_t i = 0; i < split_vtx_idx; i++) {
                auto& vtx = vertex_map->at(cur_vertices->at(i).first);
                vtx.neighborhood_ptr = (uint64_t) new_segment;
                vtx.neighbor_offset = cur_vertices->at(i).second;
                vtx.range_node_idx = cur_node_num + new_nodes.size();
            }
            new_nodes.emplace_back(NeoRangeNode{cur_vertices->at(0).first, split_idx - 1, (uint64_t) new_segment,
                                                new_prop_segment});

            std::move(new_segment->value.begin() + split_idx, new_segment->value.begin() + new_segment_size, next_segment->value.begin());
            range_property_map_copy(new_prop_segment, split_idx, new_segment_size, next_prop_segment, 0);
            for(uint16_t i = split_vtx_idx; i < cur_vertices->size(); i++) {
                cur_vertices->at(i).second -= split_idx;
                cur_vertices->at(i - split_vtx_idx) = cur_vertices->at(i);
            }
            cur_vertices->resize(cur_vertices->size() - split_vtx_idx);
            new_segment_size = new_segment_size - split_idx;
            new_segment = next_segment;
            new_prop_segment = next_prop_segment;
        };

        auto insert_to_independent = [&] (uint8_t vertex, uint64_t count) {
            if(!vertex_map->at(vertex).is_art) {
                auto vertex_range_tree = (RangeTree *) vertex_map->at(vertex).neighborhood_ptr;
                if (vertex_map->at(vertex).degree + new_edge_ed - new_edge_st >= ART_EXTRACT_THRESHOLD) {
                    this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Upgraded, (void*) vertex_range_tree});
                    RangeTreeInsertElemBatchRes res = vertex_range_tree->range_tree2art_batch(vertex, vertex_map->at(vertex).degree,
                                                                  edges + new_edge_st,
                                                                  properties + new_edge_st,
                                                                  new_edge_ed - new_edge_st,
                                                                  *this->next->resources, trace_block);
                    vertex_map->at(vertex).is_art = true;
                    vertex_map->at(vertex).neighborhood_ptr = (uint64_t) res.tree_ptr;
                    vertex_map->at(vertex).degree += res.new_inserted;
                } else {
                    this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
                    RangeTreeInsertElemBatchRes res = vertex_range_tree->insert_element_batch(vertex, edges + new_edge_st,
                                                                                              properties + new_edge_st,
                                                                                              new_edge_ed - new_edge_st,
                                                                                              *this->next->resources, trace_block);
                    vertex_map->at(vertex).neighborhood_ptr = (uint64_t) res.tree_ptr;
                    vertex_map->at(vertex).degree += res.new_inserted;
                }
            } else {
                auto vertex_art = (ART *) vertex_map->at(vertex).neighborhood_ptr;
                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
                ARTInsertElemBatchRes res = vertex_art->insert_element_batch(edges + new_edge_st,
                                                                     properties + new_edge_st,
                                                                     new_edge_ed - new_edge_st, trace_block);
                vertex_map->at(vertex).neighborhood_ptr = (uint64_t) res.art_ptr;
                vertex_map->at(vertex).degree += res.new_inserted;
            }
        };

//        auto insert_to_independent = [&] (uint8_t vertex, uint64_t count) {
//            if(!vertex_map->at(vertex).is_art) {
//                auto vertex_range_tree = (RangeTree *) vertex_map->at(vertex).neighborhood_ptr;
//                if (vertex_map->at(vertex).degree + new_edge_ed - new_edge_st >= ART_EXTRACT_THRESHOLD) {
//                    this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Upgraded, (void*) vertex_range_tree});
////                    results.emplace_back(
////                        pool->enqueue([new_edge_st, new_edge_ed, edges, properties, this, vertex_range_tree, vertex](uint64_t thread_id) {
//                    RangeTreeInsertElemBatchRes res = vertex_range_tree->range_tree2art_batch(vertex, vertex_map->at(vertex).degree,
//                                                                                              edges + new_edge_st,
//                                                                                              properties + new_edge_st,
//                                                                                              new_edge_ed - new_edge_st,
//                                                                                              *this->next->resources, get_trace_block(thread_id));
//                    vertex_map->at(vertex).is_art = true;
//                    vertex_map->at(vertex).neighborhood_ptr = (uint64_t) res.tree_ptr;
//                    vertex_map->at(vertex).degree += res.new_inserted;
////                            return true;
////                        })
////                    );
//                } else {
//                    this->next->resources->emplace_back(GCResourceInfo{Range_Tree_Copied, (void*) vertex_range_tree});
////                    results.emplace_back(
////                        pool->enqueue([new_edge_st, new_edge_ed, edges, properties, this, vertex_range_tree, vertex](uint64_t thread_id) {
//                    RangeTreeInsertElemBatchRes res = vertex_range_tree->insert_element_batch(vertex, edges + new_edge_st,
//                                                                                              properties + new_edge_st,
//                                                                                              new_edge_ed - new_edge_st,
//                                                                                              *this->next->resources, get_trace_block(thread_id));
//                    vertex_map->at(vertex).neighborhood_ptr = (uint64_t) res.tree_ptr;
//                    vertex_map->at(vertex).degree += res.new_inserted;
////                            return true;
////                        })
////                    );
//                }
//            } else {
//                auto vertex_art = (ART *) vertex_map->at(vertex).neighborhood_ptr;
//                this->next->resources->emplace_back(GCResourceInfo{ART_Tree, (void*) vertex_art});
////                results.emplace_back(
////                        pool->enqueue([new_edge_st, new_edge_ed, edges, properties, this, vertex_art, vertex](uint64_t thread_id) {
//                ARTInsertElemBatchRes res = vertex_art->insert_element_batch(edges + new_edge_st,
//                                                                             properties + new_edge_st,
//                                                                             new_edge_ed - new_edge_st, get_trace_block(thread_id));
//                vertex_map->at(vertex).neighborhood_ptr = (uint64_t) res.art_ptr;
//                vertex_map->at(vertex).degree += res.new_inserted;
////                        return true;
////                    })
////                );
//            }
//        };

        while(new_edge_ed < count && old_edge_ed < old_edge_count) {
            uint16_t new_vertex = edges[new_edge_st].first & VERTEX_GROUP_MASK;
            uint16_t old_vertex = vertices->at(vertex_idx).first;

            if(new_vertex > old_vertex) {
                old_edge_ed += vertex_map->at(old_vertex).degree;
                assert(old_edge_ed <= old_edge_count);
                if(new_segment_size + old_edge_ed - old_edge_st >= RANGE_LEAF_SIZE) {
                    move_to_next_node();
                }
                std::copy(old_segment->value.begin() + old_edge_st, old_segment->value.begin() + old_edge_ed, new_segment->value.begin() + new_segment_size);
                range_property_map_copy(old_prop_segment, old_edge_st, old_edge_ed, new_prop_segment, new_segment_size);
                cur_vertices->emplace_back(old_vertex, new_segment_size);
                new_segment_size += old_edge_ed - old_edge_st;
                old_edge_st = old_edge_ed;
                vertex_idx++;
                continue;
            } else if(new_vertex < old_vertex) {
                while (new_edge_ed < count && (edges[new_edge_ed].first & VERTEX_GROUP_MASK) == new_vertex) {
                    new_edge_ed++;
                }
                if(!vertex_map->at(new_vertex).is_independent) {
                    vertex_map->at(new_vertex).exist = true;
                    vertex_map->at(new_vertex).degree = new_edge_ed - new_edge_st;
                    if (new_segment_size + new_edge_ed - new_edge_st >= RANGE_LEAF_SIZE) {
                        move_to_next_node();
                    }
                    cur_vertices->emplace_back(new_vertex, new_segment_size);
                    while (new_edge_st < new_edge_ed) {
                        new_segment->value.at(new_segment_size) = edges[new_edge_st].second;
                        map_set_sa_range_property(new_prop_segment, new_segment_size, properties[new_edge_st]);
                        new_segment_size++;
                        new_edge_st++;
                    }
                } else {
                    insert_to_independent(new_vertex, new_edge_ed - new_edge_st);
                    new_edge_st = new_edge_ed;
                }
                continue;
            } else {
                uint16_t cur_vertex = new_vertex;
                auto &vertex = vertex_map->at(cur_vertex);
                while (new_edge_ed < count && (edges[new_edge_ed].first & VERTEX_GROUP_MASK) == cur_vertex) {
                    new_edge_ed++;
                }
                uint64_t vertex_new_edge_count = new_edge_ed - new_edge_st;

                old_edge_ed += vertex.degree;
                vertex_idx++;   // move to next old vertex at first
                assert(old_edge_ed <= old_edge_count);
                uint64_t total_count = vertex.degree + vertex_new_edge_count;

                // check if there is need to extract to an independent tree
                if (total_count >= RANGE_LEAF_SIZE / 2) {
                    // Extract to an independent tree
                    std::vector<RangeElement> new_edges;
                    new_edges.reserve(total_count);
                    auto cur_old_edge = (RangeElement *) vertex.neighborhood_ptr + vertex.neighbor_offset;
                    auto old_edge_end = cur_old_edge + vertex.degree;
                    uint64_t old_dst = *cur_old_edge;
                    void *new_tree = nullptr;
                    std::vector<Property_t*> new_edge_properties;
                    new_edges.reserve(total_count);
                    while (cur_old_edge < old_edge_end && new_edge_st < new_edge_ed) {
                        if (old_dst < edges[new_edge_st].second) {
                            new_edges.emplace_back(old_dst);
                            new_edge_properties.push_back(map_get_all_range_property(old_prop_segment, cur_old_edge - old_segment->value.begin()));
                            cur_old_edge++;
                            old_dst = *cur_old_edge;
                        } else if (old_dst > edges[new_edge_st].second) {
                            new_edges.emplace_back(edges[new_edge_st].second);
                            new_edge_properties.push_back(properties[new_edge_st]);
                            new_edge_st++;
                        } else {
                            new_edges.emplace_back(edges[new_edge_st].second);
                            new_edge_properties.push_back(properties[new_edge_st]);
                            new_edge_st++;
                            cur_old_edge++;
                            old_dst = *cur_old_edge;
                        }
                    }

                    while (cur_old_edge < old_edge_end) {
                        new_edges.emplace_back(*cur_old_edge);
                        new_edge_properties.push_back(map_get_all_range_property(old_prop_segment, cur_old_edge - old_segment->value.begin()));
                        cur_old_edge++;
                    }
                    while (new_edge_st < new_edge_ed) {
                        new_edges.emplace_back(edges[new_edge_st].second);
                        new_edge_properties.push_back(properties[new_edge_st]);
                        new_edge_st++;
                    }

                    if (new_edges.size() >= ART_EXTRACT_THRESHOLD) {    // To ART
                        new_tree = new ART();
                        delete ((ART*)new_tree)->root;
                        batch_subtree_build(&((ART *) new_tree)->root, 0, new_edges.data(), new_edge_properties.data(), new_edges.size(), trace_block);
                        vertex.is_art = true;
                    } else {    // To RangeTree
                        new_tree = new RangeTree(new_edges, new_edge_properties.data(), new_edges.size(), trace_block);
                    }

                    vertex.is_independent = true;
                    vertex.neighborhood_ptr = (uint64_t) new_tree;
                    vertex.range_node_idx = 0;
                    vertex.neighbor_offset = 0;
                    vertex.degree = new_edges.size();
                    independent_map.set(cur_vertex);

                    // update pointers
                    old_edge_st = old_edge_ed;

                    continue;
                }
                if (total_count + new_segment_size >= RANGE_LEAF_SIZE) {
                    move_to_next_node();
                }

                vertex.degree = total_count;
                cur_vertices->emplace_back(cur_vertex, new_segment_size);
                while (old_edge_st < old_edge_ed && new_edge_st < new_edge_ed) {
                    if (edges[new_edge_st].second < old_segment->value.at(old_edge_st)) {
                        range_segment_set(new_segment, new_prop_segment, new_segment_size, edges[new_edge_st].second, properties[new_edge_st]);
                        new_edge_st++;
                    } else if (edges[new_edge_st].second > old_segment->value.at(old_edge_st)) {
                        range_segment_set(new_segment, new_prop_segment, new_segment_size, old_segment->value.at(old_edge_st), map_get_all_range_property(old_prop_segment, old_edge_st));
                        old_edge_st++;
                    } else {
                        range_segment_set(new_segment, new_prop_segment, new_segment_size, edges[new_edge_st].second, properties[new_edge_st]);
                        old_edge_st++;
                        new_edge_st++;
                        vertex.degree--;
                    }
                    new_segment_size++;
                }
                while (old_edge_st < old_edge_ed) {
                    range_segment_set(new_segment, new_prop_segment, new_segment_size, old_segment->value.at(old_edge_st), map_get_all_range_property(old_prop_segment, old_edge_st));
                    old_edge_st++;
                    new_segment_size++;
                }
                while (new_edge_st < new_edge_ed) {
                    range_segment_set(new_segment, new_prop_segment, new_segment_size, edges[new_edge_st].second, properties[new_edge_st]);
                    new_edge_st++;
                    new_segment_size++;
                }

            }
        }

        // Handle the remaining edges
        while (new_edge_ed < count) {
            assert(new_edge_st == new_edge_ed);
            uint8_t cur_vertex = edges[new_edge_st].first & VERTEX_GROUP_MASK;
            auto &vertex = vertex_map->at(cur_vertex);
            vertex.exist = true;
            while (new_edge_ed < count && (edges[new_edge_ed].first & VERTEX_GROUP_MASK) == cur_vertex) {
                new_edge_ed++;
            }

            // check if the new edges are needed to be inserted into an independent tree
            if(vertex.is_independent) {
                insert_to_independent(cur_vertex, new_edge_ed - new_edge_st);
                new_edge_st = new_edge_ed;
                continue;
            }
            if (new_edge_ed - new_edge_st >= RANGE_LEAF_SIZE / 2) {
                // Extract to an independent tree
                std::vector<RangeElement> new_edges;
                new_edges.reserve(new_edge_ed - new_edge_st);
                while(new_edge_st < new_edge_ed) {
                    new_edges.push_back(edges[new_edge_st].second);
                    new_edge_st++;
                }

                void *new_tree = nullptr;
                if (new_edges.size() >= ART_EXTRACT_THRESHOLD) {    // To ART
                    new_tree = new ART();
                    delete ((ART*)new_tree)->root;
                    batch_subtree_build<true>(&((ART *) new_tree)->root, 0, new_edges.data(), properties + new_edge_st, new_edges.size(), trace_block);
                    vertex.is_art = true;
                } else {    // To RangeTree
                    new_tree = new RangeTree(new_edges, properties + new_edge_st, new_edges.size(), trace_block);
                }
                vertex.is_independent = true;
                vertex.neighborhood_ptr = (uint64_t) new_tree;
                vertex.degree = new_edges.size();
                vertex.range_node_idx = 0;
                vertex.neighbor_offset = 0;
                independent_map.set(cur_vertex);

                continue;
            }
            if (new_edge_ed - new_edge_st + new_segment_size >= RANGE_LEAF_SIZE) {
                move_to_next_node();
            }

            cur_vertices->emplace_back(cur_vertex, new_segment_size);
            vertex.degree = new_edge_ed - new_edge_st;
            while (new_edge_st < new_edge_ed) {
                range_segment_set(new_segment, new_prop_segment, new_segment_size, edges[new_edge_st].second, properties[new_edge_st]);
                new_edge_st++;
                new_segment_size++;
            }
        }
        while (old_edge_ed < old_edge_count) {
            uint8_t cur_vertex = vertices->at(vertex_idx).first;
            vertex_idx++;
            old_edge_ed += vertex_map->at(cur_vertex).degree;
            assert(old_edge_ed <= old_edge_count);
            if (new_segment_size + old_edge_ed - old_edge_st >= RANGE_LEAF_SIZE) {
                move_to_next_node();
            }

            cur_vertices->emplace_back(cur_vertex, new_segment_size);
            std::copy(old_segment->value.begin() + old_edge_st, old_segment->value.begin() + old_edge_ed,
                      new_segment->value.begin() + new_segment_size);
            range_property_map_copy(old_prop_segment, old_edge_st, old_edge_ed, new_prop_segment, new_segment_size);
            new_segment_size += old_edge_ed - old_edge_st;
            old_edge_st = old_edge_ed;
        }

        // mount the last segment
        if (new_segment_size != 0) {
            // bind vertices
            for(uint16_t i = 0; i < cur_vertices->size(); i++) {
                auto& vtx = vertex_map->at(cur_vertices->at(i).first);
                vtx.neighborhood_ptr = (uint64_t) new_segment;
                vtx.neighbor_offset = cur_vertices->at(i).second;
                vtx.range_node_idx = cur_node_num + new_nodes.size();
            }
            new_nodes.emplace_back(NeoRangeNode{cur_vertices->at(0).first, new_segment_size - 1, (uint64_t)new_segment, new_prop_segment});
        } else {
            trace_block->deallocate_range_element_segment(new_segment);
            trace_block->deallocate_range_prop_vec(new_prop_segment);
        }

        for(auto && result: results) {
            result.get();
        }
#ifndef NDEBUG
        if(!cur_vertices->empty()) {
            for(int i = 0; i < cur_vertices->size() - 1; i++) {
                assert(cur_vertices->at(i).first < cur_vertices->at(i + 1).first);
                assert(cur_vertices->at(i).second < cur_vertices->at(i + 1).second);
            }
        }
#endif
        delete cur_vertices;
        delete vertices;
    }

    void NeoTreeVersion::insert_edge_batch(const std::pair<RangeElement, RangeElement>* edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        if(count == 0 || edges == nullptr) {
            throw std::runtime_error("NeoTreeVersion::insert_edge_batch(): Invalid input");
        }
        auto new_node_block = new std::vector<NeoRangeNode>{};
        new_node_block->reserve(node_block->size());

        int64_t old_node_idx = 0;
        int64_t list_st = 0;
        int64_t list_ed = 0;
        auto new_nodes = new std::vector<NeoRangeNode>{};

        while(list_ed < count && old_node_idx < node_block->size()) {
            auto next_key = old_node_idx != node_block->size() - 1 ? node_block->at(old_node_idx + 1).key : std::numeric_limits<uint64_t>::max();
            if((edges[list_ed].first & VERTEX_GROUP_MASK) >= next_key) {
                new_node_block->push_back(node_block->at(old_node_idx));
                old_node_idx += 1;
                continue;
            }

            this->next->resources->emplace_back(GCResourceInfo{Outer_Segment, (void*) next->node_block->at(old_node_idx).arr_ptr});
//            std::cout << "collect:" << ((void*) next->node_block->at(old_node_idx).arr_ptr) << std::endl;
            if(next->node_block->at(old_node_idx).property) {
                this->next->resources->emplace_back(GCResourceInfo{Range_Property_Map_All_Modified,
                                                                   (void *) next->node_block->at(
                                                                           old_node_idx).property});
            }
            while (list_ed < count && (edges[list_ed].first & VERTEX_GROUP_MASK) < next_key) {
                list_ed += 1;
            }

            node_insert_edge_batch(old_node_idx, *new_nodes, new_node_block->size(), edges + list_st, properties + list_st, list_ed - list_st, trace_block);

            // insert new nodes in reverse order
            for(auto & new_node : *new_nodes) {
                new_node_block->push_back(new_node);
            }
            new_nodes->clear();

            // update pointers
            list_st = list_ed;
            old_node_idx += 1;
        }

        // Handle the remaining edges
        if (list_ed < count) {
            append_new_list(new_node_block->size(), *new_nodes, edges, properties, list_ed + 1, trace_block);
            for(auto & new_node : *new_nodes) {
                new_node_block->push_back(new_node);
            }
        }
        while(old_node_idx < node_block->size()) {
            new_node_block->push_back(node_block->at(old_node_idx));
            old_node_idx += 1;
        }

        // apply the new node block
        delete node_block;
        delete new_nodes;
        if(new_node_block->empty()) {
            new_node_block->push_back(NeoRangeNode{0, 0, 0, nullptr});
        }
        new_node_block->at(0).key = 0;
        node_block = new_node_block;
    }

}
