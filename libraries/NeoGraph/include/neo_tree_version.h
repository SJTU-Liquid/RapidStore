#pragma once

#include <fstream>
#include "neo_range_ops.h"
#include "neo_range_tree.h"
#include "../utils/thread_pool.h"
#include "../utils/types.h"
#include "../utils/config.h"
#include "neo_reader_trace.h"

namespace container {
    class NeoTreeVersion {
    public:
        RangeNodeSegment_t* node_block;
        NeoTreeVersion* next;
        uint64_t timestamp;
        VertexMap_t * vertex_map;
        Bitmap<INDEPENDENT_MAP_BLOCK_NUM> independent_map{};
        bool resource_handled{};
#if VERTEX_PROPERTY_NUM == 1
        VertexPropertyVec_t* vertex_property_map;
#elif VERTEX_PROPERTY_NUM > 1
        MultiVertexPropertyVec_t* vertex_property_map;
#endif
        std::atomic<uint64_t> ref_cnt{}; // mark the number of txns that reference this version, the version will be deleted when the version is not the latest && ref_cnt == 0
        std::vector<GCResourceInfo>* resources;

        // functions
        explicit NeoTreeVersion(NeoTreeVersion* , WriterTraceBlock* trace_block);
        void clean(WriterTraceBlock* trace_block);
        ~NeoTreeVersion();

        [[nodiscard]] bool has_vertex(uint64_t vertex) const;

        [[nodiscard]] bool has_edge(uint64_t src, uint64_t dest) const;

        [[nodiscard]] uint64_t get_degree(uint64_t vertex) const;

        [[nodiscard]] RangeElement* get_neighbor_addr(uint64_t vertex) const;

#if VERTEX_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_vertex_property(uint64_t vertex, uint8_t property_id) const;
#endif
#if VERTEX_PROPERTY_NUM > 1
        void get_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
#if EDGE_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const;
#endif
#if EDGE_PROPERTY_NUM > 1
        void get_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif

        ///@return false if vertex does not exist
        bool get_neighbor(uint64_t src, std::vector<uint64_t>& neighbor) const;

        [[nodiscard]] std::pair<uint64_t, uint64_t> get_filling_info() const;

        ///@return false if vertex does not exist
        template<typename F>
        void edges(uint64_t src, F&& callback) const;

        static void intersect(NeoTreeVersion* version1, uint64_t src1, NeoTreeVersion* version2, uint64_t src2, std::vector<uint64_t> &result);

        static uint64_t intersect(NeoTreeVersion* version1, uint64_t src1, NeoTreeVersion* version2, uint64_t src2);

        std::vector<uint16_t>* get_vertices_in_node(uint16_t node_idx);

        std::vector<std::pair<uint16_t, uint16_t>>* get_vertices_in_node_with_offset(uint16_t node_idx);

        void insert_vertex(uint64_t vertex, Property_t* property);

        void insert_vertex_batch(const uint64_t* vertices, Property_t ** properties, uint64_t count);

#if VERTEX_PROPERTY_NUM >= 1
        void set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property);

        void set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property);
#endif

#if VERTEX_PROPERTY_NUM > 1
        void set_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, Property_t* properties);
#endif

        void insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block);

        void second_insert_edge(uint64_t src, RangeElement target, NeoVertex& vertex, Property_t* property, WriterTraceBlock* trace_block);

        void append_new_list(uint64_t cur_node_num, std::vector<NeoRangeNode> &new_nodes, const std::pair<RangeElement, RangeElement>* edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

        void node_insert_edge_batch(uint16_t node_idx, std::vector<NeoRangeNode> &new_nodes, uint64_t cur_node_num, const std::pair<RangeElement, RangeElement>* edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

        void insert_edge_batch(const std::pair<RangeElement, RangeElement>* edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block);

#if EDGE_PROPERTY_NUM >= 1
        void set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property, WriterTraceBlock* trace_block);

        void set_edge_string_property(uint64_t src, uint64_t dest, uint8_t property_id, std::string&& property);
#endif

#if EDGE_PROPERTY_NUM > 1
        void set_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, Property_t* properties);
#endif

        void gc_copied(WriterTraceBlock* trace_block);

        void handle_resources_ref();

        void gc_ref(WriterTraceBlock* trace_block);

        void destroy();

        void remove_vertex(uint64_t vertex, bool is_directed, WriterTraceBlock* trace_block);

        void remove_edge(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block);

    private:

        [[nodiscard]] NeoRangeNode* find_range_node(uint64_t vertex) const;

        //@return -1 when exist
        int find_position_to_be_inserted(uint64_t src, uint64_t dest, NeoVertex vertex, RangeElementSegment_t * arr, uint16_t arr_size, std::vector<uint16_t>* vertices);

        uint16_t find_mid_count(std::vector<uint16_t>* vertices);

        void remove_node(NeoRangeNode& node, uint64_t node_idx, uint16_t nodes_move_begin_point);

        void vertex_map_update(RangeElementSegment_t* segment, uint16_t* vertices, uint16_t vertex_num, uint16_t node_idx, int offset);

        void vertex_map_update_split(RangeElementSegment_t* segment, uint16_t* vertices, uint16_t vertex_num, uint16_t node_idx, uint16_t last_vertex_unchanged, int offset);

        RangeTree* extract2range_tree(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, NeoRangeNode& range_node, WriterTraceBlock* trace_block);

        ART* direct2art(uint64_t src, uint64_t degree, uint64_t new_element, Property_t* new_property, NeoRangeNode& range_node, WriterTraceBlock* trace_block);
    };
}

// ---------------------------------------Implementation---------------------------------------
namespace container {
    template<typename F>
    void NeoTreeVersion::edges(uint64_t src, F&& callback) const {
        auto vertex = vertex_map->at(src & VERTEX_GROUP_MASK);
        if (!vertex.is_independent) {
            auto iter = (RangeElement *) node_block->at(vertex.range_node_idx).arr_ptr + vertex.neighbor_offset;
            for (auto i = 0; i < vertex.degree; i++) {
                uint64_t dst = iter[i];
                callback(dst, 0.0);
            }
        } else if (!vertex.is_art) {
            ((RangeTree*)vertex.neighborhood_ptr)->for_each(callback);
        } else {
            ((ART*)vertex.neighborhood_ptr)->for_each_element(callback);
        }
    }

}

