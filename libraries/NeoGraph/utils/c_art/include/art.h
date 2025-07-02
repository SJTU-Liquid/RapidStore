#pragma once

#include <cstdint>
#include <tbb/parallel_sort.h>
#include "art_node.h"
#include "art_node_ops_copy.h"
#include "../../types.h"
#include "art_leaf.h"
#include "include/neo_reader_trace.h"

namespace container {

    class ART {
    public:
        ARTNode *root;
        std::atomic<uint64_t> ref_cnt{1};
        std::vector<ARTResourceInfo>* resources;

        explicit ART();

        ~ART();

        /**
         * Searches for a leaf in the ART tree
         * @arg key The key
         * @return nullptr if the item was not found, otherwise
         */
        [[nodiscard]] ARTLeaf *search(ARTKey key) const;

        [[nodiscard]] bool has_element(uint64_t element) const;

#if EDGE_PROPERTY_NUM != 0
        [[nodiscard]] Property_t get_property(uint64_t element, uint8_t property) const;
#endif
#if EDGE_PROPERTY_NUM > 1
        void get_properties(uint64_t element, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res);
#endif

        void range_intersect(RangeElement *b, uint16_t b_size, std::vector<uint64_t> &result) const;

        void intersect(ART *b, std::vector<uint64_t> &result) const;

        uint64_t range_intersect(RangeElement *b, uint16_t b_size) const;

        uint64_t intersect(ART *b) const;

        [[nodiscard]] std::pair<uint64_t, uint64_t> get_filling_info() const;

        /**
         * Searches for the deepest node where the key should be inserted. This function must ensure that the key must be inserted in a leaf bond to the node logically (might split/expand the node if necessary).
         * @arg key The key
         * @return pointer to the node where the key should be inserted.
         */
        [[nodiscard]] ARTNode** find_match_node(ARTKey key) const;

        ///@brief create a new ART with the necessary path to the inserted element.
        ///@return the root of new the ART tree and the deepest match node.
        [[nodiscard]] std::tuple<std::vector<ARTNode*>*, ART*, ARTNode**> copy_path(uint64_t value, bool need_exist, WriterTraceBlock* trace_block) const;

        ///@return true if the element does not exist in the ART tree.
        bool insert_element(ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block);
        bool insert_element(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block);

        ARTInsertElemCopyRes insert_element_copy(ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block);
        ARTInsertElemCopyRes insert_element_copy(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block);

        void recursive_remove_node(ARTKey key, ARTNode** node, ARTNode** parent, uint8_t depth, WriterTraceBlock* trace_block);

        bool remove_element(ARTKey key, uint64_t value, WriterTraceBlock* trace_block);
        bool remove_element(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block);

        ARTRemoveElemCopyRes remove_element_copy(ARTKey key, uint64_t value, WriterTraceBlock* trace_block);
        ARTRemoveElemCopyRes remove_element_copy(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block);

        ///Till now, we don't need src at all.
        ///@return the number of new elements inserted.
        ARTInsertElemBatchRes insert_element_batch(const std::pair<RangeElement, RangeElement> *edges, Property_t** properties, uint64_t count, WriterTraceBlock* trace_block);

#if EDGE_PROPERTY_NUM >= 1
        ART* set_property(uint64_t element, uint8_t property_id, Property_t property, WriterTraceBlock* trace_block);
#endif
#if EDGE_PROPERTY_NUM > 1
        ART* set_properties(uint64_t element, std::vector<uint8_t>* property_ids, Property_t* properties);
#endif

        void handle_resources_copied(WriterTraceBlock* trace_block);

        void handle_resources_ref();

        void gc_ref(WriterTraceBlock* trace_block);

        ///@brief remove the all the nodes and leaves that can be reached from the root.
        void destroy();

        // For each leaf
        template<typename F>
        int for_each(F &&callback) const;

        template<typename F>
        int for_each_unordered(F &&callback) const;

        // For each element
        template<typename F>
        int for_each_element(F &&element_callback) const;

        template<typename F>
        int for_each_element_unordered(F &&element_callback) const;
    };
}

//------------------------------------------IMPLEMENTATION------------------------------------------
namespace container {
    template<typename F>
    int ART::for_each(F &&callback) const {
        return tree_leaf_iter(root, callback);
    }

    template<typename F>
    int ART::for_each_unordered(F &&callback) const {
        return tree_leaf_iter_unordered(root, callback);
    }

    template<typename F>
    int ART::for_each_element(F &&element_callback) const {
//        std::cout << "root's child num: " << root->num_children << std::endl;
        return tree_leaf_iter(root, element_callback);
    }

    template<typename F>
    int ART::for_each_element_unordered(F &&element_callback) const {
        auto leaf_callback = [&element_callback] (ARTLeaf* leaf) {
            leaf->for_each(element_callback);
        };
        return tree_leaf_iter_unordered(root, leaf_callback);
    }
}