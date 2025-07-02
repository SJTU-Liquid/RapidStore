#include <cstdlib>
#include <algorithm>
#include <iostream>
#include <set>

#include "../include/art_node.h"
#include "../include/art.h"

namespace container {
    ART::ART(): root(alloc_node(NODE4, ARTKey{0}, 0, nullptr)), resources(new std::vector<ARTResourceInfo>()) {}

    ART::~ART() {
        delete resources;
    }

    ARTLeaf *ART::search(ARTKey key) const {
        ARTNode **child;
        ARTNode *n = root;
        int depth = 0;

        union {
            ARTNode_4 *p1;
            ARTNode_16 *p2;
            ARTNode_48 *p3;
            ARTNode_256 *p4;
        } p{};
        while (n) {
            // Might be a leaf
            if (IS_LEAF(n)) {
                auto l = LEAF_RAW(n);
                auto offset = GET_OFFSET(n);
                if(l->depth == 4) {
                    assert(ARTKey::check_partial_match(ARTKey{l->at(offset)}, key, l->depth));
                    return (ARTLeaf*)n;
                } else {
                    // path compression
                    if(ARTKey::check_partial_match(ARTKey{l->at(offset)}, key, l->depth)) {
                        return (ARTLeaf*)n;
                    }
                }
                return nullptr;
            }
            p.p1 = (ARTNode_4*)n;
            // Recursively search
            if(n->depth == depth) {
                child = find_child(n, key[depth]);
                n = (child) ? *child : nullptr;
                depth++;
            } else {
                // path compression
                assert(n->depth > depth);
                if(ARTKey::check_partial_match(n->prefix, key, n->depth - 1)) {
                    depth = n->depth;
                    child = find_child(n, key[depth]);
                    n = (child) ? *child : nullptr;
                } else {
                    return nullptr;
                }
            }
        }
        return nullptr;
    }

    bool ART::has_element(uint64_t element) const {
//        if(element == 5643554) {
//            std::cout << "has_element: " << element << std::endl;
//        }
        ARTLeaf* leaf = search(ARTKey{element});
        if(leaf == nullptr) {
            return false;
        }
        return LEAF_RAW(leaf)->has_element(element, GET_OFFSET(leaf));
    }

#if EDGE_PROPERTY_NUM != 0
    Property_t ART::get_property(uint64_t element, uint8_t property) const {
        ARTLeaf* leaf = search(ARTKey{element});
        if(leaf == nullptr) {
            return Property_t();
        }
        auto raw_leaf = LEAF_RAW(leaf);
        auto offset = GET_OFFSET(leaf);
        auto idx = raw_leaf->find(element, offset);
        if(raw_leaf->at(idx) == element) {
            return raw_leaf->get_property(idx, property);
        } else {
            return Property_t();
        }
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    void ART::get_properties(uint64_t element, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) {
        ARTLeaf* leaf = search(ARTKey{element});
        if(leaf == nullptr) {
            return;
        }
        auto idx = leaf->find(element, 0);
        if(leaf->at(idx) == element) {
            leaf->get_properties(idx, property_ids, res);
        }
    }
#endif

    void ART::intersect(ART *b, std::vector<uint64_t> &result) const {
        node_intersect(this->root, b->root, result);
    }

    void ART::range_intersect(RangeElement *b, uint16_t b_size, std::vector<uint64_t> &result) const {
        node_range_intersect(this->root, b, b_size, result);
    }

    uint64_t ART::intersect(ART *b) const {
        return node_intersect(this->root, b->root);
    }

    uint64_t ART::range_intersect(RangeElement *b, uint16_t b_size) const {
        return node_range_intersect(this->root, b, b_size);
    }

    std::pair<uint64_t, uint64_t> ART::get_filling_info() const {
        return get_node_filling_info(root);
    }

    ARTNode** ART::find_match_node(ARTKey key) const {
        ARTNode *const *n = &root;
        ARTNode *const *child = n;
        int depth = 0;
        union {
            ARTNode_4 *p1;
            ARTNode_16 *p2;
            ARTNode_48 *p3;
            ARTNode_256 *p4;
        } p{};
        while (child) {
            p.p1 = (ARTNode_4*)(*child);
            // Recursively search
            if((*child)->depth == depth) {
                n = child;
                child = find_child(*n, key[depth]);
                if(child == nullptr || IS_LEAF(*child)) {
                    return (ARTNode**)n;   // found or empty
                }
            } else {
                // path compression
                assert((*child)->depth > depth);
                if(ARTKey::check_partial_match((*child)->prefix, key, (*child)->depth)) {
                    depth = (*child)->depth;
                    n = child;
                    child = find_child(*n, key[depth]);
                    if(child == nullptr || IS_LEAF(*child)) {
                        return (ARTNode**)n;   // found or empty
                    }
                } else {
                    return (ARTNode**)n;   // not match
                }
            }
            depth++;
        }
        throw std::runtime_error("ART::find_match_node(): should not reach here");
    }

    std::tuple<std::vector<ARTNode*>*, ART*, ARTNode**> ART::copy_path(uint64_t value, bool need_exist, WriterTraceBlock* trace_block) const {
        ARTKey key{value};
        auto path = new std::vector<ARTNode*>{};
        path->reserve(6);

        // find the path
        ARTNode **child;
        auto n = (ARTNode**)&root;
        int depth = 0;

        while (n) {
            // Recursively search
            if((*n)->depth == depth) {
                path->push_back(*n);
                child = find_child(*n, key[depth]);
                if(child == nullptr) {
                    break;
                } else if(IS_LEAF(*child)) {
                    if(LEAF_RAW(*child)->has_element(value, 0) != need_exist) {
                        delete path;
                        return {nullptr, nullptr, nullptr};
                    } else {
                        break;
                    }
                }
                depth++;
            } else {
                // path compression
                assert((*n)->depth > depth);
                if(ARTKey::check_partial_match((*n)->prefix, key, (*n)->depth)) {
                    path->push_back(*n);
                    depth = (*n)->depth;
                    child = find_child(*n, key[depth]);
                    if(child == nullptr) {
                        break;
                    } else if(IS_LEAF(*child)) {
                        if(LEAF_RAW(*child)->has_element(value, 0) != need_exist) {
                            delete path;
                            return {nullptr, nullptr, nullptr}; // already exists
                        } else {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
            n = child;
        }
        if(path->empty()) {
            auto longest_common_prefix = ARTKey::longest_common_prefix(key, ARTKey{root->prefix});
            if(longest_common_prefix >= root->depth) {
                path->push_back(root);
            } else {
                auto new_art = new ART();
                new_art->root->depth = longest_common_prefix;
                new_art->root->prefix = ARTKey{key, longest_common_prefix, false};
                add_child(new_art->root, &new_art->root, root->prefix[longest_common_prefix], copy_node(root, trace_block), trace_block);

                path->push_back(root);
                return {path, new_art, (ARTNode**)&new_art->root};
            }
        }

        // link the nodes
        auto new_art = new ART();
        delete ((ARTNode_4*) new_art->root);    // delete the default root
        new_art->root = copy_node(path->at(0), trace_block);
        ARTNode** child_res = &new_art->root;
        for(int i = 1; i < path->size(); i++) {
            auto idx = find_child_idx(*child_res, key[(*child_res)->depth]);
            child_res = add_child_copy(*child_res, idx, copy_node(path->at(i), trace_block));
        }

        return {path, new_art, child_res};  // old path, new art, new node to be inserted
    }

    bool ART::insert_element(ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block) {
        auto node = find_match_node(key);
        return insert(node, key, value, property, trace_block);
    }

    bool ART::insert_element(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block) {
        return insert_element(ARTKey(dest), dest, property, trace_block);
    }

    ARTInsertElemCopyRes ART::insert_element_copy(ARTKey key, uint64_t value, Property_t* property, WriterTraceBlock* trace_block) {
        std::tuple<std::vector<ARTNode*>*, ART*, ARTNode**> copy_res = copy_path(value, false, trace_block);
        auto path = std::get<0>(copy_res);
        if(path == nullptr) {
            return ARTInsertElemCopyRes{false, 0};
        }
        ART* copied_art = std::get<1>(copy_res);
        ARTNode** node = std::get<2>(copy_res);

        for(auto path_node: *path) {
            resources->push_back(ARTResourceInfo{ARTResourceType::ART_Node_Copied, (void*)path_node});
        }
//        bool is_new = insert_copy(node, key, value, property, *resources, trace_block);
        bool is_new = insert(node, key, value, property, trace_block);
        delete path;

        return ARTInsertElemCopyRes{is_new, (uint64_t)copied_art};
    }

    ARTInsertElemCopyRes ART::insert_element_copy(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block) {
        return insert_element_copy(ARTKey(dest), dest, property, trace_block);
    }

    bool ART::remove_element(ARTKey key, uint64_t value, WriterTraceBlock* trace_block) {
        return true;
    }

    bool ART::remove_element(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block) {
        return true;
    }

    ARTRemoveElemCopyRes ART::remove_element_copy(ARTKey key, uint64_t value, WriterTraceBlock* trace_block) {
        return ARTRemoveElemCopyRes{true, false};
    }

    ARTRemoveElemCopyRes ART::remove_element_copy(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block) {
        return ARTRemoveElemCopyRes{true, false};
    }

#if EDGE_PROPERTY_NUM >= 1
    ART* ART::set_property(uint64_t element, uint8_t property_id, Property_t property, WriterTraceBlock* trace_block) {
        return nullptr;
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    ART* ART::set_properties(uint64_t element, std::vector<uint8_t>* property_ids, Property_t* properties) {
        assert(false);
        ARTLeaf* leaf = search(ARTKey{element});
        if(leaf == nullptr) {
            return nullptr;
        }
        auto idx = leaf->find(element, 0);
        if(leaf->at(idx) == element) {
            leaf->set_properties(idx, property_ids, properties);
        }
    }
#endif

    void ART::handle_resources_copied(WriterTraceBlock* trace_block) {
        for(auto resource: *resources) {
            switch (resource.type) {
                case ARTResourceType::ART_Leaf: {
                    auto leaf = (ARTLeaf *) resource.ptr;
                    leaf_clean(leaf, trace_block);
                    delete leaf;
                    break;
                }
                case ARTResourceType::ART_Node_Copied: {
                    auto node = (ARTNode *) resource.ptr;
                    delete_node(node, trace_block);
                    break;
                }
                case ARTResourceType::ART_Node_Mounted: {
                    break;
                }
#if EDGE_PROPERTY_NUM != 0
                case ARTResourceType::ART_Property_Vec: {
                    trace_block->deallocate_art_prop_vec((ARTPropertyVec_t*) resource.ptr);
                    break;
                }
                case ARTResourceType::ART_Property_Map_All_Modified: {
                    trace_block->deallocate_art_prop_vec((ARTPropertyVec_t*) resource.ptr);
                    break;
                }
//                case ARTResourceType::Multi_ART_Property_Vec_Copied: {
//                    delete (MultiARTPropertyVec_t*) resource.ptr;
//                    break;
//                }
#endif
                default:
                    std::cerr <<"ART::insert_element_batch(): unknown resource type" << std::endl;
                    abort();
            }
        }
        resources->clear();
    };

    void ART::handle_resources_ref() {
        for(auto resource: *resources) {
            switch (resource.type) {
                case ARTResourceType::ART_Leaf: {
                    auto leaf = (ARTLeaf *) resource.ptr;
                    leaf->ref_cnt -= 1;
                    break;
                }
                case ARTResourceType::ART_Node_Copied: {
                    auto node = (ARTNode *) resource.ptr;
                    node->ref_cnt -= 1;
                    node_ref(node);
                    break;
                }
                case ARTResourceType::ART_Node_Mounted: {
                    auto node = (ARTNode *) resource.ptr;
                    node->ref_cnt += 1;
                    break;
                }
#if EDGE_PROPERTY_NUM != 0
                case ARTResourceType::ART_Property_Vec: {
                    auto property_vec = (ARTPropertyVec_t*) resource.ptr;
                    property_vec->ref_cnt -= 1;
                    break;
                }
                case ARTResourceType::ART_Property_Map_All_Modified: {
#if EDGE_PROPERTY_NUM > 1
                    auto map = (MultiARTPropertyVec_t *)resource.ptr;
#else
                    auto map = (ARTPropertyVec_t*)resource.ptr;
#endif
                    map->ref_cnt -= 1;
                    break;
                }
                case ARTResourceType::Multi_ART_Property_Vec_Copied: {
                    auto map = (MultiARTPropertyVec_t *)resource.ptr;
                    for(auto property_vec: map->properties) {
                        property_vec->ref_cnt -= 1;
                    }
                    delete map;
                    break;
                }
#endif
                default:
                    std::cerr <<"ART::handle_resources_ref(): unknown resource type" << std::endl;
                    abort();
            }
        }
        resources->clear();
    }

    void ART::gc_ref(WriterTraceBlock* trace_block) {
        root->ref_cnt = 1;
        gc_node_ref(root, trace_block);
    }

    void ART::destroy() {
        recursive_destroy_node(root);
    }
}