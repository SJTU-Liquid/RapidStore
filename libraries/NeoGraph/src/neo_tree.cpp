#include "../include/neo_tree.h"
#include "../include/neo_index.h"
#include "utils/helper.h"
#include "include/neo_reader_trace.h"

// vertex_map: 10 Byte * 256 -> pooling
// range_block: 8 Byte * 256 -> pooling
// range_element_segment: 16 Byte * 256 -> pooling
// Node4: 1 Byte * 4 + 8 Byte * 4
// Node16: 1 Byte * 16 + 8 Byte * 16
// Node48: 1 Byte * 256 + 8 Byte * 48 -> pooling
// Node256: 8 Byte * 256 -> pooling

namespace container {
    NeoTree::NeoTree(uint64_t prefix): version_head(nullptr), direct_gc_flag(true), version_num(0) {}

    NeoTree::~NeoTree() {
        NeoTreeVersion* version = version_head;
        if(version == nullptr) {
            return;
        }
        version->destroy();
        delete version;
    }

    bool NeoTree::has_vertex(uint64_t vertex, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return false;
        }
        auto res = version->has_vertex(vertex);
        release_version(version);
        return res;
    }

    bool NeoTree::has_edge(uint64_t src, uint64_t dest, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return false;
        }
        auto res = version->has_edge(src, dest);
        release_version(version);
        return res;
    }

    uint64_t NeoTree::get_degree(uint64_t vertex, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return false;
        }
        auto res = version->get_degree(vertex);
        release_version(version);
        return res;
    }

    RangeElement *NeoTree::get_neighbor_addr(uint64_t vertex, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return nullptr;
        }
        auto res = version->get_neighbor_addr(vertex);
        release_version(version);
        return res;
    }

    std::pair<uint64_t, uint64_t> NeoTree::get_filling_info(uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return std::make_pair(0, 0);
        }
        auto res = version->get_filling_info();
        release_version(version);
        return res;
    }

#if VERTEX_PROPERTY_NUM >= 1
    Property_t NeoTree::get_vertex_property(uint64_t vertex, uint8_t property_id, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            return std::numeric_limits<Property_t>::max();
        }
        auto res = version->get_vertex_property(vertex, property_id);
        release_version(version);
        return res;
    }
#endif

#if VERTEX_PROPERTY_NUM > 1
    void NeoTree::get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            throw std::runtime_error("version not found");
        }
        version->get_vertex_properties(vertex, property_ids, res);
        release_version(version);
    }
#endif

#if EDGE_PROPERTY_NUM >= 1
    Property_t NeoTree::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            throw std::runtime_error("version not found");
        }
        auto res = version->get_edge_property(src, dest, property_id);
        release_version(version);
        return res;
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    void NeoTree::get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res, uint64_t timestamp) const {
        auto version = find_version(timestamp);
        if(version == nullptr) {
            throw std::runtime_error("version not found");
        }
        version->get_edge_properties(src, dest, property_ids, res);
        release_version(version);
    }
#endif

    bool NeoTree::get_neighbor(uint64_t src, std::vector<RangeElement> &neighbor, uint64_t timestamp) const {
        auto collect = [&] (uint64_t dest, uint64_t weight) {
            neighbor.push_back(dest);
            return 0;
        };
        edges(src, collect, timestamp);
        return !neighbor.empty();
    }

    void NeoTree::intersect(NeoTree* tree1, uint64_t src1, NeoTree* tree2, uint64_t src2, std::vector<uint64_t> &result, uint64_t timestamp) {
        NeoTreeVersion* version1 = nullptr;
        NeoTreeVersion* version2 = nullptr;
        if(tree1 == tree2) {
            version1 = tree1->find_version(timestamp);
            version2 = version1;
        } else {
            version1 = tree1->find_version(timestamp);
            version2 = tree2->find_version(timestamp);
        }
        if(version1 == nullptr || version2 == nullptr) {
            return;
        }
        NeoTreeVersion::intersect(version1, src1, version2, src2, result);
        if(tree1 == tree2) {
            NeoTree::release_version(version1);
        } else {
            NeoTree::release_version(version1);
            NeoTree::release_version(version2);
        }
    }

    uint64_t NeoTree::intersect(NeoTree* tree1, uint64_t src1, NeoTree* tree2, uint64_t src2, uint64_t timestamp) {
        NeoTreeVersion* version1 = nullptr;
        NeoTreeVersion* version2 = nullptr;
        if(tree1 == tree2) {
            version1 = tree1->find_version(timestamp);
            version2 = version1;
        } else {
            version1 = tree1->find_version(timestamp);
            version2 = tree2->find_version(timestamp);
        }
        if(version1 == nullptr || version2 == nullptr) {
            return 0;
        }
        uint64_t res = NeoTreeVersion::intersect(version1, src1, version2, src2);
        if(tree1 == tree2) {
            NeoTree::release_version(version1);
        } else {
            NeoTree::release_version(version1);
            NeoTree::release_version(version2);
        }
        return res;
    }

    void NeoTree::insert_vertex(uint64_t vertex, Property_t* property, WriterTraceBlock* trace_block) {
        assert(!uncommited_version);
        NeoTreeVersion* version = version_head;
        if(version) {
            version->ref_cnt += 1;
        }
        auto new_version = new NeoTreeVersion(version, trace_block);
        new_version->insert_vertex(vertex, property);
        finish_version(new_version);
    }

    void NeoTree::insert_vertex_batch(const uint64_t* vertices, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        assert(!uncommited_version);
        NeoTreeVersion* version = version_head;
        if(version) {
            version->ref_cnt += 1;
        }
        auto new_version = new NeoTreeVersion(version, trace_block);
        new_version->insert_vertex_batch(vertices, properties, count);
        finish_version(new_version);
    }

#if VERTEX_PROPERTY_NUM >= 1
    void NeoTree::set_vertex_property(uint64_t vertex, uint8_t property_id, Property_t property) {
        NeoTreeVersion* version = find_version(timestamp);
        auto new_version = new NeoTreeVersion(version, timestamp);
        new_version->set_vertex_property(vertex, property_id, property);
        commit_version(new_version);
    }

    void NeoTree::set_vertex_string_property(uint64_t vertex, uint8_t property_id, std::string&& property) {
        NeoTreeVersion* version = find_version(timestamp);
        auto new_version = new NeoTreeVersion(version, timestamp);
        new_version->set_vertex_string_property(vertex, property_id, std::move(property));
        commit_version(new_version);
    }
#endif

#if VERTEX_PROPERTY_NUM > 1
    void NeoTree::set_vertex_properties(uint64_t vertex, std::vector<uint8_t>* property_ids, Property_t* properties) {
        NeoTreeVersion* version = find_version(timestamp);
        auto new_version = new NeoTreeVersion(version, timestamp);
        new_version->set_vertex_properties(vertex, property_ids, properties);
        commit_version(new_version);
    }
#endif

    void NeoTree::insert_edge(uint64_t src, uint64_t dest, Property_t* property, WriterTraceBlock* trace_block) {
#ifndef NDEBUG
//        if(src != 2) return;
#endif
        assert(!uncommited_version);
        NeoTreeVersion* version = version_head;
        version->ref_cnt += 1;
        auto new_version = new NeoTreeVersion(version, trace_block);
        assert(new_version->next);
        new_version->insert_edge(src, dest, property, trace_block);
        finish_version(new_version);
        assert(uncommited_version);
    }


    void NeoTree::insert_edge_batch(const std::pair<RangeElement, RangeElement> *edges, Property_t ** properties, uint64_t count, WriterTraceBlock* trace_block) {
        assert(!uncommited_version);
        NeoTreeVersion* version = version_head;
        version_head->ref_cnt += 1;
        auto new_version = new NeoTreeVersion(version, trace_block);
        new_version->insert_edge_batch(edges, properties, count, trace_block);
        finish_version(new_version);
        assert(uncommited_version);
    }

#if EDGE_PROPERTY_NUM >= 1
    void NeoTree::set_edge_property(uint64_t src, uint64_t dest, uint8_t property_id, Property_t property, WriterTraceBlock* trace_block) {
        assert(!uncommited_version);
        NeoTreeVersion* version = version_head;
        version_head->ref_cnt += 1;
        auto new_version = new NeoTreeVersion(version, trace_block);
        assert(new_version->next);
        new_version->set_edge_property(src, dest, 0, property, trace_block);
        finish_version(new_version);
        assert(uncommited_version);
    }
#endif

#if EDGE_PROPERTY_NUM > 1
    void NeoTree::set_edge_properties(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, Property_t* properties) {
        NeoTreeVersion* version = find_version(timestamp);
        auto new_version = new NeoTreeVersion(version, timestamp);
        new_version->set_edge_properties(src, dest, property_ids, properties);
        commit_version(new_version);
    }
#endif

    bool NeoTree::remove_vertex(uint64_t vertex, bool is_directed, WriterTraceBlock* trace_block) {
        NeoTreeVersion* version = version_head;
        NeoVertex& vertex_entry = version->vertex_map->at(vertex);
        version_head->ref_cnt += 1;
        auto new_version = new NeoTreeVersion(version, trace_block);
        new_version->remove_vertex(vertex, is_directed, trace_block);
        finish_version(new_version);
        return true;
    }

    void NeoTree::remove_edge(uint64_t src, uint64_t dest, WriterTraceBlock* trace_block) {
#ifndef NDEBUG
//        if(src != 2) return;
#endif
        assert(!uncommited_version);
        NeoTreeVersion* version = version_head;
        version_head->ref_cnt += 1;
        auto new_version = new NeoTreeVersion(version, trace_block);
        assert(new_version->next);
        new_version->remove_edge(src, dest, trace_block);
        finish_version(new_version);
        assert(uncommited_version);
    }


    NeoTreeVersion* NeoTree::find_version(uint64_t timestamp) const {
        auto cur = version_head;
        if(!(cur && cur->timestamp >= timestamp)) {
            while(uncommited_version) {
                // wait for writer's commit
            }
        }
        while(cur != nullptr) {
            if(timestamp >= cur->timestamp) {
                // add reference
                cur->ref_cnt = cur->ref_cnt + 1;
                return cur;
            }
            cur = cur->next;
        }

        return nullptr;
    }

    bool NeoTree::finish_version(NeoTreeVersion *version) {
        uncommited_version = version;
        assert(uncommited_version);
        return true;
    }

    bool NeoTree::commit_version(uint64_t timestamp) {
#ifndef NDEBUG
        if(uncommited_version) {
            uncommited_version->timestamp = timestamp;
            version_head = uncommited_version;
            uncommited_version = nullptr;
        }
#else
        // set new version as head
        if(uncommited_version) {
            uncommited_version->timestamp = timestamp;
            version_head = uncommited_version;
            uncommited_version = nullptr;
        }
#endif
        return true;
    }

    void NeoTree::version_gc(NeoTreeVersion*& head, std::vector<uint64_t>& readers, WriterTraceBlock* trace_block) {
        NeoTreeVersion* curr_version = head;
        NeoTreeVersion* prev_version = nullptr;
        int curr_reader_idx = readers.size() - 1;

        while (curr_version != nullptr) {
            bool might_be_obtained = false;
            // Move to the next reader whose timestamp is less than the current version's timestamp
            if(curr_reader_idx >= 0 && readers[curr_reader_idx] >= curr_version->timestamp) {
                might_be_obtained = true;
                while (curr_reader_idx >= 0 && readers[curr_reader_idx] >= curr_version->timestamp) {
                    curr_reader_idx--;
                }
            }
            curr_version->handle_resources_ref();
            if (might_be_obtained) {
                // At least one reader can obtain this version; keep it
                prev_version = curr_version;
                curr_version = curr_version->next;
            } else {
                // No reader can obtain this version; delete it if is 0
                NeoTreeVersion* temp = curr_version;
                if (temp->ref_cnt == 0) {
                    curr_version = curr_version->next;
                    assert(prev_version != nullptr);
                    prev_version->next = curr_version;
                    temp->gc_ref(trace_block);
                    temp->clean(trace_block);
                    delete temp;
                    version_num -= 1;
                    if(version_num <= 1) {  // ref counts return to 1
                        direct_gc_flag = true;
                    }
                } else {
                    prev_version = curr_version;
                    curr_version = curr_version->next;
                }
            }
        }
    }

    void NeoTree::gc(WriterTraceBlock* trace_block) {
        uncommited_version = nullptr;
        version_num += 1;
        if(version_num > 2) {
            direct_gc_flag = false;
        }
        auto version = version_head;
        if(version_head->next == nullptr) {
            writer_lock.unlock();
            return;
        }
        version->next->ref_cnt.fetch_and(~VERSION_HEAD_MASK);
        version->next->ref_cnt.fetch_sub(1);
        if(direct_gc_flag) {  // try direct gc
//                auto next_timestamp = version_head->next->timestamp;
//                auto head_timestamp = version_head->timestamp;
            if (version_head->next->ref_cnt == 0 && !version_head->next->resource_handled && get_read_txn_num() == 0) {
//                    if(*read_txn_count != 0) {
//                        std::vector<uint64_t> actives;
//                        get_active_reader_info(actives);
//                        for (auto active_timestamp: actives) {
//                            if (active_timestamp >= next_timestamp &&
//                                active_timestamp < head_timestamp) {   // will be acquired by active reader
//                                return;
//                            }
//                        }
//                    }
//                    if (version_head->next->ref_cnt != 0) {
//                        std::cerr << "version_head->next->ref_cnt != 0" << std::endl;
//                        abort();
//                    }

#ifndef NDEBUG
                // check the correctness of the version num
                auto cur = version_head;
                int cnt = 0;
                while(cur != nullptr) {
                    cnt += 1;
                    cur = cur->next;
                }
                assert(cnt == version_num);
#endif
                version_head->next->gc_copied(trace_block);
                version_head->next->clean(trace_block);
                delete version_head->next;
                version_head->next = nullptr;
                version_num -= 1;
//                writer_lock.unlock();
                return;
            }
        }

        // acquire actives
        std::vector<uint64_t> actives;
        get_active_reader_info(actives);
        version_gc(version_head, actives, trace_block);
//        writer_lock.unlock();
    }
}

//    void NeoTree::insert_edge_batch(const std::pair<uint64_t, uint64_t> *edges, Property_t ** properties, uint64_t count) {
//        assert(!uncommited_version);
//        NeoTreeVersion* version = version_head;
//        version_head->ref_cnt += 1;
//        auto new_version = new NeoTreeVersion(version);
//        new_version->insert_edge_batch(edges, properties, count);
//        finish_version(new_version);
//        assert(uncommited_version);
//    }