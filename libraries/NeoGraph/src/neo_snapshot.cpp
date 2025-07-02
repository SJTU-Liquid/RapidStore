#include "../include/neo_snapshot.h"
#include "utils/helper.h"

namespace container {
    NeoSnapshot::NeoSnapshot(const TransactionManager *tm) : index(tm->index_impl),
                                                                        versions(new std::vector<NeoTreeVersion *>()) {
        trace_block = reader_register();
        timestamp = tm->get_read_timestamp();
        set_timestamp(trace_block, timestamp);
        // add all versions
        versions->resize(index->forest->size());
        for (uint64_t idx = 0; idx < index->forest->size(); idx++) {
            auto tree = index->forest->at(idx).get();
            if(tree != nullptr) {
                versions->at(idx) = tree->find_version(timestamp);
            }
        }
        add_read_txn_num();
        set_status(trace_block, 2); // running
    }

    NeoSnapshot::NeoSnapshot(const NeoSnapshot &other) : index(other.index), timestamp(other.timestamp),
                                                         versions(new std::vector<NeoTreeVersion *>) {
        trace_block = reader_register();
        set_timestamp(trace_block, timestamp);
        versions->resize(other.versions->size());
        for (int i = 0; i < other.versions->size(); i++) {
            if (other.versions->at(i) != nullptr) {
                versions->at(i) = other.versions->at(i);
                if (versions->at(i) != nullptr) {
                    assert(versions->at(i)->ref_cnt < VERSION_HEAD_MASK || versions->at(i)->ref_cnt < (VERSION_HEAD_MASK + (std::numeric_limits<uint64_t>::max() - VERSION_HEAD_MASK) / 2));
                    versions->at(i)->ref_cnt += 1;
                }
            }
        }
        add_read_txn_num();
        set_status(trace_block, 2); // running
    }

    NeoSnapshot::~NeoSnapshot() {
        // release all versions
        for (auto version: *versions) {
            if (version == nullptr) {
                continue;
            }
            NeoTree::release_version(version);
        }
        dec_read_txn_num();
        delete versions;
        reader_unregister(trace_block);
    }


    bool NeoSnapshot::has_vertex(uint64_t vertex) const {
        NeoTreeVersion *version = find_version(vertex);
        if (version != nullptr) {
            return version->has_vertex(vertex);
        }
        return false;
    }

    bool NeoSnapshot::has_edge(uint64_t src, uint64_t dest) const {
        NeoTreeVersion *version = find_version(src);
        if (version != nullptr) {
            return version->has_edge(src, dest);
        }
        return false;
    }

    uint64_t NeoSnapshot::get_degree(uint64_t vertex) const {
        NeoTreeVersion *version = find_version(vertex);
        if (version != nullptr) {
            return version->get_degree(vertex);
        }
        return 0;
    }

    bool NeoSnapshot::get_neighbor(uint64_t src, std::vector<uint64_t> &neighbor) const {
        NeoTreeVersion *version = find_version(src);
        if (version != nullptr) {
            return version->get_neighbor(src, neighbor);
        }
        return false;
    }

    RangeElement *NeoSnapshot::get_neighbor_addr(uint64_t src) const {
        NeoTreeVersion *version = find_version(src);
        if (version != nullptr) {
            return version->get_neighbor_addr(src);
        }
        return nullptr;
    }

#if VERTEX_PROPERTY_NUM >= 1
    Property_t NeoSnapshot::get_vertex_property(uint64_t vertex, uint8_t property_id) const {
        NeoTreeVersion *version = find_version(vertex);
        if (version != nullptr) {
            return version->get_vertex_property(vertex, property_id);
        }
        return std::numeric_limits<Property_t>::max();
    }
#endif
#if VERTEX_PROPERTY_NUM > 1
    void NeoSnapshot::get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
        NeoTreeVersion *version = find_version(vertex);
        if (version == nullptr) {
            throw std::runtime_error("NeoSnapshot::get_vertex_multi_property: version not found");
        }
        version->get_vertex_properties(vertex, property_ids, res);
    }
#endif
#if EDGE_PROPERTY_NUM >= 1
    Property_t NeoSnapshot::get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const {
        NeoTreeVersion *version = find_version(src);
        if (version != nullptr) {
            return version->get_edge_property(src, dest, property_id);
        }
        return std::numeric_limits<Property_t>::max();
    }
#endif
#if EDGE_PROPERTY_NUM > 1
    void NeoSnapshot::get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const {
        NeoTreeVersion *version = find_version(src);
        if (version == nullptr) {
            throw std::runtime_error("NeoSnapshot::get_edge_multi_property: version not found");
        }
        version->get_edge_properties(src, dest, property_ids, res);
    }
#endif

    void NeoSnapshot::intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) const {
        auto version1 = find_version(src1);
        auto version2 = find_version(src2);
        if(version1 == nullptr || version2 == nullptr) {
            return;
        }
        NeoTreeVersion::intersect(version1, src1, version2, src2, result);
    }

    uint64_t NeoSnapshot::intersect(uint64_t src1, uint64_t src2) const {
        auto version1 = find_version(src1);
        auto version2 = find_version(src2);
        if(version1 == nullptr || version2 == nullptr) {
            return 0;
        }

//        auto res2 = 0;
//        std::vector<uint64_t> neighbor_1;
//        get_neighbor(src1, neighbor_1);
//        auto index_1 = 0;
//        auto get_intersection = [&](uint64_t vertex, double weight) {
//            while(index_1 < neighbor_1.size() && neighbor_1[index_1] < vertex) {
//                index_1++;
//            }
//            if(index_1 < neighbor_1.size() && neighbor_1[index_1] == vertex) {
//                std::cout << vertex << " ";
//                res2 += 1;
//                index_1++;
//            }
//        };
//        edges(src2, get_intersection);
//        std::cout << std::endl;
//
//        std::vector<uint64_t> intersection1;
        auto res1 = NeoTreeVersion::intersect(version1, src1, version2, src2);
        // check the correctness of the result
//        if(res1 != res2) {
//            std::cerr << "NeoSnapshot::intersect: result not correct between " << src1 << " and " << src2 << std::endl;
//            abort();
//        }
        return res1;
    }

    NeoTreeVersion *NeoSnapshot::find_version(uint64_t vertex) const {
        if(NeoGraphIndex::gen_tree_direction(vertex) >= versions->size()) {
            return nullptr;
        }
        return versions->at(NeoGraphIndex::gen_tree_direction(vertex));
    }

}