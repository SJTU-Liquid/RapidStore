#pragma once

#include <cstdint>
#include "neo_index.h"
#include "neo_transaction.h"

namespace  container {
    class NeoSnapshot {
        const NeoGraphIndex* index;
        uint64_t timestamp;
        ReaderTraceBlock* trace_block;
        std::vector<NeoTreeVersion*>* versions;
    public:
        explicit NeoSnapshot(const TransactionManager* tm);
        NeoSnapshot(const NeoSnapshot& other);
        NeoSnapshot(NeoSnapshot&& other) = default;
        ~NeoSnapshot();

        [[nodiscard]] bool has_vertex(uint64_t vertex) const;

        [[nodiscard]] bool has_edge(uint64_t src, uint64_t dest) const;

        [[nodiscard]] uint64_t get_degree(uint64_t src) const;

        bool get_neighbor(uint64_t src, std::vector<uint64_t> &neighbor) const;

        [[nodiscard]] RangeElement *get_neighbor_addr(uint64_t src) const;

#if VERTEX_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_vertex_property(uint64_t vertex, uint8_t property_id) const;
#endif
#if VERTEX_PROPERTY_NUM > 1
        void get_vertex_multi_property(uint64_t vertex, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
#if EDGE_PROPERTY_NUM >= 1
        [[nodiscard]] Property_t get_edge_property(uint64_t src, uint64_t dest, uint8_t property_id) const;
#endif
#if EDGE_PROPERTY_NUM > 1
        void get_edge_multi_property(uint64_t src, uint64_t dest, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
        ///@return false if vertex does not exist
        template<typename F>
        void edges(uint64_t src, F &&callback) const;

        void intersect(uint64_t src1, uint64_t src2, std::vector<uint64_t> &result) const;

        uint64_t intersect(uint64_t src1, uint64_t src2) const;

    private:
        [[nodiscard]] NeoTreeVersion* find_version(uint64_t vertex) const;
    };

    template<typename F>
    void NeoSnapshot::edges(uint64_t src, F &&callback) const {
        NeoTreeVersion* version = find_version(src);
        if(version != nullptr) {
            version->edges(src, callback);
        }
    }
};