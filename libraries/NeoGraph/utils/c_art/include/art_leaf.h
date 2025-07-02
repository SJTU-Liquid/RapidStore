#pragma once

#include <cstdint>
#include <array>
#include <functional>
#include <functional>
#include <immintrin.h>
#include "../../config.h"
#include "../../types.h"
#include "helper.h"
#include "utils/bitmap/include/bitmap.h"
#include "include/neo_reader_trace.h"

namespace container {
    struct ARTLeaf;
    struct ARTLeaf8;
    struct ARTLeaf16;
    struct ARTLeaf32;
    struct ARTLeaf64;

    struct ARTLeaf {
        ARTKey key{0};   // only the least 40 bits are used
        uint16_t size{};
        uint8_t type{};
        uint8_t is_single_byte{}; // depth + is_single_byte, 0, 1-> 64, 2, 3->32, 4 -> 16, 5 -> 8
        uint8_t depth{};
        std::atomic<uint16_t> ref_cnt{1};
#if EDGE_PROPERTY_NUM == 1
        ARTPropertyVec_t* property_map;
#elif EDGE_PROPERTY_NUM > 1
        MultiARTPropertyVec_t* property_map;
#endif

        ARTLeaf(ARTKey key, uint8_t depth, bool is_single_byte);

        virtual ~ARTLeaf() = default;

        [[nodiscard]] virtual uint64_t at(uint16_t pos_idx) const = 0;

#if EDGE_PROPERTY_NUM != 0
        [[nodiscard]] Property_t get_property(uint16_t pos_idx, uint8_t property_id) const;
#endif
#if EDGE_PROPERTY_NUM > 1
        void get_properties(uint16_t pos_idx, std::vector<uint8_t>* property_ids, std::vector<Property_t>& res) const;
#endif
        [[nodiscard]] virtual bool has_element(uint64_t element, uint8_t begin_idx) const = 0;

        [[nodiscard]] virtual uint16_t find(uint64_t element, uint8_t begin_idx) const;

        [[nodiscard]] virtual uint16_t get_byte_num(uint8_t depth) const = 0;

        virtual void insert(uint64_t element, Property_t* property, uint16_t pos_idx) = 0;

#if EDGE_PROPERTY_NUM != 0
        void set_property(uint16_t pos_idx, uint8_t property_id, Property_t prop);
#endif
#if EDGE_PROPERTY_NUM > 1
        void set_properties(uint16_t pos_idx, std::vector<uint8_t>* property_ids, Property_t* properties);
#endif

        virtual void remove(uint16_t pos_idx, uint8_t target_byte) = 0;

        // NOTE the size will be directly add to the dst leaf, manually set the size of the dst leaf if there is overlap
        virtual void copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const = 0;

        // append a segment of elements to the leaf
        virtual void append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) = 0;

        virtual void leaf_check() const = 0;

        template<typename F>
        void for_each(F &&f) const;
    };

    // add a std::array<uint8_t, ART_LEAF_SIZE> to store the elements
    struct ARTLeaf8 : public ARTLeaf {
        Bitmap<4> value;

        ARTLeaf8(ARTKey key, uint8_t depth, bool is_single_byte);

        [[nodiscard]] uint64_t at(uint16_t pos_idx) const override;

        [[nodiscard]] bool has_element(uint64_t element, uint8_t begin_idx) const override;

        [[nodiscard]] uint16_t find(uint64_t element, uint8_t begin_idx) const override;

        [[nodiscard]] uint16_t get_byte_num(uint8_t depth) const override;

        template<typename F>
        void do_for_each(F &&f) const;

        void insert(uint64_t element, Property_t* property, uint16_t pos_idx) override;

        void remove(uint16_t pos_idx, uint8_t target_byte) override;

        ///@Note this function MUST be called after modifying the dst's size
        void copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const override;

        void append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) override;

        void leaf_check() const override;
    };

    struct ARTLeaf16 : public ARTLeaf {
        std::array<uint16_t, ART_LEAF_SIZE>* value;

        ARTLeaf16(ARTKey key, uint8_t depth, bool is_single_byte);

        [[nodiscard]] uint64_t at(uint16_t pos_idx) const override;

        [[nodiscard]] bool has_element(uint64_t element, uint8_t begin_idx) const override;

        [[nodiscard]] uint16_t get_byte_num(uint8_t depth) const override;

        template<typename F>
        void do_for_each(F &&f) const;

        void insert(uint64_t element, Property_t* property, uint16_t pos_idx) override;

        void remove(uint16_t pos_idx, uint8_t target_byte) override;

        void copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const override;

        void append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) override;

        void leaf_check() const override;
    };

    struct ARTLeaf32 : public ARTLeaf {
        std::array<uint32_t, ART_LEAF_SIZE>* value;

        ARTLeaf32(ARTKey key, uint8_t depth, bool is_single_byte);

        [[nodiscard]] uint64_t at(uint16_t pos_idx) const override;

        [[nodiscard]] bool has_element(uint64_t element, uint8_t begin_idx) const override;

        [[nodiscard]] uint16_t get_byte_num(uint8_t depth) const override;

        template<typename F>
        void do_for_each(F &&f) const;

        void insert(uint64_t element, Property_t* property, uint16_t pos_idx) override;

        void remove(uint16_t pos_idx, uint8_t target_byte) override;

        void copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const override;

        void append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) override;

        void leaf_check() const override;
    };

    struct ARTLeaf64 : public ARTLeaf {
        std::array<uint64_t, ART_LEAF_SIZE>* value;

        ARTLeaf64(ARTKey key, uint8_t depth, bool is_single_byte);

        [[nodiscard]] bool has_element(uint64_t element, uint8_t begin_idx) const override;

        [[nodiscard]] uint64_t at(uint16_t pos_idx) const override;

        [[nodiscard]] uint16_t get_byte_num(uint8_t depth) const override;

        template<typename F>
        void do_for_each(F &&f) const;

        void insert(uint64_t element, Property_t* property, uint16_t pos_idx) override;

        void remove(uint16_t pos_idx, uint8_t target_byte) override;

        void copy_to_leaf(uint16_t begin_idx, uint16_t end_idx, ARTLeaf *dst, uint16_t dst_idx) const override;

        void append_from_list(RangeElement* elem_list, Property_t ** prop_list, uint16_t count) override;

        void leaf_check() const override;
    };

    ARTLeaf* alloc_leaf(ARTKey key, uint8_t depth, bool is_single_byte, bool not_empty, WriterTraceBlock* trace_block);

    void leaf_clean(ARTLeaf* leaf, WriterTraceBlock* trace_block);
    void leaf_destroy(ARTLeaf* leaf);

    uint64_t get_list_byte_num(uint64_t* list, uint64_t size, uint8_t depth);

    template<typename F>
    void ARTLeaf8::do_for_each(F &&f) const {
        uint64_t mask = key.key;
        value.for_each([&](uint8_t idx) {
            f((idx | mask), 0.0);
        });
    }

    template<typename F>
    void ARTLeaf16::do_for_each(F &&f) const {
        uint64_t mask = key.key;
        for(int j = 0; j < size; j++) {
            f((value->at(j) | mask), 0.0);
        }
    }

    template<typename F>
    void ARTLeaf32::do_for_each(F &&f) const {
        uint64_t mask = key.key;
        for(int j = 0; j < size; j++) {
            f((value->at(j) | mask), 0.0);
        }
    }

    template<typename F>
    void ARTLeaf64::do_for_each(F &&f) const {
        for(int j = 0; j < size; j++) {
            f(value->at(j), 0.0);
        }
    }

    template<typename F>
    void ARTLeaf::for_each(F &&f) const {
        switch(type) {
            case LEAF8: {
                ((ARTLeaf8*)this)->do_for_each(std::forward<F>(f));
                break;
            }
            case LEAF16: {
                ((ARTLeaf16*)this)->do_for_each(std::forward<F>(f));
                break;
            }
            case LEAF32: {
                ((ARTLeaf32*)this)->do_for_each(std::forward<F>(f));
                break;
            }
            case LEAF64: {
                ((ARTLeaf64*)this)->do_for_each(std::forward<F>(f));
                break;
            }
        }
    }
}