#pragma once
#include <array>
#include <cstdlib>
#include <algorithm>
#include <immintrin.h>
#include <cstdint>
#include <limits>

namespace container {

    template<size_t BLOCK_NUM>
    struct Bitmap {
        // #bits = BLOCK_NUM * 64
        std::array<uint64_t, BLOCK_NUM> data{};

        Bitmap() = default;
        Bitmap(const Bitmap &bitmap) = default;
        Bitmap(Bitmap &&bitmap) = default;
        Bitmap &operator=(const Bitmap &bitmap) = default;

        void set(uint64_t index);

        void reset(uint64_t index);

        [[nodiscard]] bool get(uint64_t index) const;

        ///@return the value that is relatively situated at the given index
        [[nodiscard]] uint64_t at(uint64_t index) const;

        [[nodiscard]] bool empty() const;

        [[nodiscard]] uint64_t find_first();

        [[nodiscard]] uint64_t find_first(uint64_t begin);

        ///@return the index of the first element that is greater than or equal to the given element
        [[nodiscard]] uint64_t lower_bound(uint64_t element, uint64_t prefix) const;

        [[nodiscard]] uint64_t consume();

        ///NOTE this is absolute index
        [[nodiscard]] uint64_t consume(uint64_t begin);

        template<typename F>
        void for_each(F &&f) const {
            for (size_t i = 0; i < BLOCK_NUM; i++) {
                uint64_t mask = data[i];
                while (mask) {
                    uint64_t t = mask & -mask;
                    uint64_t index = __builtin_ctzll(mask);
                    f(index + i * 64);
                    mask ^= t;
                }
            }
        }

        ///Note: begin and end are both inclusive and relative in the bitmap
        template<typename F>
        void for_each(F &&f, uint64_t begin, uint64_t end) const {
            uint64_t count = 0;
            for (size_t i = 0; i < BLOCK_NUM; i++) {
                uint64_t mask = data[i];
                while (mask) {
                    uint64_t t = mask & -mask;
                    uint64_t index = __builtin_ctzll(mask);
                    if(count >= begin && count < end) {
                        f(index + i * 64);
                    } else if(count >= end) {
                        return;
                    }
                    count++;
                    mask ^= t;
                }
            }
        }

        template<typename F>
        void for_each_zero(F &&f) const {
            for (size_t i = 0; i < BLOCK_NUM; i++) {
                uint64_t mask = ~data[i];
                while (mask) {
                    uint64_t t = mask & -mask;
                    uint64_t index = __builtin_ctzll(mask);
                    f(index + i * 64);
                    mask ^= t;
                }
            }
        }

    };

    template<size_t BLOCK_NUM>
    void Bitmap<BLOCK_NUM>::set(uint64_t index) {
        const uint64_t block = index / 64;
        const uint64_t offset = index % 64;
        data[block] |= 1ULL << offset;
    }

    template<size_t BLOCK_NUM>
    void Bitmap<BLOCK_NUM>::reset(uint64_t index) {
        const uint64_t block = index / 64;
        const uint64_t offset = index % 64;
        data[block] &= ~(1ULL << offset);
    }

    template<size_t BLOCK_NUM>
    bool Bitmap<BLOCK_NUM>::get(uint64_t index) const {
        const uint64_t block = index / 64;
        const uint64_t offset = index % 64;
        return data[block] & (1ULL << offset);
    }

    template<size_t BLOCK_NUM>
    uint64_t Bitmap<BLOCK_NUM>::at(uint64_t pos_idx) const {
        uint16_t count = 0;
        for (size_t i = 0; i < BLOCK_NUM; i++) {
            uint64_t mask = data[i];
            while (mask) {
                uint64_t t = mask & -mask;
                uint64_t index = __builtin_ctzll(mask);
                if (count == pos_idx) {
                    return index + i * 64;
                }
                count++;
                mask ^= t;
            }
        }
        return 0;
    }

    template<size_t BLOCK_NUM>
    bool Bitmap<BLOCK_NUM>::empty() const {
        return std::all_of(data.begin(), data.end(), [](uint64_t i) { return i == 0; });
    }

    template<size_t BLOCK_NUM>
    uint64_t Bitmap<BLOCK_NUM>::find_first() {
        for (size_t i = 0; i < BLOCK_NUM; i++) {
            uint64_t mask = data[i];
            if (mask) {
                uint64_t index = __builtin_ctzll(mask);
                return index + i * 64;
            }
        }
        return std::numeric_limits<uint64_t>::max();
    }

    template<size_t BLOCK_NUM>
    uint64_t Bitmap<BLOCK_NUM>::find_first(uint64_t begin) {
        const uint64_t begin_block = begin / 64;
        for (size_t i = begin_block; i < BLOCK_NUM; i++) {
            uint64_t mask = data[i];
            if (mask) {
                uint64_t index = __builtin_ctzll(mask);
                return index + i * 64;
            }
        }
        return std::numeric_limits<uint64_t>::max();
    }

    template<size_t BLOCK_NUM>
    uint64_t Bitmap<BLOCK_NUM>::lower_bound(uint64_t element, uint64_t prefix) const {
        uint64_t target = element & 0xFF;
        uint64_t res = 0;
        if((element & ~0xFF) == prefix){
            for (size_t i = 0; i < BLOCK_NUM; i++) {
                uint64_t mask = data[i];
                while (mask) {
                    uint64_t t = mask & -mask;
                    uint64_t index = __builtin_ctzll(mask);
                    if((index + i * 64) >= target){
                        break;
                    } else {
                        res ++;
                    }
                    mask ^= t;
                }
            }
        } else {
            for (size_t i = 0; i < BLOCK_NUM; i++) {
                uint64_t mask = data[i];
                while (mask) {
                    uint64_t t = mask & -mask;
                    uint64_t index = __builtin_ctzll(mask);
                    if (((index + i * 64) | prefix) >= element) {
                        break;
                    } else {
                        res++;
                    }
                    mask ^= t;
                }
            }
        }
        return res;
    }

    template<size_t BLOCK_NUM>
    uint64_t Bitmap<BLOCK_NUM>::consume() {
        for (size_t i = 0; i < BLOCK_NUM; i++) {
            uint64_t mask = data[i];
            if (mask) {
                uint64_t t = mask & -mask;
                uint64_t index = __builtin_ctzll(mask);
                data[i] = mask ^ t;
                return index + i * 64;
            }
        }
        return std::numeric_limits<uint64_t>::max();
    }

    template<size_t BLOCK_NUM>
    uint64_t Bitmap<BLOCK_NUM>::consume(uint64_t begin) {
        const uint64_t begin_block = begin / 64;
        for (size_t i = begin_block; i < BLOCK_NUM; i++) {
            uint64_t mask = data[i];
            if (mask) {
                uint64_t t = mask & -mask;
                uint64_t index = __builtin_ctzll(mask);
                data[i] = mask ^ t;
                return index + i * 64;
            }
        }
        return std::numeric_limits<uint64_t>::max();
    }
}