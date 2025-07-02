#include "types.h"

namespace container {

    ARTKey::ARTKey(uint64_t dst) : key(dst & 0x00000000FFFFFF00) {};

    ARTKey::ARTKey(uint64_t dst, uint8_t depth, bool is_single_byte) : key(dst & 0x00000000FFFFFF00) {
        // Now the key domain of the leaves are the same to their parent nodes
        switch (depth + is_single_byte) {
//            case 0:
//                key &= 0x0000000000000000;  // uint64_t
//                break;
//            case 1:
//                key &= 0x0000FF0000000000;  // uint64_t
//                break;
            case 0:
                key &= 0x0000FFFF00000000;  // uint32_t
                break;
            case 1:
                key &= 0x0000FFFFFF000000;  // uint32_t
                break;
            case 2:
                key &= 0x0000FFFFFFFF0000;  // uint16_t
                break;
            case 3:
                key &= 0x0000FFFFFFFFFF00;  // uint8_t
                break;
            default:
                std::cout << "ARTKey::ARTKey(): Invalid depth" << std::endl;
                assert(false);
        }
    }

    ARTKey::ARTKey(ARTKey key, uint8_t depth, bool is_single_byte): key(key.key) {
        switch (depth + is_single_byte) {
//            case 0:
//                this->key &= 0x0000000000000000;  // uint64_t
//                break;
//            case 1:
//                this->key &= 0x0000FF0000000000;  // uint64_t
//                break;
            case 0:
                this->key &= 0x0000FFFF00000000;  // uint32_t
                break;
            case 1:
                this->key &= 0x0000FFFFFF000000;  // uint32_t
                break;
            case 2:
                this->key &= 0x0000FFFFFFFF0000;  // uint16_t
                break;
            case 3:
                this->key &= 0x0000FFFFFFFFFF00;  // uint8_t
                break;
            default:
                std::cout << "ARTKey::ARTKey(): Invalid depth" << std::endl;
                assert(false);
        }

    }

    uint8_t ARTKey::operator[](int idx) const {
        assert(idx < 5);
        return (key >> ((3 - idx) * 8)) & 0xFF;
    }

    uint8_t &ARTKey::operator[](int idx) {
        assert(idx < 5);
        return reinterpret_cast<uint8_t *>(&key)[3 - idx];
    }

    bool ARTKey::operator==(const ARTKey &rhs) const {
        return key == rhs.key;
    }

    bool ARTKey::operator!=(const ARTKey &rhs) const {
        return key != rhs.key;
    }

    bool ARTKey::operator<(const ARTKey &rhs) const {
        for(uint8_t depth = 0; depth < 3; depth++) {
            if((*this)[depth] != rhs[depth]) {
                return (*this)[depth] < rhs[depth];
            }
        }
        return false;
    }

    void ARTKey::print() const {
        for (int i = 0; i < 3; i++) {
            std::cout << (int) (*this)[i] << " ";
        }
        std::cout << std::endl;
    }

    uint8_t get_key_byte(uint64_t key, uint8_t depth) {
        return (key >> ((3 - depth) * 8)) & 0xFF;
    }

    NeoRangeNode::NeoRangeNode(): key(0), size(0), arr_ptr(0)
#if EDGE_PROPERTY_NUM > 0
    , property(nullptr)
#endif
    {}

    NeoRangeNode::NeoRangeNode(uint64_t key, uint64_t size, uint64_t arr_ptr, void* property_ptr): key(key), size(size), arr_ptr(arr_ptr)
#if EDGE_PROPERTY_NUM == 1
    , property((container::RangePropertyVec_t *)property_ptr)
#elif EDGE_PROPERTY_NUM > 1
    , property((container::MultiRangePropertyVec_t *)property_ptr)
#endif
    {}

    bool NeoRangeNode::is_empty() const {
#if EDGE_PROPERTY_NUM == 0
        return *((uint64_t*)this) == 0;
#else
        return *((uint64_t*)this) == 0 && *((uint64_t*)this + 1) == 0;
#endif
    }

//    RangeElement::RangeElement() : dst(0xFFFFFFFFFFFF) {}
//
//    RangeElement::RangeElement(uint64_t dst): dst(dst) {
//    };
//
//    bool RangeElement::operator<(const RangeElement &rhs) const {
//        return this->dst < rhs.dst;
//    }
//
//    bool RangeElement::operator==(const RangeElement &rhs) const {
//        return this->dst == rhs.dst;
//    }
//
//    bool RangeElement::operator!=(const RangeElement &rhs) const {
//        return !(*this == rhs);
//    }

    InRangeNode::InRangeNode(): size(0), arr_ptr(0) {}

    InRangeNode::InRangeNode(uint64_t size, uint64_t arr_ptr): size(size), arr_ptr(arr_ptr) {}

#if EDGE_PROPERTY_NUM != 0
    InRangeNode::InRangeNode(uint64_t size, uint64_t arr_ptr, RangePropertyVec_t* prop_arr_ptr): size(size), arr_ptr(arr_ptr), property_map(prop_arr_ptr) {}
#endif

}
