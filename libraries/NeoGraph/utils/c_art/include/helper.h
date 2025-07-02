#pragma once

#include <cstdint>
#include <iostream>

#define LEAF8 1
#define LEAF16 2
#define LEAF32 3
#define LEAF64 4

#define NODE4   1
#define NODE16  2
#define NODE48  3
#define NODE256 4

#define KEY_LEN 3

/**
 * Macros to manipulate pointer tags
 */
#define IS_LEAF(x) (((uint64_t)(x)) & 0x8000000000000000)
#define SET_LEAF(x) ((void*)(((uint64_t)(x)) | 0x8000000000000000))

/**
 *  Offset field is used to store the distance between the area of this pointer and the beginning of the leaf segment.
 *  This field the 49-56th bits of the pointer.
 *  NOTE: This field is set only when the leaf flag is set!
 */
#define GET_OFFSET(x) ((((uint64_t)(x)) & 0x00FF000000000000) >> 48)
#define SET_OFFSET(x, offset) (assert(offset < 256), assert(IS_LEAF(x)), ((void*)((((uint64_t)(x)) & 0xFF00FFFFFFFFFFFF) | ((offset) << 48))))

#define LEAF_POINTER_CTOR(x, offset) (assert(offset < 256), ((void*)(((uint64_t)(x)) & 0x0000FFFFFFFFFFFF | ((uint64_t)1 << 63) | (((uint64_t)offset) << 48))))

#define LEAF_RAW(x) ((ARTLeaf*)(((uint64_t)(x)) & 0xFFFFFFFFFFFF))

