cmake_minimum_required(VERSION 3.10)
project(container)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Release mode")
    add_compile_options(-O3)
else ()
    message(STATUS "Debug mode")
endif ()

add_compile_options(-mavx2 -mfma -mavx512f -mavx512dq -mavx512cd -mavx512bw -mavx512vl)

add_library(art_new STATIC
        ../types.h
        ../types.cpp

        include/helper.h
        include/art.h
        include/art_node.h
        include/art_leaf.h
        include/art_node_ops_copy.h
        include/art_node_iter.h
        include/art_node_ops.h

        src/art_node.cpp
        src/art.cpp
        src/art_leaf.cpp
        src/art_node_ops_copy.cpp
        src/art_node_ops.cpp
        src/art_node_iter.cpp
        include/art_iter.h
        src/art_iter.cpp
)
target_link_libraries(art_new PUBLIC neo_bitmap)