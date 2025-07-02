cmake_minimum_required(VERSION 3.10)
project(container)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories("/usr/local")

set(CMAKE_CXX_STANDARD 17)

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3)
endif ()

#add_compile_options(-DSEGMENT_POOL_ENABLE)
add_compile_options(-DBATCH_UPDATE)
#set (CMAKE_CXX_FLAGS "-mno-avx")

# Enable AVX512F
add_compile_options(-mavx2 -mfma -mavx512f -mavx512dq -mavx512cd -mavx512bw -mavx512vl)

# ------------------Submodule------------------
#add_subdirectory(graph)
add_subdirectory(utils/c_art)
#add_subdirectory(utils/art_new)
add_subdirectory(utils/bitmap)

# ------------------Library--------------------
add_library(neo_graph STATIC
        utils/types.h
        utils/helper.h
        utils/thread_pool.h
        utils/spin_lock.h
        utils/spin_lock.cpp
        include/neo_property.h
        include/neo_wrapper.h
        include/neo_reader_trace.h
        include/neo_snapshot.h
        include/neo_transaction.h
        include/neo_index.h
        include/neo_tree.h
        include/neo_range_ops.h
        include/neo_range_tree.h
        include/neo_tree_version.h

        utils/types.cpp
        src/neo_property.cpp
        src/neo_snapshot.cpp
        src/neo_reader_trace.cpp
        src/neo_transaction.cpp
        src/neo_index.cpp
        src/neo_tree.cpp
        src/neo_range_ops.cpp
        src/neo_range_tree.cpp
        src/neo_tree_version.cpp
)
target_link_libraries(neo_graph PUBLIC tbb neo_bitmap)
target_link_libraries(neo_graph PUBLIC c_art)
#target_link_libraries(neo_graph PUBLIC art_new)