cmake_minimum_required(VERSION 3.10)
project(container)

include_directories(.)

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Release mode")
    add_compile_options(-O3)
else ()
    message(STATUS "Debug mode")
endif ()

add_compile_options(-mavx2 -mfma -mavx512f -mavx512dq -mavx512cd -mavx512bw -mavx512vl)

add_library(neo_bitmap STATIC
        ../types.h
        ../types.cpp

        include/bitmap.h

        src/bitmap.cpp
)