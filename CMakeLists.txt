cmake_minimum_required(VERSION 3.11)
project(driver)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -mcx16")

add_subdirectory(readers)
add_subdirectory(graph)
add_subdirectory(libraries/teseo)
add_subdirectory(libraries/sortledton)
add_subdirectory(libraries/LiveGraph)
add_subdirectory(libraries/aspen)
add_subdirectory(libraries/NeoGraph)
add_subdirectory(utils)
add_subdirectory(third-party/libcuckoo)
#add_subdirectory(third-party/)
add_subdirectory(wrapper)
add_subdirectory(dataset_preprocessor)
#add_subdirectory(types)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/utils)
include_directories(.)

find_package(Boost REQUIRED COMPONENTS program_options)
include_directories(${Boost_INCLUDE_DIRS})
set_target_properties(Boost::program_options PROPERTIES POSITION_INDEPENDENT_CODE ON)

find_package(TBB REQUIRED)

#include(FetchContent)
#FetchContent_Declare(
#  googletest
#  GIT_REPOSITORY https://github.com/google/googletest.git
#  GIT_TAG        release-1.12.0
#)
# FetchContent_MakeAvailable(googletest)

#enable_testing()

#include(GoogleTest)
#gtest_discover_tests(insert_and_delete_test)
#gtest_discover_tests(analysis_test)

