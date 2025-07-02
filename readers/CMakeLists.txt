project(reader)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SRC_LIST)
add_library(${PROJECT_NAME} SHARED ${SRC_LIST})