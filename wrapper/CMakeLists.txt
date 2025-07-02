cmake_minimum_required(VERSION 3.10)
project(wrapper)

find_package(OpenMP REQUIRED)
if(OpenMP_FOUND)
    message(STATUS "OpenMP found! Version: ${OpenMP_VERSION}")
    get_cmake_property(_variableNames VARIABLES)
    foreach (_variableName ${_variableNames})
        if (_variableName MATCHES "OpenMP")
            message(STATUS "${_variableName}=${${_variableName}}")
        endif()
    endforeach()
else()
    message(FATAL_ERROR "OpenMP not found!")
endif()
 set(ITTNOTIFY_INCLUDE_DIR "/opt/intel/oneapi/vtune/latest/include/")
 set(ITTNOTIFY_LIBRARY "/opt/intel/oneapi/vtune/latest/lib64/libittnotify.a")
 include_directories(${ITTNOTIFY_INCLUDE_DIR})

#add_compile_options(-DLIKWID_PERFMON)
add_compile_options(-O3)

 SET(LIBRARIES neo_wrapper.out livegraph_wrapper.out teseo_wrapper.out sortledton_wrapper.out aspen_wrapper.out)

#SET(LIBRARIES neo_wrapper.out sortledton_wrapper.out teseo_wrapper.out livegraph_wrapper.out )
 # Libraries

#ADD_EXECUTABLE(llama_wrapper.out wrapper.h apps/llama_wrapper/llama_wrapper.h apps/llama_wrapper/llama_wrapper.cpp)
#TARGET_LINK_LIBRARIES(llama_wrapper.out PUBLIC llama ${OpenMP_CXX_LIBRARIES})
#
ADD_EXECUTABLE(livegraph_wrapper.out wrapper.h apps/livegraph/livegraph_wrapper.h apps/livegraph/livegraph_wrapper.cpp)
TARGET_LINK_LIBRARIES(livegraph_wrapper.out PUBLIC livegraph libcuckoo tbb ${OpenMP_CXX_LIBRARIES})

ADD_EXECUTABLE(teseo_wrapper.out wrapper.h apps/teseo_wrapper/teseo_wrapper.h apps/teseo_wrapper/teseo_wrapper.cpp)
TARGET_LINK_LIBRARIES(teseo_wrapper.out PUBLIC teseo numa event_pthreads event_core pthread event ${OpenMP_CXX_LIBRARIES})

ADD_EXECUTABLE(sortledton_wrapper.out wrapper.h apps/sortledton_wrapper/sortledton_wrapper.h apps/sortledton_wrapper/sortledton_wrapper.cpp)
TARGET_LINK_LIBRARIES(sortledton_wrapper.out PUBLIC sortledton tbb ${OpenMP_CXX_LIBRARIES})

ADD_EXECUTABLE(aspen_wrapper.out driver.h wrapper.h apps/aspen_wrapper/aspen_wrapper.h apps/aspen_wrapper/aspen_wrapper.cpp)
TARGET_LINK_LIBRARIES(aspen_wrapper.out PUBLIC aspen ${OpenMP_CXX_LIBRARIES})

ADD_EXECUTABLE(neo_wrapper.out wrapper.h apps/neo_wrapper/neo_wrapper.h apps/neo_wrapper/neo_wrapper.cpp)
TARGET_LINK_LIBRARIES(neo_wrapper.out PUBLIC neo_graph tbb ${OpenMP_CXX_LIBRARIES})

 FOREACH(LIB IN LISTS LIBRARIES)
     TARGET_LINK_LIBRARIES(${LIB} PUBLIC reader graph utils atomic ${OpenMP_CXX_LIBRARIES} ${ITTNOTIFY_LIBRARY})
     target_include_directories(${LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
     target_include_directories(${LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
     target_include_directories(${LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../libraries/LiveGraph)
     target_include_directories(${LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../libraries/sortledton)
     target_include_directories(${LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../libraries/NeoGraph)
     target_include_directories(${LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../types)
 ENDFOREACH ()


