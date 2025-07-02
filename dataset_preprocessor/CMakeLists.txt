project(dataset_preprocessor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "-O3")

find_package(Boost REQUIRED COMPONENTS program_options)
set_target_properties(Boost::program_options PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(${PROJECT_NAME} main.cpp parser.cpp dataset_preprocessor.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE ${Boost_LIBRARIES})
target_link_libraries(${PROJECT_NAME} PRIVATE Boost::program_options )
