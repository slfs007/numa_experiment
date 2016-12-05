cmake_minimum_required(VERSION 3.3)
project(NUMA_LAB)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g")

set(SOURCE_FILES main.cpp )
set(GEN_SOURCE gen_ckp_and_log.cpp)
set(EXECUTABLE_OUTPUT_PATH ./build/)
add_executable(numa_test ${SOURCE_FILES})
add_executable(gen_file ${GEN_SOURCE})
target_link_libraries(numa_test tbb cpp_redis)