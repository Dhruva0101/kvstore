cmake_minimum_required(VERSION 3.16)
project(kvstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()
add_compile_options(-Wall -Wextra -Wpedantic)

add_library(kvstore
  src/kvstore.cpp
  src/wal.cpp
  src/crc32.cpp
  src/sstable.cpp
  src/filter.cpp
  src/server.cpp
  src/protocol.cpp
  src/client.cpp
  src/raft.cpp
  src/raft_storage.cpp
)
find_package(Threads REQUIRED)
target_link_libraries(kvstore PUBLIC Threads::Threads)
target_include_directories(kvstore PUBLIC include)

add_executable(kvcli src/main.cpp)
target_link_libraries(kvcli PRIVATE kvstore)

add_executable(kvserver src/kvserver_main.cpp)
target_link_libraries(kvserver PRIVATE kvstore)

add_executable(kvbench src/kvbench_main.cpp)
target_link_libraries(kvbench PRIVATE kvstore)

option(KV_BUILD_TESTS "Build the GoogleTest suite" ON)
if(KV_BUILD_TESTS)
# ---- Tests: use an installed GoogleTest if present, otherwise download it ----
find_package(GTest QUIET)
if(NOT GTest_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

enable_testing()
add_executable(kvstore_tests
  tests/kvstore_test.cpp
  tests/wal_test.cpp
  tests/sstable_test.cpp
  tests/filter_test.cpp
  tests/server_test.cpp
  tests/raft_test.cpp
)
target_link_libraries(kvstore_tests PRIVATE kvstore GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(kvstore_tests)
endif()
