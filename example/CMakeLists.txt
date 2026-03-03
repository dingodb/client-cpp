project(Example)
cmake_minimum_required(VERSION 3.5)
set (CMAKE_CXX_STANDARD 17)

# Option 1: Use local repository (when building from project root)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../include/tikv_client.h")
    message(STATUS "Using local tikvcpp source")

    # Set build type
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE Release)
    endif()

    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CARGO_CMD cargo build --verbose --target-dir=${CMAKE_CURRENT_BINARY_DIR}/rust)
        set(TARGET_DIR "debug")
    else()
        set(CARGO_CMD cargo build --release --target-dir=${CMAKE_CURRENT_BINARY_DIR}/rust)
        set(TARGET_DIR "release")
    endif()

    set(RUST_PART_LIB "${CMAKE_CURRENT_BINARY_DIR}/rust/${TARGET_DIR}/libtikvrust.a")
    set(RUST_PART_CXX "${CMAKE_CURRENT_BINARY_DIR}/rust/cxxbridge/client-cpp/src/lib.rs.cc")
    set(RUST_PART_H "${CMAKE_CURRENT_BINARY_DIR}/rust/cxxbridge/client-cpp/src/lib.rs.h")

    add_custom_command(
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/../Cargo.toml
                ${CMAKE_CURRENT_SOURCE_DIR}/../src/lib.rs
                ${CMAKE_CURRENT_SOURCE_DIR}/../build.rs
        OUTPUT ${RUST_PART_LIB} ${RUST_PART_CXX} ${RUST_PART_H}
        COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/rust ${CARGO_CMD}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
    )

    add_library(tikvcpp STATIC ${CMAKE_CURRENT_SOURCE_DIR}/../src/tikv_client.cpp ${RUST_PART_CXX})
    target_include_directories(tikvcpp PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}/rust/cxxbridge/client-cpp/src
        ${CMAKE_CURRENT_SOURCE_DIR}/../include
    )

    add_library(tikvrust STATIC IMPORTED)
    set_target_properties(tikvrust PROPERTIES IMPORTED_LOCATION ${RUST_PART_LIB})

    add_dependencies(tikvcpp tikvrust)
else()
    # Option 2: Use installed package
    message(STATUS "Using installed tikvcpp package")
    find_package(tikvcpp 0.1.0 REQUIRED)
endif()

add_executable(raw raw.cpp)
target_link_libraries(raw tikvcpp tikvrust pthread dl ssl crypto)
add_executable(txn txn.cpp)
target_link_libraries(txn tikvcpp tikvrust pthread dl ssl crypto)
