cmake_minimum_required(VERSION 3.12)
project(TorchFS VERSION 0.1.0 LANGUAGES CXX)

# ──────────────────────────────────────
# Global compile flags
# ──────────────────────────────────────
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS  OFF)
add_definitions(-D_FILE_OFFSET_BITS=64)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
elseif(MSVC)
    add_compile_options(/W4)
endif()

# ──────────────────────────────────────
# FUSE (pkg-config)
# ──────────────────────────────────────
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse3)
include_directories(${FUSE_INCLUDE_DIRS})

# ──────────────────────────────────────
# Dependencies from vcpkg
# ──────────────────────────────────────
find_package(gflags           CONFIG REQUIRED)
find_package(unofficial-brpc  CONFIG REQUIRED)
find_package(unofficial-braft CONFIG REQUIRED)
find_package(RocksDB          CONFIG REQUIRED)
find_package(protobuf         CONFIG REQUIRED)   # vcpkg’s protobuf 3.11.x

# ──────────────────────────────────────
# Generate protos at build-time
# ──────────────────────────────────────
file(GLOB PROTO_FILES "src/common/proto/*.proto")
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# ──────────────────────────────────────
# Source lists
# ──────────────────────────────────────
file(GLOB CLIENT_SOURCES   src/client/*.cc)
file(GLOB METADATA_SOURCES src/metadata/*.cc)
file(GLOB COMMON_SOURCES   src/common/*.cc)

# ──────────────────────────────────────
# torchfs (FUSE client) binary
# ──────────────────────────────────────
add_executable(torchfs
    ${CLIENT_SOURCES}
    ${COMMON_SOURCES}
    ${PROTO_SRCS})

target_include_directories(torchfs PRIVATE
    src/include
    ${CMAKE_CURRENT_BINARY_DIR})  # Include path for generated protobuf headers

target_compile_definitions(torchfs PRIVATE
    FUSE_USE_VERSION=30 _GNU_SOURCE _FILE_OFFSET_BITS=64)

target_link_libraries(torchfs PRIVATE
    ${FUSE_LIBRARIES}
    gflags::gflags
    unofficial::brpc::brpc-static
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite
    RocksDB::rocksdb)

# ──────────────────────────────────────
# metadata (Raft KV) binary
# ──────────────────────────────────────
add_executable(metadata
    ${METADATA_SOURCES}
    ${COMMON_SOURCES}
    ${PROTO_SRCS})

target_include_directories(metadata PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    src/include)

target_link_libraries(metadata PRIVATE
    gflags::gflags
    unofficial::brpc::brpc-static
    unofficial::braft::braft-static
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite
    RocksDB::rocksdb
)
