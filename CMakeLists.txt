cmake_minimum_required(VERSION 3.12)
project(TorchFS VERSION 0.1.0 LANGUAGES CXX)

# ──────────────────────────────────────
# Global compile flags
# ──────────────────────────────────────
set(CMAKE_CXX_STANDARD    20)
set(CMAKE_CXX_EXTENSIONS  OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_definitions(-D_FILE_OFFSET_BITS=64)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
elseif (MSVC)
  add_compile_options(/W4)
endif()

# ──────────────────────────────────────
# FUSE (pkg-config)
# ──────────────────────────────────────
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse3)
include_directories(${FUSE_INCLUDE_DIRS})

# ──────────────────────────────────────
# liberasurecode: try vcpkg first, then system fallback
# ──────────────────────────────────────
include_directories(/usr/include)

# ──────────────────────────────────────
# gflags (system vs vcpkg)
# ──────────────────────────────────────
find_path(GFLAGS_INCLUDE_DIR
  NAMES gflags/gflags.h
  PATHS /usr/include /usr/local/include
)
find_library(GFLAGS_LIBRARY
  NAMES gflags
  PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib
)
if (NOT GFLAGS_INCLUDE_DIR OR NOT GFLAGS_LIBRARY)
  message(FATAL_ERROR "Could not find system gflags; install libgflags-dev")
endif()
include_directories(${GFLAGS_INCLUDE_DIR})
set(EXTRA_GFLAGS_LIBS ${GFLAGS_LIBRARY})

# ──────────────────────────────────────
# protobuf (system or vcpkg)
# ──────────────────────────────────────
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
set(EXTRA_PROTO_LIBS
  ${Protobuf_PROTOC_LIBRARY}
  ${Protobuf_LIBPROTOBUF}
  ${Protobuf_LIBPROTOBUF_LITE}
)

# ──────────────────────────────────────
# RocksDB (system only)
# ──────────────────────────────────────
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
find_library(ROCKSDB_LIBRARY NAMES rocksdb)
if (NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
  message(FATAL_ERROR "Could not find system RocksDB; install librocksdb-dev")
endif()
include_directories(${ROCKSDB_INCLUDE_DIR})
set(EXTRA_ROCKSDB_LIBS ${ROCKSDB_LIBRARY})

# ──────────────────────────────────────
# vcpkg-only: brpc & braft
# ──────────────────────────────────────
find_package(unofficial-brpc  CONFIG REQUIRED)
find_package(unofficial-braft CONFIG REQUIRED)

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
file(GLOB STORAGE_SOURCES  src/storage/*.cc)
file(GLOB COMMON_SOURCES   src/common/*.cc)

# ──────────────────────────────────────
# torchfs (FUSE client)
# ──────────────────────────────────────
add_executable(torchfs
  ${CLIENT_SOURCES}
  ${COMMON_SOURCES}
  ${PROTO_SRCS}
)
target_include_directories(torchfs PRIVATE
  src/include
  /usr/local/lib
  ${CMAKE_CURRENT_BINARY_DIR}
)
target_compile_definitions(torchfs PRIVATE
  FUSE_USE_VERSION=30 _GNU_SOURCE _FILE_OFFSET_BITS=64
)
target_link_libraries(torchfs PRIVATE
  ${FUSE_LIBRARIES}
  ${EXTRA_GFLAGS_LIBS}
  erasurecode
  unofficial::brpc::brpc-static
  unofficial::braft::braft-static
  ${EXTRA_PROTO_LIBS}
  ${EXTRA_ROCKSDB_LIBS}
)

# ──────────────────────────────────────
# metadata (Raft KV) binary
# ──────────────────────────────────────
add_executable(metadata
  ${METADATA_SOURCES}
  ${COMMON_SOURCES}
  ${PROTO_SRCS}
)
target_include_directories(metadata PRIVATE
  src/include
  ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(metadata PRIVATE
  ${EXTRA_GFLAGS_LIBS}
  ${EXTRA_ERASURE_LIBS}
  unofficial::brpc::brpc-static
  unofficial::braft::braft-static
  ${EXTRA_PROTO_LIBS}
  ${EXTRA_ROCKSDB_LIBS}
)

# ──────────────────────────────────────
# storage (Erasure Coding) binary
# ──────────────────────────────────────
add_executable(storage
  ${STORAGE_SOURCES}
  ${COMMON_SOURCES}
  ${PROTO_SRCS}
)
target_include_directories(storage PRIVATE
  src/include
  ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(storage PRIVATE
  ${EXTRA_GFLAGS_LIBS}
  ${EXTRA_ERASURE_LIBS}
  unofficial::brpc::brpc-static
  ${EXTRA_PROTO_LIBS}
)
